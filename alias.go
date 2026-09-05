// The `alias` object's implementation — a second name for a collection.
//
// Unlike a collection's fixed settings, an alias is genuinely reconcilable: pointing
// an existing alias at a different collection is what a blue-green swap IS, it costs
// no data, and Qdrant does it atomically. So this object converges in place and never
// needs the refuse-or-recreate split next door.
//
// It still reads the state back, for the reason the whole artifact does: measured
// against a live 1.18.3, `create_alias` on a name that already exists re-points it
// and answers 200, and `delete_alias` on a name that does not exist ALSO answers 200.
// The response is the same either way, so it cannot be the source of `changed`.
package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// aliasEntry is one row of GET /aliases.
type aliasEntry struct {
	AliasName      string `json:"alias_name"`
	CollectionName string `json:"collection_name"`
}

// listAliases reads every alias on the instance as alias -> collection.
func listAliases(ctx context.Context, api qdrantAPI, apiKey string) (map[string]string, error) {
	res, err := api.do(ctx, http.MethodGet, "/aliases", nil)
	if err != nil {
		return nil, fmt.Errorf("GET aliases: %s", redactError(err, apiKey))
	}
	if !res.ok() {
		return nil, fmt.Errorf("GET aliases: %s", redactString(res.errorText(), apiKey))
	}
	var payload struct {
		Aliases []aliasEntry `json:"aliases"`
	}
	if err := res.decodeResult(&payload); err != nil {
		return nil, fmt.Errorf("GET aliases: %s", redactError(err, apiKey))
	}
	out := make(map[string]string, len(payload.Aliases))
	for _, a := range payload.Aliases {
		out[a.AliasName] = a.CollectionName
	}
	return out, nil
}

// updateAliases posts one batch of alias actions. Qdrant applies a batch atomically,
// which is what makes a swap safe to run against a serving cluster.
func updateAliases(ctx context.Context, api qdrantAPI, actions []map[string]any, apiKey string) error {
	res, err := api.do(ctx, http.MethodPost, "/collections/aliases", map[string]any{"actions": actions})
	if err != nil {
		return fmt.Errorf("update aliases: %s", redactError(err, apiKey))
	}
	if !res.ok() {
		return fmt.Errorf("update aliases: %s", redactString(res.errorText(), apiKey))
	}
	return nil
}

// applyAliasPresent points one alias at one collection.
func (m *QdrantModule) applyAliasPresent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	name := strings.TrimSpace(stringOrEmpty(f["name"]))
	collection := strings.TrimSpace(stringOrEmpty(f["collection"]))

	// The target is checked before the alias is moved. Qdrant will refuse an alias
	// to a collection that does not exist, but the message it gives names the
	// alias, and an operator reading it in a run log should be told which of the
	// two names was the wrong one.
	if _, exists, err := readCollection(ctx, api, collection, apiKey); err != nil {
		return sendFailure(stream, err.Error())
	} else if !exists {
		return sendFailure(stream, fmt.Sprintf("alias %q: target collection %q does not exist", name, collection))
	}

	before, err := listAliases(ctx, api, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if before[name] == collection {
		return sendOutcome(stream, false, fmt.Sprintf("alias %q already points at %q", name, collection), map[string]any{
			"name":       name,
			"collection": collection,
		})
	}

	if err := updateAliases(ctx, api, []map[string]any{
		{"create_alias": map[string]any{"collection_name": collection, "alias_name": name}},
	}, apiKey); err != nil {
		return sendFailure(stream, err.Error())
	}

	after, err := listAliases(ctx, api, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if after[name] != collection {
		return sendFailure(stream, fmt.Sprintf(
			"alias %q: the update reported success but the alias points at %q, not %q",
			name, renderAliasTarget(after[name]), collection))
	}

	message := fmt.Sprintf("alias %q now points at %q", name, collection)
	if previous, had := before[name]; had {
		message = fmt.Sprintf("alias %q re-pointed from %q to %q", name, previous, collection)
	}
	return sendOutcome(stream, true, message, map[string]any{
		"name":       name,
		"collection": collection,
		"previous":   before[name],
	})
}

// applyAliasAbsent removes one alias. Idempotent: an alias that is not there is a
// no-op that sends nothing.
func (m *QdrantModule) applyAliasAbsent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	name := strings.TrimSpace(stringOrEmpty(f["name"]))

	before, err := listAliases(ctx, api, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	target, exists := before[name]
	if !exists {
		return sendOutcome(stream, false, fmt.Sprintf("alias %q is already absent", name), map[string]any{"name": name})
	}

	if err := updateAliases(ctx, api, []map[string]any{
		{"delete_alias": map[string]any{"alias_name": name}},
	}, apiKey); err != nil {
		return sendFailure(stream, err.Error())
	}

	after, err := listAliases(ctx, api, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if _, stillThere := after[name]; stillThere {
		return sendFailure(stream, fmt.Sprintf("alias %q still exists after the delete reported success", name))
	}
	return sendOutcome(stream, true, fmt.Sprintf("alias %q removed (pointed at %q)", name, target), map[string]any{
		"name":     name,
		"previous": target,
	})
}

// renderAliasTarget names what an alias points at, for a message where it may point
// at nothing.
func renderAliasTarget(collection string) string {
	if collection == "" {
		return "nothing"
	}
	return collection
}

func validateAliasPresent(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	errs = append(errs, validateName(f, "name")...)
	return append(errs, validateName(f, "collection")...)
}

func validateAliasAbsent(f map[string]*structpb.Value) []string {
	return append(validateAddr(f), validateName(f, "name")...)
}
