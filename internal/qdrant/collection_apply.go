// The `collection` object's four actions — the half of collection.go that talks to a
// live Qdrant. The domain decisions (what is reconcilable, what is not, and what the
// difference is) live next door in collection.go and are a pure function of two maps;
// this file is what carries one out.
//
// Two rules run through all of it, and both are answers to the same measured
// behaviour — Qdrant answers 200 `{"result":true}` whether or not it did what was
// asked:
//
//   - the refusal comes BEFORE the first mutating request, never after a partial one;
//   - every change is verified by READING THE COLLECTION BACK, and one that did not
//     take fails the step rather than being reported as done.
package qdrant

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// --- validation ---
//
// Everything checkable from the declaration alone is checked here, so a run that
// cannot work refuses before anything happens. What is NOT checkable here is drift
// against a live collection — that needs the instance — so [reconcileCollection]
// re-decides it before its first mutation rather than after (NIM-786: the point of
// the phase is to refuse BEFORE, and a check that only Validate performs is a check
// the runtime never runs, since it calls Apply).

// validateCollectionProbed / validateCollectionAbsent — the subject and a reachable
// instance are all either needs.
func validateCollectionProbed(f map[string]*structpb.Value) []string {
	return append(validateAddr(f), validateName(f, "name")...)
}

func validateCollectionAbsent(f map[string]*structpb.Value) []string {
	return validateCollectionProbed(f)
}

// validateCollectionPresent — everything checkable without the instance.
//
// The vector checks are here rather than left to Qdrant on purpose: a missing
// `distance` is a 400 from the create call, which arrives AFTER the run has started
// doing things, and a phase that lets through what the next phase will refuse is not
// a validation phase.
func validateCollectionPresent(f map[string]*structpb.Value) []string {
	errs := append(validateCollectionProbed(f), validateVectors(f)...)
	// The bounds and key sets Qdrant enforces on a create body (limits.go). Refused
	// here so a typo stops the run before it starts rather than arriving as a 400 from
	// the create. A misspelled key inside a passthrough map matters especially: Qdrant
	// accepts it, discards it, and still answers 200, so it would otherwise be drift
	// this module could never reconcile and would report on every run.
	declared := declaredCollection(f)
	errs = append(errs, checkCollectionBounds(declared)...)
	return append(errs, checkPassthroughKeys(declared)...)
}

// validateVectors checks the declared vector layout in both of its spellings.
func validateVectors(f map[string]*structpb.Value) []string {
	raw := structAsMap(f["vectors"])
	if len(raw) == 0 {
		return []string{"params.vectors: must be a non-empty map — either one vector's parameters ({size: 768, distance: Cosine}) or a map of named vectors ({text: {size: 768, distance: Cosine}})"}
	}
	// The single, unnamed form is told apart by carrying `size` at the top level —
	// the same way Qdrant tells them apart.
	if _, single := raw["size"]; single {
		return validateVectorSpec(raw, "params.vectors")
	}

	var errs []string
	for _, name := range sortedKeys(raw) {
		addr := "params.vectors." + name
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "/?#") {
			errs = append(errs, fmt.Sprintf("params.vectors: vector name %q is not usable — it addresses a URL path segment when the vector is added to a live collection", name))
			continue
		}
		spec, ok := raw[name].(map[string]any)
		if !ok {
			errs = append(errs, fmt.Sprintf("%s: must be a map of that vector's parameters ({size: 768, distance: Cosine})", addr))
			continue
		}
		errs = append(errs, validateVectorSpec(spec, addr)...)
	}
	return errs
}

// --- actions ---

// applyCollectionProbed reads one collection and reports it. An absent collection is a
// READING (`exists: false`), not a failure: a gate asking "is it there and green" has
// to be able to ask before it is.
func (m *Module) applyCollectionProbed(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	name := strings.TrimSpace(stringOrEmpty(f["name"]))

	info, exists, err := readCollection(ctx, api, name, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		// Every key the state's Description promises, so a gate written against
		// register.self.points_count resolves rather than hitting a missing field on
		// the one reading where the collection is not there.
		return sendOutcome(stream, false, fmt.Sprintf("collection %q does not exist", name), map[string]any{
			"name":                  name,
			"exists":                false,
			"status":                "",
			"optimizer_status":      "",
			"points_count":          int64(0),
			"segments_count":        int64(0),
			"indexed_vectors_count": int64(0),
		})
	}
	return sendOutcome(stream, false, fmt.Sprintf("collection %q: %s", name, info.Status), map[string]any{
		"name":                  name,
		"exists":                true,
		"status":                info.Status,
		"optimizer_status":      optimizerStatusText(info.OptimizerStatus),
		"points_count":          countOrZero(info.PointsCount),
		"segments_count":        countOrZero(info.SegmentsCount),
		"indexed_vectors_count": countOrZero(info.IndexedVectors),
	})
}

// applyCollectionPresent reconciles a collection to the declared shape and REFUSES
// rather than recreating when the declaration asks for something only a recreation
// could deliver.
func (m *Module) applyCollectionPresent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	return m.reconcileCollection(ctx, stream, api, params)
}

// reconcileCollection creates the collection when it is missing and reconciles what
// Qdrant can reconcile on a live one. It has no destructive path at all: v1 offers no
// state that rebuilds a collection, so nothing here can reach a DELETE.
func (m *Module) reconcileCollection(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	name := strings.TrimSpace(stringOrEmpty(f["name"]))
	declared := declaredCollection(f)

	info, exists, err := readCollection(ctx, api, name, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}

	if !exists {
		if err := m.createCollection(ctx, api, name, createBody(declared), apiKey); err != nil {
			return sendFailure(stream, err.Error())
		}
		return m.verifyCollection(ctx, stream, api, declared, name, apiKey, "created", fmt.Sprintf("collection %q created", name),
			map[string]any{"created": true})
	}

	plan := planCollection(declared, info.Config)

	if len(plan.conflicts) > 0 {
		// The refusal lands here, before a single mutating request. Applying the
		// reachable half first and failing afterwards would leave the collection in a
		// shape nobody declared — which is worse than either outcome the author asked
		// for.
		return sendFailure(stream, refusalText(name, plan.conflicts))
	}

	if plan.empty() {
		// `exists` too, and for the same reason the absent branch of `probed` carries
		// the full set: a gate written against a key must find it on EVERY run, and
		// the converged run is the one it will meet most often.
		return sendOutcome(stream, false, fmt.Sprintf("collection %q is already in the declared shape", name), map[string]any{
			"name": name, "exists": true, "created": false,
		})
	}

	changes := planSummary(plan)
	for _, vector := range sortedKeys(plan.addVectors) {
		if err := m.addVector(ctx, api, name, vector, plan.addVectors[vector], apiKey); err != nil {
			return sendFailure(stream, err.Error())
		}
	}
	if len(plan.patch) > 0 {
		if err := m.patchCollection(ctx, api, name, plan.patch, apiKey); err != nil {
			return sendFailure(stream, err.Error())
		}
	}
	return m.verifyCollection(ctx, stream, api, declared, name, apiKey, "updated",
		fmt.Sprintf("collection %q reconciled: %s", name, strings.Join(changes, ", ")),
		map[string]any{"created": false})
}

// verifyCollection re-reads the collection and refuses to report success while the
// declaration is still unmet.
//
// This is the guard the whole object is shaped around. Qdrant accepts an update it
// discards, so "the request returned 200" is not evidence and only the collection
// itself is. It catches two things: a setting this module classified as reconcilable
// that a given Qdrant build does not, and — far likelier in practice — a misspelled key
// inside one of the passthrough maps, which Qdrant drops in silence and which would
// otherwise show up as a step that reports a change on every run and never converges.
func (m *Module) verifyCollection(ctx context.Context, stream eventStream, api qdrantAPI, declared map[string]any, name, apiKey, did, message string, output map[string]any) error {
	info, exists, err := readCollection(ctx, api, name, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		return sendFailure(stream, fmt.Sprintf("collection %q is absent immediately after the request that should have created it", name))
	}
	if plan := planCollection(declared, info.Config); !plan.empty() {
		// `did` is load-bearing in this message, not decoration. After a failed
		// create the collection EXISTS and is wrong, which is a different thing to
		// go and fix than an update that did not land — and an operator told only
		// "the setting did not change" would go looking for a collection they
		// believe was never made.
		return sendFailure(stream, fmt.Sprintf(
			"collection %q was %s, but Qdrant did NOT apply everything declared:\n  %s\n"+
				"Qdrant answers 200 {\"result\":true} to a field it discards, so this was caught by reading the collection back rather than from the response. It is most often a misspelled key inside a passthrough map (hnsw_config, optimizers_config, quantization_config, wal_config) — check it against the Qdrant version you are running.",
			name, did, strings.Join(append(conflictPaths(plan.conflicts), planSummary(plan)...), "\n  ")))
	}
	output["name"] = name
	output["exists"] = true
	return sendOutcome(stream, true, message, output)
}

// applyCollectionAbsent removes a collection. Idempotent: one that is not there is a
// no-op that sends no DELETE at all.
func (m *Module) applyCollectionAbsent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	name := strings.TrimSpace(stringOrEmpty(f["name"]))

	_, exists, err := readCollection(ctx, api, name, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		return sendOutcome(stream, false, fmt.Sprintf("collection %q is already absent", name), map[string]any{"name": name})
	}
	removed, err := m.dropCollection(ctx, api, name, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !removed {
		// Qdrant answers 200 {"result":false} when there was nothing to delete, and
		// the commonest way to get here is an ALIAS: `GET /collections/<x>` RESOLVES
		// one and answers with the target's config, so the read above said "present",
		// while DELETE does not resolve and removes nothing. Saying "still exists"
		// would send an operator looking for a collection that was never there under
		// that name.
		if aliases, aErr := listAliases(ctx, api, apiKey); aErr == nil {
			if target, isAlias := aliases[name]; isAlias {
				return sendFailure(stream, fmt.Sprintf(
					"nothing was deleted: %q is an ALIAS pointing at collection %q, not a collection. Qdrant resolves an alias on a read but not on a delete, so this state cannot remove it — use qdrant.alias.absent for the alias, or name the collection itself.",
					name, target))
			}
		}
		return sendFailure(stream, fmt.Sprintf(
			"nothing was deleted: Qdrant reported there was no collection %q to remove, although it read as present a moment earlier", name))
	}
	// And the removal is confirmed by reading it back, not by the response.
	if _, stillThere, err := readCollection(ctx, api, name, apiKey); err != nil {
		return sendFailure(stream, err.Error())
	} else if stillThere {
		return sendFailure(stream, fmt.Sprintf("collection %q still exists after the delete reported success", name))
	}
	return sendOutcome(stream, true, fmt.Sprintf("collection %q deleted", name), map[string]any{"name": name})
}

// --- messages ---

// refusalText is what an author sees when the declaration cannot be reached without
// destroying data. It names every conflicting setting with both values and says what
// the two ways forward are; a refusal that only said "cannot reconcile" would send
// them to read this source.
func refusalText(name string, conflicts []conflict) string {
	lines := make([]string, 0, len(conflicts))
	for _, c := range conflicts {
		lines = append(lines, "  "+c.String())
	}
	return fmt.Sprintf(
		"collection %q cannot be reconciled in place — %s can only be changed by recreating the collection, which destroys every point in it:\n%s\n"+
			"Bring the declaration back in line with the live collection. This module offers no state that rebuilds one: reaching the declared shape means dropping the collection and creating it again, which destroys every point in it, and that is a decision to take deliberately — take a snapshot (qdrant.snapshot.created), then qdrant.collection.absent, then qdrant.collection.present.",
		name, plural(len(conflicts), "setting", "settings"), strings.Join(lines, "\n"))
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

func joinConflicts(conflicts []conflict) string {
	out := make([]string, 0, len(conflicts))
	for _, c := range conflicts {
		out = append(out, c.String())
	}
	return strings.Join(out, "; ")
}

func conflictPaths(conflicts []conflict) []string {
	out := make([]string, 0, len(conflicts))
	for _, c := range conflicts {
		out = append(out, c.String())
	}
	return out
}

// planSummary names what a reconciliation is about to change, one sorted line per
// leaf, so the event message says which settings moved rather than "updated".
func planSummary(plan collectionPlan) []string {
	out := leafPaths(plan.patch, "")
	for _, name := range sortedKeys(plan.addVectors) {
		out = append(out, "vectors."+quoteVectorName(name)+" (added)")
	}
	sort.Strings(out)
	return out
}

// leafPaths walks a request body into sorted `a.b.c=value` lines.
func leafPaths(m map[string]any, prefix string) []string {
	var out []string
	for _, key := range sortedKeys(m) {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if nested, ok := m[key].(map[string]any); ok && len(nested) > 0 {
			out = append(out, leafPaths(nested, path)...)
			continue
		}
		out = append(out, path+"="+renderValue(m[key]))
	}
	return out
}

// --- requests ---

func (m *Module) createCollection(ctx context.Context, api qdrantAPI, name string, body map[string]any, apiKey string) error {
	res, err := api.do(ctx, http.MethodPut, "/collections/"+url.PathEscape(name), body)
	if err != nil {
		return fmt.Errorf("create collection %q: %s", name, redactError(err, apiKey))
	}
	if !res.ok() {
		return fmt.Errorf("create collection %q: %s", name, redactString(res.errorText(), apiKey))
	}
	return nil
}

func (m *Module) patchCollection(ctx context.Context, api qdrantAPI, name string, body map[string]any, apiKey string) error {
	res, err := api.do(ctx, http.MethodPatch, "/collections/"+url.PathEscape(name), body)
	if err != nil {
		return fmt.Errorf("update collection %q: %s", name, redactError(err, apiKey))
	}
	if !res.ok() {
		return fmt.Errorf("update collection %q: %s", name, redactString(res.errorText(), apiKey))
	}
	return nil
}

// dropCollection removes a collection and reports whether it actually removed one.
//
// The bool is not decoration. Qdrant answers 200 `{"result":false}` when there was
// nothing to delete, and "nothing to delete" is reachable with a name that reads as
// present: `GET /collections/<x>` RESOLVES AN ALIAS and returns the target's config,
// while `DELETE /collections/<x>` does not resolve and removes nothing. Treating that
// as a successful drop is how this state came to announce destroyed data that was
// entirely intact.
func (m *Module) dropCollection(ctx context.Context, api qdrantAPI, name string, apiKey string) (bool, error) {
	res, err := api.do(ctx, http.MethodDelete, "/collections/"+url.PathEscape(name), nil)
	if err != nil {
		return false, fmt.Errorf("delete collection %q: %s", name, redactError(err, apiKey))
	}
	if !res.ok() {
		return false, fmt.Errorf("delete collection %q: %s", name, redactString(res.errorText(), apiKey))
	}
	var removed bool
	if err := res.decodeResult(&removed); err != nil {
		// An answer whose result cannot be read is not evidence either way; assume
		// nothing was removed rather than report a destruction that may not have
		// happened.
		return false, nil
	}
	return removed, nil
}

// addVector adds one named vector to an existing collection. The body is Qdrant's
// untagged VectorNameConfig, whose dense arm is `{"dense": {…}}`.
//
// spec carries ONLY the creation keys — [splitVectorSpec] has already taken the
// tunable ones out, because Qdrant's DenseVectorConfig does not accept them and would
// discard them in silence.
//
// `wait=true` for the same reason every other verified write in this artifact uses it:
// without it the endpoint answers `acknowledged` and the read-back that follows races
// the update, which on a busy collection shows up as an intermittent failure blaming a
// declaration that was fine.
//
// This is the one mutating endpoint of Qdrant's that behaves the way the rest of this
// object has to compensate for: re-sending an identical configuration is a no-op, and
// the same name with a different size is a 400 that says exactly that, rather than a
// silent success.
func (m *Module) addVector(ctx context.Context, api qdrantAPI, collection, vector string, spec any, apiKey string) error {
	path := "/collections/" + url.PathEscape(collection) + "/vectors/" + url.PathEscape(vector) + "?wait=true"
	res, err := api.do(ctx, http.MethodPut, path, map[string]any{"dense": spec})
	if err != nil {
		return fmt.Errorf("add vector %s to collection %q: %s", quoteVectorName(vector), collection, redactError(err, apiKey))
	}
	if !res.ok() {
		return fmt.Errorf("add vector %s to collection %q: %s", quoteVectorName(vector), collection, redactString(res.errorText(), apiKey))
	}
	return nil
}
