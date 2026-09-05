// The `index` object's implementation — a payload field index on a collection.
//
// This object is the deliberate CONTRAST with `collection`. Reaching a declared
// payload index sometimes means dropping the existing one and building it again, and
// here that is done in place, without an acknowledgement and without a separate
// state — because rebuilding a payload index destroys an INDEX, not data. The points
// and their payloads are untouched; Qdrant reads them again to build it. The rule
// this artifact follows is not "never do anything that looks destructive", it is
// "never destroy data quietly", and keeping the two cases visibly apart is what makes
// the refusal next door mean something.
//
// The cost that IS real is a window: between the drop and the create, filtered
// queries on that field fall back to a full scan. That belongs in the state
// description, not in a confirmation flag.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// payloadSchemaTypes is the closed set of field schemas this object manages,
// confirmed against a live 1.18.3: each of these is echoed back verbatim as
// `payload_schema.<field>.data_type`, which is what makes the comparison exact.
//
// Qdrant also accepts an OBJECT schema (a text field with a tokenizer, a parameterised
// integer index). Those are deliberately out of this version: the read-back reports
// only `data_type`, so this module could not tell a live `text` index with one
// tokenizer from a declaration with another, and would report converged on a
// collection that is not. A half-compared index is worse than an unsupported one.
var payloadSchemaTypes = []any{"keyword", "integer", "float", "bool", "geo", "datetime", "uuid", "text"}

// payloadSchema reads a collection's payload indexes as field -> data_type.
func payloadSchema(ctx context.Context, api qdrantAPI, collection, apiKey string) (map[string]string, bool, error) {
	info, exists, err := readCollection(ctx, api, collection, apiKey)
	if err != nil || !exists {
		return nil, exists, err
	}
	out := map[string]string{}
	for field, spec := range info.PayloadSchema {
		if m, isMap := spec.(map[string]any); isMap {
			if dataType, isStr := m["data_type"].(string); isStr {
				out[field] = dataType
			}
		}
	}
	return out, true, nil
}

// indexPath is the create endpoint; `wait=true` makes the call return only once the
// index is built, so the read-back that follows is not a race against a background
// optimizer.
func indexPath(collection string) string {
	return "/collections/" + url.PathEscape(collection) + "/index?wait=true"
}

func dropIndexPath(collection, field string) string {
	return "/collections/" + url.PathEscape(collection) + "/index/" + url.PathEscape(field) + "?wait=true"
}

// applyIndexPresent brings one payload field index to the declared schema.
func (m *QdrantModule) applyIndexPresent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	collection := strings.TrimSpace(stringOrEmpty(f["collection"]))
	field := strings.TrimSpace(stringOrEmpty(f["field"]))
	want := strings.TrimSpace(stringOrEmpty(f["schema"]))

	live, exists, err := payloadSchema(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		return sendFailure(stream, fmt.Sprintf("collection %q does not exist", collection))
	}

	got, indexed := live[field]
	if indexed && got == want {
		return sendOutcome(stream, false, fmt.Sprintf("payload index %s.%s is already %s", collection, field, want), map[string]any{
			"collection": collection,
			"field":      field,
			"schema":     want,
		})
	}

	// A field indexed under a different schema has to be dropped first: Qdrant
	// refuses to build a second index over the same field.
	if indexed {
		if err := m.dropIndex(ctx, api, collection, field, apiKey); err != nil {
			return sendFailure(stream, err.Error())
		}
	}

	res, err := api.do(ctx, http.MethodPut, indexPath(collection), map[string]any{
		"field_name":   field,
		"field_schema": want,
	})
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("create payload index %s.%s: %s", collection, field, redactError(err, apiKey)))
	}
	if !res.ok() {
		return sendFailure(stream, fmt.Sprintf("create payload index %s.%s: %s", collection, field, redactString(res.errorText(), apiKey)))
	}

	after, _, err := payloadSchema(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if after[field] != want {
		return sendFailure(stream, fmt.Sprintf(
			"payload index %s.%s: the create reported success but the field is indexed as %q, not %q",
			collection, field, renderIndexState(after[field]), want))
	}

	message := fmt.Sprintf("payload index %s.%s created as %s", collection, field, want)
	if indexed {
		message = fmt.Sprintf("payload index %s.%s rebuilt from %s to %s", collection, field, got, want)
	}
	return sendOutcome(stream, true, message, map[string]any{
		"collection": collection,
		"field":      field,
		"schema":     want,
		"previous":   got,
	})
}

// applyIndexAbsent removes one payload field index. Idempotent.
func (m *QdrantModule) applyIndexAbsent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	collection := strings.TrimSpace(stringOrEmpty(f["collection"]))
	field := strings.TrimSpace(stringOrEmpty(f["field"]))

	live, exists, err := payloadSchema(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		return sendFailure(stream, fmt.Sprintf("collection %q does not exist", collection))
	}
	got, indexed := live[field]
	if !indexed {
		return sendOutcome(stream, false, fmt.Sprintf("payload index %s.%s is already absent", collection, field), map[string]any{
			"collection": collection,
			"field":      field,
		})
	}

	if err := m.dropIndex(ctx, api, collection, field, apiKey); err != nil {
		return sendFailure(stream, err.Error())
	}
	after, _, err := payloadSchema(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if _, stillThere := after[field]; stillThere {
		return sendFailure(stream, fmt.Sprintf("payload index %s.%s still exists after the delete reported success", collection, field))
	}
	return sendOutcome(stream, true, fmt.Sprintf("payload index %s.%s removed (was %s)", collection, field, got), map[string]any{
		"collection": collection,
		"field":      field,
		"previous":   got,
	})
}

func (m *QdrantModule) dropIndex(ctx context.Context, api qdrantAPI, collection, field, apiKey string) error {
	res, err := api.do(ctx, http.MethodDelete, dropIndexPath(collection, field), nil)
	if err != nil {
		return fmt.Errorf("delete payload index %s.%s: %s", collection, field, redactError(err, apiKey))
	}
	if !res.ok() {
		return fmt.Errorf("delete payload index %s.%s: %s", collection, field, redactString(res.errorText(), apiKey))
	}
	return nil
}

// renderIndexState names a field's index state for a message where it may have none.
func renderIndexState(dataType string) string {
	if dataType == "" {
		return "not indexed"
	}
	return dataType
}

func validateIndexPresent(f map[string]*structpb.Value) []string {
	errs := validateIndexAbsent(f)
	schema := strings.TrimSpace(stringOrEmpty(f["schema"]))
	if schema == "" {
		return append(errs, "params.schema: must be a non-empty string")
	}
	for _, known := range payloadSchemaTypes {
		if known == schema {
			return errs
		}
	}
	names := make([]string, 0, len(payloadSchemaTypes))
	for _, k := range payloadSchemaTypes {
		names = append(names, fmt.Sprintf("%v", k))
	}
	return append(errs, fmt.Sprintf("params.schema: must be one of %s, got %q", strings.Join(names, ", "), schema))
}

func validateIndexAbsent(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	errs = append(errs, validateName(f, "collection")...)
	return append(errs, validateName(f, "field")...)
}
