// The two contracts this artifact inherited as paid-for rules, asserted across EVERY
// object rather than on one convenient action.
//
//   - NIM-778: a param of the wrong type is refused, never coerced.
//   - NIM-786: Validate refuses what Apply refuses, and Apply refuses it too — a
//     runner need not call Validate at all.
package qdrant

import (
	"strings"
	"testing"
)

// allObjects pairs every object with one state, for the sweeps below. Every object is
// listed on purpose: these rules are artifact-wide, and a sweep over "the interesting
// one" is how the asymmetry NIM-778 opens with got in — `persist` was made strict by
// hand and `tls` beside it was not.
func allObjects(m *Module) []struct {
	object string
	state  string
	params map[string]any
} {
	return []struct {
		object string
		state  string
		params map[string]any
	}{
		{"instance", "pinged", map[string]any{}},
		{"instance", "ready-probed", map[string]any{}},
		{"instance", "version-probed", map[string]any{}},
		{"collection", "probed", map[string]any{"name": "docs"}},
		{"collection", "absent", map[string]any{"name": "docs"}},
		{"collection", "present", map[string]any{"name": "docs", "vectors": map[string]any{"size": 4, "distance": "Cosine"}}},
		{"alias", "present", map[string]any{"name": "live", "collection": "docs"}},
		{"alias", "absent", map[string]any{"name": "live"}},
		{"index", "present", map[string]any{"collection": "docs", "field": "tenant", "schema": "keyword"}},
		{"index", "absent", map[string]any{"collection": "docs", "field": "tenant"}},
		{"snapshot", "created", map[string]any{"collection": "docs"}},
		{"snapshot", "absent", map[string]any{"collection": "docs", "name": "docs-1.snapshot"}},
	}
}

func objectByName(m *Module, name string) *object {
	return objects(m)[name]
}

// TestWrongTypedParamIsRefusedNotCoerced — NIM-778, on every state.
//
// `tls: "true"` as a string is the exact shape of the live defect: it used to read as
// `tls: false`, so the connection went out in plaintext WITH the credential on it, and
// the step reported itself reconciled. Nothing upstream catches it — the runtime calls
// Apply rather than Validate, and the Keeper's static check returns nil on a `${…}`
// cell, so `tls: "${ vars.qdrant_tls }"` over a string var lints clean. This is the
// last place that can say no.
func TestWrongTypedParamIsRefusedNotCoerced(t *testing.T) {
	badValues := map[string]any{
		"tls":             "true",
		"tls_skip_verify": "yes",
		"timeout_sec":     "30",
	}

	for _, tc := range allObjects(nil) {
		for param, bad := range badValues {
			t.Run(tc.object+"."+tc.state+"/"+param, func(t *testing.T) {
				api := newFakeAPI(t)
				obj := objectByName(moduleWith(api), tc.object)

				params := baseParams(tc.params)
				params[param] = bad

				// Validate refuses it...
				reply := runValidate(t, obj, tc.state, params)
				if reply.GetOk() {
					t.Errorf("Validate accepted %s=%v of the wrong type", param, bad)
				}

				// ...and so does Apply, which is the half that matters, because the
				// runtime calls Apply and may never call Validate.
				stream := runApply(t, obj, tc.state, params)
				if !stream.final().GetFailed() {
					t.Errorf("Apply accepted %s=%v of the wrong type: %s", param, bad, stream.final().GetMessage())
				}
				if !strings.Contains(stream.final().GetMessage(), "params."+param) {
					t.Errorf("the refusal does not name the parameter: %s", stream.final().GetMessage())
				}
				// Refused BEFORE a socket was opened: the whole point is that the
				// credential never leaves the process on the wrong transport.
				if len(api.calls) != 0 {
					t.Errorf("a wrong-typed param still reached the instance: %v", api.pathsHit())
				}
			})
		}
	}
}

// TestIntegerParamRefusesAFraction — truncation is a guess, and a guess is what this
// rule forbids. `shard_number: 2.5` silently creating two shards is the same class of
// surprise as `tls: "true"` addressing plaintext.
func TestIntegerParamRefusesAFraction(t *testing.T) {
	api := newFakeAPI(t)
	obj := moduleWith(api).collection()
	params := baseParams(map[string]any{
		"name":         "docs",
		"vectors":      map[string]any{"size": 4, "distance": "Cosine"},
		"shard_number": 2.5,
	})

	if runValidate(t, obj, "present", params).GetOk() {
		t.Error("Validate accepted a fractional shard_number")
	}
	stream := runApply(t, obj, "present", params)
	if !stream.final().GetFailed() {
		t.Errorf("Apply accepted a fractional shard_number: %s", stream.final().GetMessage())
	}
	if len(api.calls) != 0 {
		t.Errorf("a fractional integer still reached the instance: %v", api.pathsHit())
	}
}

// TestValidateRefusesEverythingApplyRefuses — NIM-786.
//
// The live defect it is named for: a validator swallowed an error, so Validate came
// back clean and Apply failed in the middle of a run, after earlier steps had already
// changed the host. A phase that lets through what the next phase will refuse is not a
// validation phase.
//
// Both directions are asserted. Validate must refuse, AND Apply must refuse the same
// input on its own — because a runner need not call Validate at all.
func TestValidateRefusesEverythingApplyRefuses(t *testing.T) {
	cases := []struct {
		name   string
		object string
		state  string
		params map[string]any
	}{
		{"no addr", "collection", "probed", map[string]any{"name": "docs"}},
		{"empty addr", "collection", "probed", map[string]any{"addr": "  ", "name": "docs"}},
		{"addr carrying a scheme", "collection", "probed", baseParams(map[string]any{"addr": "https://qdrant-1:6333", "name": "docs"})},
		{"empty collection name", "collection", "probed", baseParams(map[string]any{"name": ""})},
		{"collection name with a slash", "collection", "probed", baseParams(map[string]any{"name": "a/b"})},
		{"collection name with trailing space", "collection", "probed", baseParams(map[string]any{"name": "docs "})},
		{"no vectors", "collection", "present", baseParams(map[string]any{"name": "docs"})},
		{"vector without size", "collection", "present", baseParams(map[string]any{
			"name": "docs", "vectors": map[string]any{"distance": "Cosine"}})},
		{"vector without distance", "collection", "present", baseParams(map[string]any{
			"name": "docs", "vectors": map[string]any{"size": 4}})},
		{"vector with a zero size", "collection", "present", baseParams(map[string]any{
			"name": "docs", "vectors": map[string]any{"size": 0, "distance": "Cosine"}})},
		{"vector with a lowercase distance", "collection", "present", baseParams(map[string]any{
			"name": "docs", "vectors": map[string]any{"size": 4, "distance": "cosine"}})},
		{"vector with an unmanaged key", "collection", "present", baseParams(map[string]any{
			"name": "docs", "vectors": map[string]any{"size": 4, "distance": "Cosine", "tokenizer": "word"}})},
		{"unknown key inside a passthrough map", "collection", "present", baseParams(map[string]any{
			"name": "docs", "vectors": map[string]any{"size": 4, "distance": "Cosine"},
			"wal_config": map[string]any{"wal_capacity_mbb": 64}})},
		{"alias without a target", "alias", "present", baseParams(map[string]any{"name": "live"})},
		{"index with an unknown schema", "index", "present", baseParams(map[string]any{
			"collection": "docs", "field": "tenant", "schema": "keywords"})},
		{"index without a field", "index", "present", baseParams(map[string]any{
			"collection": "docs", "schema": "keyword"})},
		{"negative max_age_sec", "snapshot", "created", baseParams(map[string]any{
			"collection": "docs", "max_age_sec": -1})},
		{"snapshot without a name", "snapshot", "absent", baseParams(map[string]any{"collection": "docs"})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeAPI(t)
			obj := objectByName(moduleWith(api), tc.object)

			reply := runValidate(t, obj, tc.state, tc.params)
			if reply.GetOk() {
				t.Fatalf("Validate accepted input Apply refuses — the phase is decorative for this case")
			}

			stream := runApply(t, obj, tc.state, tc.params)
			if !stream.final().GetFailed() {
				t.Fatalf("Apply accepted what Validate refused: %s", stream.final().GetMessage())
			}
			if len(api.mutating()) != 0 {
				t.Errorf("refused input still mutated the instance: %v", api.mutating())
			}
		})
	}
}

// TestUnknownStateNamesTheObject — with five objects in one artifact, "unknown state"
// alone leaves an author guessing whether the word is wrong or the object is.
func TestUnknownStateNamesTheObject(t *testing.T) {
	obj := moduleWith(newFakeAPI(t)).collection()

	reply := runValidate(t, obj, "presnt", baseParams(map[string]any{"name": "docs"}))
	if reply.GetOk() {
		t.Fatal("Validate accepted an unknown state")
	}
	msg := strings.Join(reply.GetErrors(), "; ")
	for _, want := range []string{"presnt", "collection", "present"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the message must mention %q: %s", want, msg)
		}
	}
}
