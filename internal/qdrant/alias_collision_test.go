// Regressions for the assumption underneath a whole family of defects: that a
// collection name addresses a collection.
//
// It does not always. Qdrant lets an ALIAS occupy a name, and the three verbs then
// disagree about it, all measured on a live 1.18.3:
//
//	GET    /collections/<x>   RESOLVES the alias, answering with the target's config
//	DELETE /collections/<x>   does NOT resolve — removes nothing, answers 200 {"result":false}
//	PUT    /collections/<x>   refused: "Alias with the same name already exists"
//
// `collection.absent` is the one destructive state v1 ships, and the trap is invisible
// to it: the read says present, the delete reports success, and the module would have
// announced a deletion that never happened.
package qdrant

import (
	"strings"
	"testing"
)

func aliasesResult(t *testing.T, pairs map[string]string) apiResult {
	t.Helper()
	rows := make([]any, 0, len(pairs))
	for _, a := range sortedKeys(pairs) {
		rows = append(rows, map[string]any{"alias_name": a, "collection_name": pairs[a]})
	}
	return okResult(t, map[string]any{"aliases": rows})
}

// ★ TestAbsentDoesNotClaimADeletionItDidNotPerform — the name resolves through an
// alias, so the read says present and the DELETE removes nothing while answering 200.
// Reporting changed=true there would tell an operator their data is gone when the
// collection the alias points at is untouched.
func TestAbsentDoesNotClaimADeletionItDidNotPerform(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("DELETE", "/collections/docs", okResult(t, false)).
		on("GET", "/aliases", aliasesResult(t, map[string]string{"docs": "docs_v2"}))

	event := runApply(t, moduleWith(api).collection(), "absent",
		baseParams(map[string]any{"name": "docs"})).final()

	if !event.GetFailed() {
		t.Fatalf("a delete that removed nothing must not be reported as success: %s", event.GetMessage())
	}
	if event.GetChanged() {
		t.Error("nothing was deleted, so changed must be false")
	}
	for _, want := range []string{"ALIAS", "docs_v2", "qdrant.alias.absent"} {
		if !strings.Contains(event.GetMessage(), want) {
			t.Errorf("the failure must explain the name is an alias and what to do (%q missing):\n%s", want, event.GetMessage())
		}
	}
}

// TestAbsentReportsAVanishedCollectionHonestly — the same signal without an alias
// behind it: something else removed the collection between the read and the delete.
// Nothing of ours is gone either way.
func TestAbsentReportsAVanishedCollectionHonestly(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("DELETE", "/collections/docs", okResult(t, false)).
		on("GET", "/aliases", noAliases(t))

	event := runApply(t, moduleWith(api).collection(), "absent",
		baseParams(map[string]any{"name": "docs"})).final()

	if !event.GetFailed() || event.GetChanged() {
		t.Fatalf("expected an honest failure, got failed=%v changed=%v: %s",
			event.GetFailed(), event.GetChanged(), event.GetMessage())
	}
	if !strings.Contains(event.GetMessage(), "nothing was deleted") {
		t.Errorf("the message must say nothing was deleted:\n%s", event.GetMessage())
	}
}

// TestAbsentStillWorksNormally — the guard above must not break the ordinary path.
func TestAbsentStillWorksNormally(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)),
			notFoundResult("Collection `docs` doesn't exist!")).
		on("DELETE", "/collections/docs", okTrue(t))

	event := runApply(t, moduleWith(api).collection(), "absent",
		baseParams(map[string]any{"name": "docs"})).final()

	if event.GetFailed() || !event.GetChanged() {
		t.Fatalf("a real deletion must report changed=true: failed=%v %s", event.GetFailed(), event.GetMessage())
	}
}

// TestUnknownKeyInsideAVectorIsRefused — `multivector_config` is per-vector AND
// creation-only, and Qdrant accepts an unknown key in it and discards it (measured).
// That makes it drift `present` can never reconcile: without this check the state
// refuses a collection that is otherwise exactly right, on every run, blaming nothing
// an author can see.
func TestUnknownKeyInsideAVectorIsRefused(t *testing.T) {
	params := baseParams(map[string]any{
		"name": "docs",
		"vectors": map[string]any{
			"size": 4, "distance": "Cosine",
			"multivector_config": map[string]any{"comparator": "max_sim", "bogus": 1},
		},
	})

	if reply := runValidate(t, moduleWith(newFakeAPI(t)).collection(), "present", params); reply.GetOk() {
		t.Error("Validate must refuse a key Qdrant accepts and discards inside a creation-only map")
	}
	api := newFakeAPI(t)
	if !runApply(t, moduleWith(api).collection(), "present", params).final().GetFailed() {
		t.Error("Apply must refuse it too — the runtime calls Apply")
	}
	if len(api.calls) != 0 {
		t.Errorf("the refusal must reach nothing, called %v", api.pathsHit())
	}
}

// TestHnswMemoryIsNotAccepted — the collection-level twin of a vector's `memory`:
// accepted by Qdrant, never echoed back, so no read-back can confirm it and the step
// would fail on every run blaming a legal declaration.
func TestHnswMemoryIsNotAccepted(t *testing.T) {
	if passthroughKeys["hnsw_config"]["memory"] {
		t.Fatal("hnsw_config.memory is never reported back by Qdrant, so no read-back can confirm it")
	}
	reply := runValidate(t, moduleWith(newFakeAPI(t)).collection(), "present",
		baseParams(map[string]any{
			"name":        "docs",
			"vectors":     map[string]any{"size": 4, "distance": "Cosine"},
			"hnsw_config": map[string]any{"memory": "cached"},
		}))
	if reply.GetOk() {
		t.Error("a key that can never converge must be refused rather than retried forever")
	}
}
