// Regressions for the fourth review — and for the assumption underneath them: that a
// collection name addresses a collection.
//
// It does not always. Qdrant lets an ALIAS occupy a name, and the three verbs then
// disagree about it: `GET /collections/<x>` RESOLVES the alias and answers with the
// target's config, `DELETE /collections/<x>` does NOT resolve and removes nothing while
// answering 200 `{"result":false}`, and `PUT /collections/<x>` is refused outright. All
// measured on a live 1.18.3.
//
// Read together, that turned `recreated` into a step that reported destroyed data which
// was entirely intact — and told the operator to restore from a snapshot, which would
// have destroyed the live collection the alias pointed at.
package qdrant

import (
	"encoding/json"
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

// ★ TestRecreatedRefusesANameOccupiedByAnAlias — nothing is dropped, and the operator
// is told which collection the name actually resolves to.
func TestRecreatedRefusesANameOccupiedByAnAlias(t *testing.T) {
	probe := precheckName("docs")
	api := newFakeAPI(t).
		// GET resolves the alias, so the collection reads as present with the
		// target's config — which is exactly what makes this trap invisible.
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("GET", "/collections/"+probe, notFoundResult("Collection `"+probe+"` doesn't exist!")).
		on("PUT", "/collections/"+probe, okTrue(t)).
		on("DELETE", "/collections/"+probe, okTrue(t)).
		on("GET", "/aliases", aliasesResult(t, map[string]string{"docs": "docs_v2"}))

	stream := runApply(t, moduleWith(api).collection(), "recreated", baseParams(map[string]any{
		"name":            "docs",
		"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
		"confirm_destroy": true,
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("a name occupied by an alias cannot be rebuilt and must be refused: %s", event.GetMessage())
	}
	for _, c := range api.mutating() {
		if c.path == "/collections/docs" {
			t.Fatalf("the real name was touched: %s", c.key())
		}
	}
	for _, want := range []string{"ALIAS", "docs_v2", "NOT touched"} {
		if !strings.Contains(event.GetMessage(), want) {
			t.Errorf("the refusal must mention %q:\n%s", want, event.GetMessage())
		}
	}
	// The message that must NOT appear: the one that sends an operator to restore a
	// snapshot over live data.
	if strings.Contains(event.GetMessage(), "DATA IS GONE") {
		t.Errorf("nothing was destroyed — this message would send the operator to destroy the live collection:\n%s", event.GetMessage())
	}
}

// TestADropThatRemovedNothingIsNotReportedAsDestruction — the second half of the same
// defect. Qdrant answers 200 `{"result":false}` when there was nothing to delete, and
// `res.ok()` is true for it.
func TestADropThatRemovedNothingIsNotReportedAsDestruction(t *testing.T) {
	probe := precheckName("docs")
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("GET", "/collections/"+probe, notFoundResult("absent")).
		on("PUT", "/collections/"+probe, okTrue(t)).
		on("DELETE", "/collections/"+probe, okTrue(t)).
		on("GET", "/aliases", noAliases(t)).
		// 200, but it removed nothing.
		on("DELETE", "/collections/docs", okResult(t, false))

	event := runApply(t, moduleWith(api).collection(), "recreated", baseParams(map[string]any{
		"name":            "docs",
		"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
		"confirm_destroy": true,
	})).final()

	if !event.GetFailed() {
		t.Fatal("a drop that removed nothing must not proceed to a create")
	}
	if strings.Contains(event.GetMessage(), "DATA IS GONE") {
		t.Errorf("nothing was destroyed, so the message must not say so:\n%s", event.GetMessage())
	}
	if !strings.Contains(event.GetMessage(), "Nothing was destroyed") {
		t.Errorf("the operator must be told their data survived:\n%s", event.GetMessage())
	}
}

// ★ TestPrecheckCarriesTheRealCreateBody — the property the whole pre-flight rests on,
// and the one the first round of its tests did not assert: replacing the body with any
// other would have left them all green while the probe proved nothing.
func TestPrecheckCarriesTheRealCreateBody(t *testing.T) {
	probe := precheckName("docs")
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)),
			liveCollection(t, defaultConfig(unnamedVector(8, "Cosine"), nil))).
		on("GET", "/collections/"+probe, notFoundResult("absent")).
		on("PUT", "/collections/"+probe, okTrue(t)).
		on("DELETE", "/collections/"+probe, okTrue(t)).
		on("GET", "/aliases", noAliases(t)).
		on("DELETE", "/collections/docs", okTrue(t)).
		on("PUT", "/collections/docs", okTrue(t))

	runApply(t, moduleWith(api).collection(), "recreated", baseParams(map[string]any{
		"name":            "docs",
		"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
		"shard_number":    3,
		"confirm_destroy": true,
	}))

	var probeBody, realBody string
	for _, c := range api.calls {
		switch c.key() {
		case "PUT /collections/" + probe:
			probeBody = c.bodyJSON(t)
		case "PUT /collections/docs":
			realBody = c.bodyJSON(t)
		}
	}
	if probeBody == "" || realBody == "" {
		t.Fatalf("expected both creates, got %v", api.pathsHit())
	}
	// Compare decoded, not textually: map ordering in the marshalled form is not the
	// property under test.
	var a, b map[string]any
	if err := json.Unmarshal([]byte(probeBody), &a); err != nil {
		t.Fatalf("probe body: %v", err)
	}
	if err := json.Unmarshal([]byte(realBody), &b); err != nil {
		t.Fatalf("real body: %v", err)
	}
	if !subsetEquals(a, b) || !subsetEquals(b, a) {
		t.Errorf("the pre-flight proves nothing unless it builds the SAME body:\n  probe: %s\n  real:  %s", probeBody, realBody)
	}
}

// TestUnknownKeyInsideAVectorIsRefused — `multivector_config` is per-vector AND
// creation-only, so an unknown key in it is permanent drift, which makes `recreated`
// drop and rebuild on every single run. The key check used to walk only the top level.
func TestUnknownKeyInsideAVectorIsRefused(t *testing.T) {
	params := baseParams(map[string]any{
		"name": "docs",
		"vectors": map[string]any{
			"size": 4, "distance": "Cosine",
			"multivector_config": map[string]any{"comparator": "max_sim", "bogus": 1},
		},
		"confirm_destroy": true,
	})

	if reply := runValidate(t, moduleWith(newFakeAPI(t)).collection(), "recreated", params); reply.GetOk() {
		t.Error("Validate must refuse a key Qdrant accepts and discards inside a creation-only map")
	}
	api := newFakeAPI(t)
	if !runApply(t, moduleWith(api).collection(), "recreated", params).final().GetFailed() {
		t.Error("Apply must refuse it too")
	}
	if len(api.calls) != 0 {
		t.Errorf("the refusal must reach nothing, called %v", api.pathsHit())
	}
}

// TestHnswMemoryIsNotAccepted — the collection-level twin of the vector `memory`
// already removed: accepted by Qdrant, never echoed back, so it can never converge and
// the step fails on every run blaming a legal declaration.
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

// TestPrecheckNameIsConstantLength — Qdrant caps a collection name at 255 (measured).
// A concatenated suffix made `recreated` fail on a legal 243-character name that every
// other state handles.
func TestPrecheckNameIsConstantLength(t *testing.T) {
	short := precheckName("a")
	long := precheckName(strings.Repeat("n", 250))
	if len(short) != len(long) {
		t.Errorf("the probe name must not grow with the subject: %d vs %d", len(short), len(long))
	}
	if len(long) > 255 {
		t.Errorf("the probe name must itself be a legal collection name, got %d characters", len(long))
	}
	if short == long {
		t.Error("different collections must get different probe names")
	}
}
