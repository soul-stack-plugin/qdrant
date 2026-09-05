// Tests for the guard that stands between a typo and a destroyed collection.
//
// `collection.recreated` drops the collection and then creates it again. Nothing rolls
// the drop back and Qdrant has no dry-run create, so a create body Qdrant would refuse
// ends the run with the data already gone. The bounds in limits.go are the only thing
// that can catch that, and these tests are the only thing that keeps them wired into
// the destructive path.
package qdrant

import (
	"strings"
	"testing"
)

// TestBoundsRejectWhatQdrantWouldRefuse — the values are Qdrant's own documented
// limits, read out of its OpenAPI schema.
func TestBoundsRejectWhatQdrantWouldRefuse(t *testing.T) {
	cases := []struct {
		name     string
		declared map[string]any
		wantAddr string
	}{
		{
			name:     "shard_number below one",
			declared: map[string]any{"shard_number": float64(0)},
			wantAddr: "params.shard_number",
		},
		{
			name:     "replication_factor below one",
			declared: map[string]any{"replication_factor": float64(0)},
			wantAddr: "params.replication_factor",
		},
		{
			name:     "write_consistency_factor below one",
			declared: map[string]any{"write_consistency_factor": float64(0)},
			wantAddr: "params.write_consistency_factor",
		},
		{
			name:     "vector size above the ceiling",
			declared: map[string]any{"vectors": map[string]any{"size": float64(100000), "distance": "Cosine"}},
			wantAddr: `params.vectors."".size`,
		},
		{
			name:     "vector size below one",
			declared: map[string]any{"vectors": map[string]any{"size": float64(0), "distance": "Cosine"}},
			wantAddr: `params.vectors."".size`,
		},
		{
			name:     "ef_construct below the floor",
			declared: map[string]any{"hnsw_config": map[string]any{"ef_construct": float64(2)}},
			wantAddr: "params.hnsw_config.ef_construct",
		},
		{
			name:     "full_scan_threshold below the floor",
			declared: map[string]any{"hnsw_config": map[string]any{"full_scan_threshold": float64(5)}},
			wantAddr: "params.hnsw_config.full_scan_threshold",
		},
		{
			name:     "vacuum_min_vector_number below the floor",
			declared: map[string]any{"optimizers_config": map[string]any{"vacuum_min_vector_number": float64(10)}},
			wantAddr: "params.optimizers_config.vacuum_min_vector_number",
		},
		{
			name:     "deleted_threshold outside 0..1",
			declared: map[string]any{"optimizers_config": map[string]any{"deleted_threshold": 1.5}},
			wantAddr: "params.optimizers_config.deleted_threshold",
		},
		{
			name: "a named vector's own hnsw floor",
			declared: map[string]any{"vectors": map[string]any{
				"txt": map[string]any{"size": float64(8), "distance": "Dot", "hnsw_config": map[string]any{"ef_construct": float64(1)}},
			}},
			wantAddr: "params.vectors.txt.hnsw_config.ef_construct",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := checkCollectionBounds(tc.declared)
			if len(errs) == 0 {
				t.Fatalf("expected %s to be refused, got no error", tc.wantAddr)
			}
			if !strings.Contains(errs[0], tc.wantAddr) {
				t.Errorf("error must address %s, got %q", tc.wantAddr, errs[0])
			}
		})
	}
}

// TestBoundsAcceptWhatQdrantAccepts — the other direction matters just as much. A bound
// invented here would refuse a declaration a real Qdrant takes, which is the opposite
// failure and just as obstructive.
func TestBoundsAcceptWhatQdrantAccepts(t *testing.T) {
	declared := map[string]any{
		"vectors":                  map[string]any{"size": float64(65536), "distance": "Cosine"},
		"shard_number":             float64(1),
		"replication_factor":       float64(1),
		"write_consistency_factor": float64(1),
		"hnsw_config":              map[string]any{"m": float64(0), "ef_construct": float64(4), "full_scan_threshold": float64(10)},
		"optimizers_config": map[string]any{
			"deleted_threshold": float64(0), "vacuum_min_vector_number": float64(100),
			"default_segment_number": float64(0), "flush_interval_sec": float64(0),
		},
	}
	if errs := checkCollectionBounds(declared); len(errs) != 0 {
		t.Errorf("a declaration at Qdrant's exact limits must be accepted, got %v", errs)
	}
}

// TestBoundsIgnoreWrongTypes — a value of the wrong type is checkParamTypes' business.
// Reporting it here too would give one mistake two different messages.
func TestBoundsIgnoreWrongTypes(t *testing.T) {
	if errs := checkCollectionBounds(map[string]any{"shard_number": "two"}); len(errs) != 0 {
		t.Errorf("a non-number must be left to the type check, got %v", errs)
	}
}

// ★ TestRecreatedRefusesOutOfRangeBeforeDroppingAnything is the regression test for the
// defect this file exists for.
//
// The declaration below asks for something only a rebuild could deliver (a changed
// vector size), carries confirm_destroy, and ALSO carries a shard_number Qdrant would
// refuse. Before the fix, the sequence was: plan sees a conflict → DELETE succeeds →
// PUT fails 400 → the collection and every point in it are gone for good.
//
// The assertion that matters is not the message, it is that NOTHING was sent.
func TestRecreatedRefusesOutOfRangeBeforeDroppingAnything(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)))

	stream := runApply(t, moduleWith(api).collection(), "recreated", baseParams(map[string]any{
		"name":            "docs",
		"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
		"shard_number":    0,
		"confirm_destroy": true,
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("a create body Qdrant would refuse must stop the run BEFORE the drop: %s", event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Fatalf("THE COLLECTION WAS TOUCHED: %v — a refusal after the DELETE is a destroyed collection", sent)
	}
	// Stronger than the property under test: the bounds run in Apply's own validation
	// pass, so this refusal lands before a socket is even opened. The guard inside
	// reconcileCollection, just above the DELETE, is defence in depth for the day
	// those two stop agreeing — TestBoundsRejectWhatQdrantWouldRefuse covers it
	// directly.
	if len(api.calls) != 0 {
		t.Errorf("the refusal should reach nothing at all, called %v", api.pathsHit())
	}
	if !strings.Contains(event.GetMessage(), "params.shard_number") {
		t.Errorf("the refusal must name the offending param:\n%s", event.GetMessage())
	}
}

// TestRecreatedSaysTheDataIsGoneWhenTheRebuildFails — the case the guard above cannot
// cover: the create is refused for a reason no bound predicts (a 503, a limit this
// build does not know). The collection is already dropped, and the operator has to be
// told that first, or they re-run a scenario whose later steps assume the data is there.
func TestRecreatedSaysTheDataIsGoneWhenTheRebuildFails(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		// The pre-flight passes — this is the case it cannot cover: the body is
		// acceptable, and the real create fails for a reason no check predicts.
		on("GET", "/collections/docs__ss_precheck", notFoundResult("Collection `docs__ss_precheck` doesn't exist!")).
		on("PUT", "/collections/docs__ss_precheck", okTrue(t)).
		on("DELETE", "/collections/docs__ss_precheck", okTrue(t)).
		on("DELETE", "/collections/docs", okTrue(t)).
		on("PUT", "/collections/docs", errorResult(503, "service unavailable"))

	stream := runApply(t, moduleWith(api).collection(), "recreated", baseParams(map[string]any{
		"name":            "docs",
		"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
		"confirm_destroy": true,
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatal("a failed rebuild must fail the step")
	}
	for _, want := range []string{"DROPPED", "GONE", "does not exist right now"} {
		if !strings.Contains(event.GetMessage(), want) {
			t.Errorf("the failure must say the data is gone, not just that the create failed (%q missing):\n%s", want, event.GetMessage())
		}
	}
}
