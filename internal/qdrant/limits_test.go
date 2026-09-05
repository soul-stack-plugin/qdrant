// Tests for the guard that stands between a typo and a destroyed collection.
//
// `collection.present` creates a collection when one is missing, and a body Qdrant
// rejects fails that create. The bounds in limits.go turn the common mistake into a
// message that names the parameter instead of a 400 relayed from the server.
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
