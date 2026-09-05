// Regressions for the second review's two criticals — both of them the same shape as
// the first review's: a path that reaches the DELETE and then fails, or one that
// reaches it for no reason at all.
package qdrant

import (
	"strings"
	"testing"
)

// ★ TestDeclaringAVectorDefaultIsNotDrift — measured on a live 1.18.3: Qdrant echoes a
// vector's `datatype` back ONLY when it was set explicitly. A collection created
// without one reports `{size, distance}`, and stores float32.
//
// So a declaration that spells the default out used to read as drift on a
// creation-only key — a conflict, which is a PERMANENT refusal of a declaration the
// collection already satisfies.
func TestDeclaringAVectorDefaultIsNotDrift(t *testing.T) {
	plan := planCollection(
		map[string]any{"vectors": map[string]any{
			"size": float64(8), "distance": "Cosine", "datatype": "float32",
		}},
		// As Qdrant reports it: no datatype key at all.
		defaultConfig(unnamedVector(8, "Cosine"), nil),
	)

	if !plan.empty() {
		t.Fatalf("declaring the stored default must not be drift, got conflicts %v patch %v",
			conflictPaths(plan.conflicts), sortedKeys(plan.patch))
	}
}

// And the other direction must still work: a datatype that genuinely differs from what
// the collection holds is creation-only, so it is a conflict rather than a patch.
func TestADifferentDatatypeIsStillAConflict(t *testing.T) {
	plan := planCollection(
		map[string]any{"vectors": map[string]any{
			"size": float64(8), "distance": "Cosine", "datatype": "uint8",
		}},
		defaultConfig(unnamedVector(8, "Cosine"), nil),
	)

	if got := conflictPaths(plan.conflicts); len(got) != 1 || !strings.Contains(got[0], "datatype") {
		t.Fatalf("a datatype the collection does not have must be a conflict, got %v", got)
	}
	if len(plan.patch) != 0 {
		t.Errorf("a creation-only key must never be patched, got %v", plan.patch)
	}
}

// TestPresentDoesNotRefuseADeclaredDefault is the same fact asserted where an author
// meets it: through Apply, on the state they actually write.
func TestPresentDoesNotRefuseADeclaredDefault(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(8, "Cosine"), nil)))

	stream := runApply(t, moduleWith(api).collection(), "present", baseParams(map[string]any{
		"name": "docs",
		"vectors": map[string]any{
			"size": 8, "distance": "Cosine", "datatype": "float32",
		},
	}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("a collection that already matches must be left alone: failed=%v changed=%v %s",
			event.GetFailed(), event.GetChanged(), event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Fatalf("a converged run must send nothing, sent %v", sent)
	}
}

// TestMemoryIsNotManaged — Qdrant's VectorParamsDiff lists `memory`, but a live 1.18.3
// never echoes it back, whether it was patched or set at creation. This module decides
// `changed` by reading the resource back, so a key it cannot read is a key it cannot
// honestly manage: declaring it would fail the read-back on EVERY run and blame the
// author. It is refused up front instead.
func TestMemoryIsNotManaged(t *testing.T) {
	if _, managed := vectorFields["memory"]; managed {
		t.Fatal("`memory` cannot be managed: Qdrant never reports it, so no read-back can confirm it")
	}

	reply := runValidate(t, moduleWith(newFakeAPI(t)).collection(), "present",
		baseParams(map[string]any{
			"name":    "docs",
			"vectors": map[string]any{"size": 4, "distance": "Cosine", "memory": "cold"},
		}))
	if reply.GetOk() {
		t.Error("an unmanaged vector key must be refused rather than sent and silently dropped")
	}
}

// TestWalConfigIsBoundsChecked — `wal_capacity_mb: 0` answers 422 "must be 1 or larger"
// and `wal_retain_closed: 0` panics the server into a 500, both measured. Either would
// otherwise arrive as a relayed error from the create.
func TestWalConfigIsBoundsChecked(t *testing.T) {
	errs := checkCollectionBounds(map[string]any{
		"wal_config": map[string]any{"wal_capacity_mb": float64(0)},
	})
	if len(errs) == 0 {
		t.Fatal("wal_capacity_mb 0 must be refused: Qdrant answers 422 to the create")
	}
	if !strings.Contains(errs[0], "params.wal_config.wal_capacity_mb") {
		t.Errorf("the error must address the field, got %q", errs[0])
	}
}

// TestIntegerFieldsRefuseAFraction — Qdrant's optimizer and shard counters are u64/u32,
// so a fractional value is a 400 rather than a rounded one. `flush_interval_sec` was
// declared as an unconstrained number and let one through.
func TestIntegerFieldsRefuseAFraction(t *testing.T) {
	errs := checkCollectionBounds(map[string]any{
		"optimizers_config": map[string]any{"flush_interval_sec": 1.5},
	})
	if len(errs) == 0 {
		t.Fatal("a fractional flush_interval_sec must be refused: Qdrant answers 400 `expected u64`")
	}
}

// TestIntegerFieldsHaveACeiling — a value that merely looks large is a 400
// "expected u32" from the create.
func TestIntegerFieldsHaveACeiling(t *testing.T) {
	errs := checkCollectionBounds(map[string]any{"shard_number": float64(4294967296)})
	if len(errs) == 0 {
		t.Fatal("shard_number above uint32 must be refused: Qdrant answers 400 `expected u32`")
	}
	// And the largest legal value must still pass.
	if errs := checkCollectionBounds(map[string]any{"shard_number": float64(4294967295)}); len(errs) != 0 {
		t.Errorf("the largest legal shard_number must be accepted, got %v", errs)
	}
}

// TestUnnamedVectorIsNeverAdded — the add endpoint puts the vector name in the URL
// path, so an empty one addresses `/vectors/` and answers 404. A collection built on
// sparse vectors alone reports `params.vectors: {}` (measured), which slipped past the
// form check and turned into an attempt to add the unnamed vector.
func TestUnnamedVectorIsNeverAdded(t *testing.T) {
	sparseOnly := defaultConfig(map[string]any{}, nil)

	plan := planCollection(
		map[string]any{"vectors": map[string]any{"size": float64(4), "distance": "Cosine"}},
		sparseOnly,
	)

	if _, added := plan.addVectors[""]; added {
		t.Fatal("the unnamed vector has no add endpoint — this would PUT /vectors/ and 404")
	}
	if len(plan.conflicts) == 0 {
		t.Fatal("a collection with no unnamed vector to match is a layout disagreement, not a vector to create")
	}
	if got := conflictPaths(plan.conflicts); !strings.Contains(got[0], "params.vectors") {
		t.Errorf("the conflict must address the layout, got %v", got)
	}
}

// TestConvergedRunReportsExists — a gate written against a key has to find it on every
// run, and the converged run is the one it meets most often.
func TestConvergedRunReportsExists(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)))

	stream := runApply(t, moduleWith(api).collection(), "present", baseParams(map[string]any{
		"name":    "docs",
		"vectors": map[string]any{"size": 4, "distance": "Cosine"},
	}))

	event := stream.final()
	if event.GetChanged() {
		t.Fatalf("expected a converged run: %s", event.GetMessage())
	}
	if !event.GetOutput().GetFields()["exists"].GetBoolValue() {
		t.Error("Output.exists must be true on a converged run, not only on one that changed something")
	}
}
