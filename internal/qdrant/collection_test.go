// Tests of the mutable/immutable split — the domain decision this ticket exists for.
//
// [planCollection] is a pure function of two maps, so all of it is checked here
// without a Qdrant. The live-side fixtures are copied from a real 1.18.3 response,
// defaults and all, because half of what can go wrong is a comparison that is right
// against a tidy fixture and wrong against what the server actually sends.
package qdrant

import (
	"strings"
	"testing"
)

// declared is a small builder for the declaration side, which arrives as decoded JSON.
func declared(vectors map[string]any, extra map[string]any) map[string]any {
	out := map[string]any{"vectors": vectors}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func conflictPathSet(plan collectionPlan) []string {
	out := make([]string, 0, len(plan.conflicts))
	for _, c := range plan.conflicts {
		out = append(out, c.path)
	}
	return out
}

// TestPlanIsEmptyWhenTheCollectionAlreadyMatches — the idempotency contract at its
// root: a declaration satisfied by the live collection produces no patch, no conflict
// and nothing to add, so the action sends no request at all.
func TestPlanIsEmptyWhenTheCollectionAlreadyMatches(t *testing.T) {
	live := defaultConfig(unnamedVector(4, "Cosine"), nil)
	plan := planCollection(declared(map[string]any{"size": float64(4), "distance": "Cosine"}, map[string]any{
		"shard_number":       float64(1),
		"replication_factor": float64(1),
	}), live)

	if !plan.empty() {
		t.Fatalf("plan is not empty on a matching collection:\n  conflicts=%v\n  patch=%v\n  add=%v",
			conflictPathSet(plan), plan.patch, plan.addVectors)
	}
}

// TestPartialDeclarationConvergesAgainstFilledDefaults — Qdrant reports every config
// map fully populated, so comparing whole maps would report drift on every run of
// every partial declaration, which is all of them. Only declared keys are compared.
func TestPartialDeclarationConvergesAgainstFilledDefaults(t *testing.T) {
	live := defaultConfig(unnamedVector(768, "Cosine"), nil)

	plan := planCollection(declared(map[string]any{"size": float64(768), "distance": "Cosine"}, map[string]any{
		// One key out of the five hnsw_config carries, and one out of the six in
		// optimizer_config. Both already match.
		"hnsw_config":       map[string]any{"m": float64(16)},
		"optimizers_config": map[string]any{"indexing_threshold": float64(10000)},
	}), live)

	if !plan.empty() {
		t.Fatalf("a partial declaration that already matches reported drift: patch=%v", plan.patch)
	}
}

// TestImmutableFieldsBecomeConflictsAndAreNeverPatched is the heart of the ticket.
//
// Every field here is one Qdrant ACCEPTS in a PATCH, answers 200 {"result":true} to,
// and discards. If any of them ever reached plan.patch, this artifact would send it,
// be told it succeeded, and report a change that did not happen — for ever, on every
// run.
func TestImmutableFieldsBecomeConflictsAndAreNeverPatched(t *testing.T) {
	cases := []struct {
		name     string
		declare  map[string]any
		vectors  map[string]any
		wantPath string
	}{
		{
			name:     "vector size",
			vectors:  map[string]any{"size": float64(8), "distance": "Cosine"},
			wantPath: `params.vectors."".size`,
		},
		{
			name:     "vector distance",
			vectors:  map[string]any{"size": float64(4), "distance": "Euclid"},
			wantPath: `params.vectors."".distance`,
		},
		{
			name:     "shard_number",
			declare:  map[string]any{"shard_number": float64(4)},
			wantPath: "params.shard_number",
		},
		{
			name:     "sharding_method",
			declare:  map[string]any{"sharding_method": "custom"},
			wantPath: "params.sharding_method",
		},
		{
			name:     "wal_config",
			declare:  map[string]any{"wal_config": map[string]any{"wal_capacity_mb": float64(64)}},
			wantPath: "params.wal_config",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vectors := tc.vectors
			if vectors == nil {
				vectors = map[string]any{"size": float64(4), "distance": "Cosine"}
			}
			live := defaultConfig(unnamedVector(4, "Cosine"), nil)
			plan := planCollection(declared(vectors, tc.declare), live)

			paths := conflictPathSet(plan)
			if len(paths) != 1 || paths[0] != tc.wantPath {
				t.Fatalf("conflicts = %v, want exactly [%s]", paths, tc.wantPath)
			}
			if len(plan.patch) != 0 {
				t.Fatalf("an immutable field reached the PATCH body: %v — Qdrant would answer 200 and discard it", plan.patch)
			}
		})
	}
}

// TestMutableFieldsArePatchedAtTheRightPath — the other side of the table, including
// the two path asymmetries that would each be a silent permanent-drift bug.
func TestMutableFieldsArePatchedAtTheRightPath(t *testing.T) {
	cases := []struct {
		name      string
		declare   map[string]any
		wantPatch string
	}{
		// The four scalars live at the TOP level of a create body and under `params`
		// in a patch body.
		{"replication_factor", map[string]any{"replication_factor": float64(3)}, `{"params":{"replication_factor":3}}`},
		{"write_consistency_factor", map[string]any{"write_consistency_factor": float64(2)}, `{"params":{"write_consistency_factor":2}}`},
		{"on_disk_payload", map[string]any{"on_disk_payload": false}, `{"params":{"on_disk_payload":false}}`},
		{"hnsw_config", map[string]any{"hnsw_config": map[string]any{"m": float64(32)}}, `{"hnsw_config":{"m":32}}`},
		// Written PLURAL, read back SINGULAR. If the write used the read spelling,
		// Qdrant would discard it as an unknown field and answer 200.
		{"optimizers_config", map[string]any{"optimizers_config": map[string]any{"indexing_threshold": float64(20000)}}, `{"optimizers_config":{"indexing_threshold":20000}}`},
		{"quantization_config", map[string]any{"quantization_config": map[string]any{"scalar": map[string]any{"type": "int8"}}}, `{"quantization_config":{"scalar":{"type":"int8"}}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			live := defaultConfig(unnamedVector(4, "Cosine"), nil)
			plan := planCollection(declared(map[string]any{"size": float64(4), "distance": "Cosine"}, tc.declare), live)

			if len(plan.conflicts) != 0 {
				t.Fatalf("a reconcilable field was refused: %v", conflictPathSet(plan))
			}
			if got := mustJSON(t, plan.patch); got != tc.wantPatch {
				t.Errorf("patch body = %s, want %s", got, tc.wantPatch)
			}
		})
	}
}

// TestOptimizerConfigSpellingIsNotDrift — the singular/plural asymmetry, asserted from
// the comparison side too: a declaration that MATCHES the live optimizer_config must
// produce no patch. Reading the wrong key would make every collection look like it
// needed patching, and the read-back guard would then fail every run.
func TestOptimizerConfigSpellingIsNotDrift(t *testing.T) {
	live := defaultConfig(unnamedVector(4, "Cosine"), nil)
	plan := planCollection(declared(map[string]any{"size": float64(4), "distance": "Cosine"}, map[string]any{
		"optimizers_config": map[string]any{"flush_interval_sec": float64(5)},
	}), live)

	if !plan.empty() {
		t.Fatalf("optimizers_config matching the live optimizer_config reported drift: %v", plan.patch)
	}
}

// TestShardingMethodDefaultIsNotDrift — Qdrant omits `sharding_method` from an
// auto-sharded collection. Since the field is immutable, reading the omission as drift
// would REFUSE every collection whose author spelled the default out.
func TestShardingMethodDefaultIsNotDrift(t *testing.T) {
	live := defaultConfig(unnamedVector(4, "Cosine"), nil)
	if _, present := pathValue(live, []string{"params", "sharding_method"}); present {
		t.Fatal("fixture drift: the live config should OMIT sharding_method when it is auto")
	}
	plan := planCollection(declared(map[string]any{"size": float64(4), "distance": "Cosine"}, map[string]any{
		"sharding_method": "auto",
	}), live)

	if !plan.empty() {
		t.Fatalf("declaring the default sharding_method was refused as drift: %v", conflictPathSet(plan))
	}
}

// TestNamedVectorMissingLiveIsAddedInPlace — adding a vector destroys nothing and
// Qdrant has an endpoint for it, so it is a change rather than a conflict.
func TestNamedVectorMissingLiveIsAddedInPlace(t *testing.T) {
	live := defaultConfig(map[string]any{
		"text": map[string]any{"size": float64(768), "distance": "Cosine"},
	}, nil)
	plan := planCollection(declared(map[string]any{
		"text":  map[string]any{"size": float64(768), "distance": "Cosine"},
		"image": map[string]any{"size": float64(512), "distance": "Dot"},
	}, nil), live)

	if len(plan.conflicts) != 0 {
		t.Fatalf("adding a named vector was refused: %v", conflictPathSet(plan))
	}
	if _, ok := plan.addVectors["image"]; !ok {
		t.Fatalf("addVectors = %v, want it to carry \"image\"", plan.addVectors)
	}
}

// TestNamedVectorUndeclaredIsRefusedNotDropped — the drop IS reachable in place
// (DELETE /collections/{c}/vectors/{name}), which is exactly why it has to be refused
// rather than done: reachable and safe are different properties, and that vector's
// data is the difference.
func TestNamedVectorUndeclaredIsRefusedNotDropped(t *testing.T) {
	live := defaultConfig(map[string]any{
		"text":  map[string]any{"size": float64(768), "distance": "Cosine"},
		"image": map[string]any{"size": float64(512), "distance": "Dot"},
	}, nil)
	plan := planCollection(declared(map[string]any{
		"text": map[string]any{"size": float64(768), "distance": "Cosine"},
	}, nil), live)

	if got := conflictPathSet(plan); len(got) != 1 || got[0] != "params.vectors.image" {
		t.Fatalf("conflicts = %v, want exactly [params.vectors.image]", got)
	}
	if len(plan.patch) != 0 {
		t.Fatalf("a vector removal leaked into the patch body: %v", plan.patch)
	}
}

// TestVectorFormChangeIsAConflict — the unnamed single vector and a set of named ones
// are different addressing models, not two spellings. Points written under one are
// unreachable through the other.
func TestVectorFormChangeIsAConflict(t *testing.T) {
	live := defaultConfig(unnamedVector(4, "Cosine"), nil)
	plan := planCollection(declared(map[string]any{
		"text": map[string]any{"size": float64(4), "distance": "Cosine"},
	}, nil), live)

	if len(plan.conflicts) == 0 {
		t.Fatal("moving from an unnamed vector to a named one was not refused")
	}
	if len(plan.addVectors) != 0 {
		t.Fatalf("the form change was treated as an addition: %v", plan.addVectors)
	}
}

// TestPerVectorMutableSettingsArePatched — inside a vector the split runs again:
// size/distance are fixed, the index and storage settings are not.
func TestPerVectorMutableSettingsArePatched(t *testing.T) {
	live := defaultConfig(map[string]any{
		"text": map[string]any{"size": float64(768), "distance": "Cosine", "on_disk": false},
	}, nil)
	plan := planCollection(declared(map[string]any{
		"text": map[string]any{"size": float64(768), "distance": "Cosine", "on_disk": true},
	}, nil), live)

	if len(plan.conflicts) != 0 {
		t.Fatalf("a reconcilable per-vector setting was refused: %v", conflictPathSet(plan))
	}
	if got := mustJSON(t, plan.patch); got != `{"vectors":{"text":{"on_disk":true}}}` {
		t.Errorf("patch body = %s", got)
	}
}

// TestConflictsAreSortedForDeterminism — the refusal message is built from this slice
// and is read in run logs and compared in tests; map iteration order must not reach it.
func TestConflictsAreSortedForDeterminism(t *testing.T) {
	live := defaultConfig(unnamedVector(4, "Cosine"), nil)
	decl := declared(map[string]any{"size": float64(8), "distance": "Euclid"}, map[string]any{
		"shard_number":    float64(4),
		"sharding_method": "custom",
	})

	first := conflictPathSet(planCollection(decl, live))
	for i := 0; i < 20; i++ {
		if got := conflictPathSet(planCollection(decl, live)); !equalStrings(got, first) {
			t.Fatalf("conflict order is unstable: %v then %v", first, got)
		}
	}
	if !sortedAscending(first) {
		t.Errorf("conflicts are not sorted: %v", first)
	}
}

// TestCreateBodyUsesCreationPathsAndTheAuthorsVectorSpelling — a create is the only
// moment the immutable half can be set, so all of it has to be in that body; and the
// vectors go out in the spelling the author used, since normalizing would turn a
// declaration of the unnamed vector into a collection with a vector literally named "".
func TestCreateBodyUsesCreationPathsAndTheAuthorsVectorSpelling(t *testing.T) {
	body := createBody(declared(map[string]any{"size": float64(4), "distance": "Cosine"}, map[string]any{
		"shard_number":       float64(2),
		"replication_factor": float64(2),
		"optimizers_config":  map[string]any{"indexing_threshold": float64(20000)},
		"wal_config":         map[string]any{"wal_capacity_mb": float64(64)},
	}))

	want := `{"optimizers_config":{"indexing_threshold":20000},"replication_factor":2,"shard_number":2,` +
		`"vectors":{"distance":"Cosine","size":4},"wal_config":{"wal_capacity_mb":64}}`
	if got := mustJSON(t, body); got != want {
		t.Errorf("create body =\n  %s\nwant\n  %s", got, want)
	}
}

// TestRenderValueIsDeterministicForMaps — conflict messages carry whole maps
// (wal_config), and Go prints a map in a random order.
func TestRenderValueIsDeterministicForMaps(t *testing.T) {
	v := map[string]any{"z": float64(1), "a": float64(2), "m": float64(3), "b": float64(4), "q": float64(5)}
	first := renderValue(v)
	for i := 0; i < 50; i++ {
		if got := renderValue(v); got != first {
			t.Fatalf("renderValue is unstable: %q then %q", first, got)
		}
	}
	if !strings.HasPrefix(first, "{a: ") {
		t.Errorf("renderValue does not sort keys: %s", first)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedAscending(in []string) bool {
	for i := 1; i < len(in); i++ {
		if in[i-1] > in[i] {
			return false
		}
	}
	return true
}

// --- ported from the plan-only suite, which this file absorbed ---

func TestNormalizeVectorsTellsTheTwoSpellingsApart(t *testing.T) {
	single := normalizeVectors(map[string]any{"size": float64(4), "distance": "Cosine"})
	if len(single) != 1 {
		t.Fatalf("the unnamed form must normalize to exactly one vector, got %v", sortedKeys(single))
	}
	if _, ok := single[""]; !ok {
		t.Errorf("the unnamed vector must be keyed by the empty string, got %v", sortedKeys(single))
	}

	named := normalizeVectors(map[string]any{
		"txt": map[string]any{"size": float64(8), "distance": "Dot"},
		"img": map[string]any{"size": float64(512), "distance": "Cosine"},
	})
	if got := sortedKeys(named); !equalStrings(got, []string{"img", "txt"}) {
		t.Errorf("named form: got %v, want [img txt]", got)
	}
}

// TestSubsetEqualsComparesOnlyWhatIsDeclared — the rule that makes a partial
// declaration converge. Its cost is stated in collection.go: this module cannot express
// "put that back to the default".
func TestSubsetEqualsComparesOnlyWhatIsDeclared(t *testing.T) {
	live := map[string]any{"m": float64(16), "ef_construct": float64(100), "on_disk": false}

	if !subsetEquals(map[string]any{"m": float64(16)}, live) {
		t.Error("a declared subset that matches must compare equal")
	}
	if subsetEquals(map[string]any{"m": float64(32)}, live) {
		t.Error("a declared key with a different value must not compare equal")
	}
	if subsetEquals(map[string]any{"payload_m": float64(8)}, live) {
		t.Error("a declared key absent from the live config must not compare equal")
	}
	// The asymmetry is the point: live may carry more than the declaration.
	if !subsetEquals(map[string]any{}, live) {
		t.Error("an empty declaration must compare equal to anything")
	}
}

// TestConflictMessageNamesBothValues — the refusal is only useful if it says what to
// change. An operator who reads it must learn the setting, what it is now, and what
// they asked for, without opening the plugin.
func TestConflictMessageNamesBothValues(t *testing.T) {
	plan := planCollection(
		map[string]any{"vectors": unnamedVector(8, "Cosine"), "shard_number": float64(4)},
		defaultConfig(unnamedVector(4, "Cosine"), nil),
	)
	text := refusalText("docs", plan.conflicts)

	for _, want := range []string{"docs", `params.vectors."".size`, "params.shard_number", "recreated", "confirm_destroy"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal must mention %q:\n%s", want, text)
		}
	}
	// Both the live value and the declared one, so the reader knows which side to fix.
	if !strings.Contains(text, "is 4") || !strings.Contains(text, "declared 8") {
		t.Errorf("the refusal must carry the live AND the declared value:\n%s", text)
	}
}

// TestRenderValuePrintsWholeNumbersWithoutADecimalPoint — every number arrives as a
// float64 through both structpb and encoding/json, and "shard_number is 2e+00" in a
// message an operator reads is a defect of its own.
func TestRenderValuePrintsWholeNumbersWithoutADecimalPoint(t *testing.T) {
	cases := map[string]struct {
		in   any
		want string
	}{
		"whole float":    {float64(2), "2"},
		"fractional":     {0.2, "0.2"},
		"string":         {"Cosine", `"Cosine"`},
		"bool":           {true, "true"},
		"absent":         {nil, "unset"},
		"large fraction": {1.5, "1.5"},
	}
	for name, tc := range cases {
		if got := renderValue(tc.in); got != tc.want {
			t.Errorf("%s: renderValue(%v) = %q, want %q", name, tc.in, got, tc.want)
		}
	}
}
