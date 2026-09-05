// Tests for the pre-flight create — the structural answer to a defect three reviews
// found three different instances of.
//
// Each round produced another value that limits.go did not know Qdrant would refuse,
// arriving as a 400 (or, for `wal_retain_closed: 0`, a 500 panic) AFTER `recreated` had
// already dropped the collection. Enumerating Qdrant's validation rules here will never
// be finished, so the module stops predicting and asks: it builds the declared body
// under a throwaway name first, and only destroys anything once the server has accepted
// it.
package qdrant

import (
	"strings"
	"testing"
)

// recreateParams is a declaration that needs a rebuild — a changed vector size.
func recreateParams(extra map[string]any) map[string]any {
	p := baseParams(map[string]any{
		"name":            "docs",
		"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
		"confirm_destroy": true,
	})
	for k, v := range extra {
		p[k] = v
	}
	return p
}

// liveFour is the collection as it stands: size 4, so the declaration above conflicts.
func liveFour(t *testing.T) apiResult {
	t.Helper()
	return liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))
}

// ★ TestPrecheckStopsBeforeTheDropWhenTheServerRefusesTheBody — the whole point. The
// server rejects the probe, so the real collection is never touched.
//
// The fake plays the measured behaviour of `wal_retain_closed: 0` on a live 1.18.3: a
// 500, not even a clean 400. No table of bounds could have been trusted to know that,
// and this test does not need one to.
func TestPrecheckStopsBeforeTheDropWhenTheServerRefusesTheBody(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveFour(t)).
		on("GET", "/collections/docs__ss_precheck", notFoundResult("Collection `docs__ss_precheck` doesn't exist!")).
		on("PUT", "/collections/docs__ss_precheck",
			errorResult(500, `Service internal error: Tokio task join error: task panicked with message "called `+"`Option::unwrap()`"+` on a `+"`None`"+` value"`))

	stream := runApply(t, moduleWith(api).collection(), "recreated", recreateParams(nil))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("a body the server refuses must stop the run: %s", event.GetMessage())
	}
	for _, c := range api.mutating() {
		if c.path == "/collections/docs" {
			t.Fatalf("THE REAL COLLECTION WAS TOUCHED: %s — the whole point is that it is not", c.key())
		}
	}
	if !strings.Contains(event.GetMessage(), "NOT touched") {
		t.Errorf("the operator must be told the collection survived:\n%s", event.GetMessage())
	}
}

// TestPrecheckRemovesItsOwnProbe — a pre-flight that left an empty collection behind on
// every run would be its own kind of mess, and would collide with the next run.
func TestPrecheckRemovesItsOwnProbe(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveFour(t), liveCollection(t, defaultConfig(unnamedVector(8, "Cosine"), nil))).
		on("GET", "/collections/docs__ss_precheck", notFoundResult("Collection `docs__ss_precheck` doesn't exist!")).
		on("PUT", "/collections/docs__ss_precheck", okTrue(t)).
		on("DELETE", "/collections/docs__ss_precheck", okTrue(t)).
		on("DELETE", "/collections/docs", okTrue(t)).
		on("PUT", "/collections/docs", okTrue(t))

	if event := runApply(t, moduleWith(api).collection(), "recreated", recreateParams(nil)).final(); event.GetFailed() {
		t.Fatalf("a valid rebuild must succeed: %s", event.GetMessage())
	}

	var probeDeleted bool
	for _, c := range api.mutating() {
		if c.key() == "DELETE /collections/docs__ss_precheck" {
			probeDeleted = true
		}
	}
	if !probeDeleted {
		t.Errorf("the probe must be removed, calls were %v", api.pathsHit())
	}
}

// TestPrecheckRunsBeforeTheDrop — ordering is the property, not the presence of the
// calls. A pre-flight that ran after the DELETE would prove nothing.
func TestPrecheckRunsBeforeTheDrop(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveFour(t), liveCollection(t, defaultConfig(unnamedVector(8, "Cosine"), nil))).
		on("GET", "/collections/docs__ss_precheck", notFoundResult("Collection `docs__ss_precheck` doesn't exist!")).
		on("PUT", "/collections/docs__ss_precheck", okTrue(t)).
		on("DELETE", "/collections/docs__ss_precheck", okTrue(t)).
		on("DELETE", "/collections/docs", okTrue(t)).
		on("PUT", "/collections/docs", okTrue(t))

	runApply(t, moduleWith(api).collection(), "recreated", recreateParams(nil))

	probeCreated, realDropped := -1, -1
	for i, c := range api.calls {
		switch c.key() {
		case "PUT /collections/docs__ss_precheck":
			probeCreated = i
		case "DELETE /collections/docs":
			realDropped = i
		}
	}
	if probeCreated < 0 || realDropped < 0 {
		t.Fatalf("expected both a probe create and a real drop, got %v", api.pathsHit())
	}
	if probeCreated > realDropped {
		t.Errorf("the pre-flight must precede the drop; it ran at %d and the drop at %d", probeCreated, realDropped)
	}
}

// TestPrecheckWillNotClobberAnExistingCollection — the probe name is unlikely, but
// "unlikely" is not a licence to delete somebody's data.
func TestPrecheckWillNotClobberAnExistingCollection(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveFour(t)).
		on("GET", "/collections/docs__ss_precheck", liveFour(t))

	event := runApply(t, moduleWith(api).collection(), "recreated", recreateParams(nil)).final()

	if !event.GetFailed() {
		t.Fatal("an occupied probe name must stop the run rather than overwrite it")
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("nothing may be sent, sent %v", sent)
	}
}

// TestPresentNeverRunsAPrecheck — `present` cannot destroy anything, so it has nothing
// to pre-flight. Sending a create it does not need would be a side effect on a state
// whose whole contract is that it refuses instead.
func TestPresentNeverRunsAPrecheck(t *testing.T) {
	api := newFakeAPI(t).on("GET", "/collections/docs", liveFour(t))

	event := runApply(t, moduleWith(api).collection(), "present", baseParams(map[string]any{
		"name":    "docs",
		"vectors": map[string]any{"size": 8, "distance": "Cosine"},
	})).final()

	if !event.GetFailed() {
		t.Fatal("present must refuse an immutable change")
	}
	for _, c := range api.calls {
		if strings.Contains(c.path, "__ss_precheck") {
			t.Errorf("present has nothing to pre-flight, yet it called %s", c.key())
		}
	}
}

// TestUnknownKeyInACreationOnlyMapIsRefused — the pre-flight cannot catch this one, and
// it is the worst of the family: Qdrant ACCEPTS an unknown key and discards it, so the
// probe succeeds. Because wal_config is creation-only the key can never appear in the
// live config, which makes it permanent drift — and permanent drift on a creation-only
// setting means `recreated` drops and rebuilds on EVERY run. A typo becomes a scheduled
// deletion.
func TestUnknownKeyInACreationOnlyMapIsRefused(t *testing.T) {
	params := recreateParams(map[string]any{
		"wal_config": map[string]any{"wal_capacity_mbb": 64},
	})

	if reply := runValidate(t, moduleWith(newFakeAPI(t)).collection(), "recreated", params); reply.GetOk() {
		t.Error("Validate must refuse a key Qdrant would discard in silence")
	}

	api := newFakeAPI(t)
	if !runApply(t, moduleWith(api).collection(), "recreated", params).final().GetFailed() {
		t.Error("Apply must refuse it too — the runtime calls Apply")
	}
	if len(api.calls) != 0 {
		t.Errorf("the refusal must reach nothing, called %v", api.pathsHit())
	}
}

// TestWalRetainClosedFloorIsOne — the floor came from Qdrant's UPDATE schema
// (WalConfigDiff, minimum 0) where the COLLECTION schema is stricter (WalConfig,
// minimum 1). Zero is not a clean rejection: measured on 1.18.3 it panics the server.
func TestWalRetainClosedFloorIsOne(t *testing.T) {
	if errs := checkCollectionBounds(map[string]any{
		"wal_config": map[string]any{"wal_retain_closed": float64(0)},
	}); len(errs) == 0 {
		t.Error("wal_retain_closed 0 must be refused — it panics Qdrant into a 500")
	}
	if errs := checkCollectionBounds(map[string]any{
		"wal_config": map[string]any{"wal_retain_closed": float64(1)},
	}); len(errs) != 0 {
		t.Errorf("1 is the documented default and must be accepted, got %v", errs)
	}
}

// TestOnDiskDefaultIsNotDrift — `on_disk` is echoed only when it was set explicitly,
// like `datatype`. Being mutable this costs a pointless PATCH rather than a drop, but a
// state reporting changed=true for a setting the collection already had is still a lie.
func TestOnDiskDefaultIsNotDrift(t *testing.T) {
	plan := planCollection(
		map[string]any{"vectors": map[string]any{
			"size": float64(4), "distance": "Cosine", "on_disk": false,
		}},
		defaultConfig(unnamedVector(4, "Cosine"), nil),
	)
	if !plan.empty() {
		t.Errorf("declaring the stored default must not be drift, got patch %v", plan.patch)
	}
}
