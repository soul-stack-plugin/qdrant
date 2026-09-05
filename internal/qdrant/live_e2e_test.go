//go:build e2e_live

// The live tier: this artifact against a REAL Qdrant, over its real HTTP transport.
//
// Behind the `e2e_live` build tag, the same way the redis artifact guards its live
// reshard — the framework compiles in the ordinary gate but does not run without an
// instance and an explicit opt-in. Run it with:
//
//	docker run -d --name qd -p 16333:6333 qdrant/qdrant:latest
//	QDRANT_ADDR=127.0.0.1:16333 GOWORK=off go test -tags e2e_live -count=1 -run Live .
//
// What it proves that the L0 fakes cannot. Every fake in this package encodes what a
// live Qdrant was OBSERVED to do — that it answers 200 to a PATCH it discards, that it
// omits `sharding_method` when it is auto, that it reads `optimizer_config` back
// singular. Those observations are the foundation the whole `collection` object is
// built on, and a fixture cannot check its own premise. This tier does: it puts the
// real server behind the same code and asserts the outcomes, so a future Qdrant that
// starts REFUSING an immutable PATCH — which would be a welcome change and would make
// several comments here stale — shows up as a failure rather than as a silent drift
// between this module's model and the thing it models.
package qdrant

import (
	"os"
	"strings"
	"testing"
)

func liveAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("QDRANT_ADDR")
	if addr == "" {
		t.Skip("QDRANT_ADDR is not set; start a Qdrant and point this at it")
	}
	return addr
}

// liveParams builds a param set against the live instance.
func liveParams(t *testing.T, extra map[string]any) map[string]any {
	out := map[string]any{"addr": liveAddr(t)}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// liveModule talks over the real net/http client — no injection point.
func liveModule() *Module { return &Module{} }

// dropLive removes a collection so a test starts from a known state.
func dropLive(t *testing.T, name string) {
	t.Helper()
	runApply(t, liveModule().collection(), "absent", liveParams(t, map[string]any{"name": name}))
}

// TestLiveInstanceProbes — the three read probes against a real server.
func TestLiveInstanceProbes(t *testing.T) {
	obj := liveModule().instance()

	pinged := runApply(t, obj, "pinged", liveParams(t, nil)).final()
	if pinged.GetFailed() {
		t.Fatalf("pinged failed: %s", pinged.GetMessage())
	}
	if pinged.GetChanged() {
		t.Error("a probe must never report changed")
	}
	if got := pinged.GetOutput().GetFields()["result"].GetStringValue(); !strings.Contains(got, "healthz") {
		t.Errorf("Output.result = %q, want the server's own healthz line", got)
	}

	ready := runApply(t, obj, "ready-probed", liveParams(t, nil)).final()
	if ready.GetFailed() {
		t.Fatalf("ready-probed failed: %s", ready.GetMessage())
	}
	if !ready.GetOutput().GetFields()["ready"].GetBoolValue() {
		t.Errorf("a started Qdrant must report ready: %s", ready.GetMessage())
	}

	version := runApply(t, obj, "version-probed", liveParams(t, nil)).final()
	if version.GetFailed() {
		t.Fatalf("version-probed failed: %s", version.GetMessage())
	}
	if got := version.GetOutput().GetFields()["version"].GetStringValue(); got == "" {
		t.Error("Output.version is empty")
	} else {
		t.Logf("live Qdrant version: %s", got)
	}
}

// TestLiveCollectionLifecycle — create, converge, patch, converge again.
func TestLiveCollectionLifecycle(t *testing.T) {
	const name = "ss_live_lifecycle"
	obj := liveModule().collection()
	dropLive(t, name)
	t.Cleanup(func() { dropLive(t, name) })

	decl := func(extra map[string]any) map[string]any {
		p := map[string]any{
			"name":    name,
			"vectors": map[string]any{"size": 4, "distance": "Cosine"},
		}
		for k, v := range extra {
			p[k] = v
		}
		return liveParams(t, p)
	}

	created := runApply(t, obj, "present", decl(nil)).final()
	if created.GetFailed() {
		t.Fatalf("create failed: %s", created.GetMessage())
	}
	if !created.GetChanged() {
		t.Error("creating a collection must report changed=true")
	}

	// ★ Idempotency against the real server, which is the claim a fake cannot make:
	// the live config comes back populated with every default Qdrant chose, and the
	// declaration is a fraction of it.
	again := runApply(t, obj, "present", decl(nil)).final()
	if again.GetFailed() {
		t.Fatalf("the second run failed: %s", again.GetMessage())
	}
	if again.GetChanged() {
		t.Errorf("a second run must be a no-op, got changed=true: %s", again.GetMessage())
	}

	// A reconcilable field really reconciles.
	patched := runApply(t, obj, "present", decl(map[string]any{
		"on_disk_payload": false,
		"hnsw_config":     map[string]any{"m": 32},
	})).final()
	if patched.GetFailed() {
		t.Fatalf("patching a reconcilable field failed: %s", patched.GetMessage())
	}
	if !patched.GetChanged() {
		t.Error("changing on_disk_payload and hnsw_config.m must report changed=true")
	}

	// ...and converges: the read-back guard would have failed the step above if the
	// change had not landed, so this asserts the second half — no perpetual drift.
	settled := runApply(t, obj, "present", decl(map[string]any{
		"on_disk_payload": false,
		"hnsw_config":     map[string]any{"m": 32},
	})).final()
	if settled.GetChanged() {
		t.Errorf("the patched collection did not converge: %s", settled.GetMessage())
	}
}

// TestLivePresentRefusesAnImmutableChangeAndTouchesNothing is the ticket's whole point,
// proved end to end.
//
// A live Qdrant ACCEPTS `size: 8` on a collection created with 4 — 200, {"result":true}
// — and discards it. So the wrong implementation is not merely imprecise here: it
// reports a reconciled collection, forever, on a collection that never moved. This
// asserts the refusal AND that the collection is unharmed afterwards.
func TestLivePresentRefusesAnImmutableChangeAndTouchesNothing(t *testing.T) {
	const name = "ss_live_immutable"
	obj := liveModule().collection()
	dropLive(t, name)
	t.Cleanup(func() { dropLive(t, name) })

	base := liveParams(t, map[string]any{
		"name":         name,
		"vectors":      map[string]any{"size": 4, "distance": "Cosine"},
		"shard_number": 2,
	})
	if e := runApply(t, obj, "present", base).final(); e.GetFailed() {
		t.Fatalf("create failed: %s", e.GetMessage())
	}

	for _, tc := range []struct {
		name    string
		mutate  func(map[string]any)
		wantHit string
	}{
		{"vector size", func(p map[string]any) { p["vectors"] = map[string]any{"size": 8, "distance": "Cosine"} }, `params.vectors."".size`},
		{"distance", func(p map[string]any) { p["vectors"] = map[string]any{"size": 4, "distance": "Euclid"} }, `params.vectors."".distance`},
		{"shard_number", func(p map[string]any) { p["shard_number"] = 4 }, "params.shard_number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := liveParams(t, map[string]any{
				"name":         name,
				"vectors":      map[string]any{"size": 4, "distance": "Cosine"},
				"shard_number": 2,
			})
			tc.mutate(params)

			event := runApply(t, obj, "present", params).final()
			if !event.GetFailed() {
				t.Fatalf("an immutable change must be refused, got changed=%v: %s", event.GetChanged(), event.GetMessage())
			}
			if !strings.Contains(event.GetMessage(), tc.wantHit) {
				t.Errorf("the refusal must name %s:\n%s", tc.wantHit, event.GetMessage())
			}
			// The refusal has to leave the author with a way forward. This module
			// ships no state that rebuilds a collection, so the way forward is three
			// declarations that each say what they do — and the snapshot has to come
			// first, or the advice is a data-loss instruction.
			for _, want := range []string{"snapshot", "absent", "present"} {
				if !strings.Contains(event.GetMessage(), want) {
					t.Errorf("the refusal must say what to do instead (%q missing):\n%s", want, event.GetMessage())
				}
			}
		})
	}

	// The collection is exactly as it was: refusing means refusing, not "refusing
	// after doing the reachable half".
	probe := runApply(t, obj, "probed", liveParams(t, map[string]any{"name": name})).final()
	if !probe.GetOutput().GetFields()["exists"].GetBoolValue() {
		t.Fatal("the refused runs destroyed the collection")
	}
	settled := runApply(t, obj, "present", base).final()
	if settled.GetFailed() || settled.GetChanged() {
		t.Errorf("the collection was disturbed by the refusals: failed=%v changed=%v %s",
			settled.GetFailed(), settled.GetChanged(), settled.GetMessage())
	}
}

// TestLiveAliasSwap — an alias re-points in place, idempotently.
func TestLiveAliasSwap(t *testing.T) {
	const blue, green, alias = "ss_live_blue", "ss_live_green", "ss_live_alias"
	collections := liveModule().collection()
	aliases := liveModule().alias()

	for _, c := range []string{blue, green} {
		dropLive(t, c)
		if e := runApply(t, collections, "present", liveParams(t, map[string]any{
			"name": c, "vectors": map[string]any{"size": 4, "distance": "Cosine"},
		})).final(); e.GetFailed() {
			t.Fatalf("create %s: %s", c, e.GetMessage())
		}
	}
	t.Cleanup(func() {
		runApply(t, aliases, "absent", liveParams(t, map[string]any{"name": alias}))
		dropLive(t, blue)
		dropLive(t, green)
	})
	runApply(t, aliases, "absent", liveParams(t, map[string]any{"name": alias}))

	first := runApply(t, aliases, "present", liveParams(t, map[string]any{"name": alias, "collection": blue})).final()
	if first.GetFailed() || !first.GetChanged() {
		t.Fatalf("pointing a new alias must change: failed=%v %s", first.GetFailed(), first.GetMessage())
	}
	if second := runApply(t, aliases, "present", liveParams(t, map[string]any{"name": alias, "collection": blue})).final(); second.GetChanged() {
		t.Errorf("re-declaring the same alias must be a no-op: %s", second.GetMessage())
	}

	swap := runApply(t, aliases, "present", liveParams(t, map[string]any{"name": alias, "collection": green})).final()
	if swap.GetFailed() || !swap.GetChanged() {
		t.Fatalf("re-pointing must change: failed=%v %s", swap.GetFailed(), swap.GetMessage())
	}
	if got := swap.GetOutput().GetFields()["previous"].GetStringValue(); got != blue {
		t.Errorf("Output.previous = %q, want %q — a rollback step needs it", got, blue)
	}

	gone := runApply(t, aliases, "absent", liveParams(t, map[string]any{"name": alias})).final()
	if gone.GetFailed() || !gone.GetChanged() {
		t.Fatalf("removing an alias must change: %s", gone.GetMessage())
	}
	if repeat := runApply(t, aliases, "absent", liveParams(t, map[string]any{"name": alias})).final(); repeat.GetChanged() {
		t.Errorf("removing an absent alias must be a no-op: %s", repeat.GetMessage())
	}
}

// TestLivePayloadIndex — create, rebuild on a type change, remove; idempotent at each
// step. This is also where the `wait=true` matters: without it the read-back would race
// the index into existence.
func TestLivePayloadIndex(t *testing.T) {
	const name = "ss_live_index"
	collections := liveModule().collection()
	indexes := liveModule().index()
	dropLive(t, name)
	t.Cleanup(func() { dropLive(t, name) })

	if e := runApply(t, collections, "present", liveParams(t, map[string]any{
		"name": name, "vectors": map[string]any{"size": 4, "distance": "Cosine"},
	})).final(); e.GetFailed() {
		t.Fatalf("create: %s", e.GetMessage())
	}

	field := map[string]any{"collection": name, "field": "tenant", "schema": "keyword"}
	made := runApply(t, indexes, "present", liveParams(t, field)).final()
	if made.GetFailed() || !made.GetChanged() {
		t.Fatalf("creating an index must change: failed=%v %s", made.GetFailed(), made.GetMessage())
	}
	if repeat := runApply(t, indexes, "present", liveParams(t, field)).final(); repeat.GetChanged() {
		t.Errorf("an index already at the declared type must be a no-op: %s", repeat.GetMessage())
	}

	// A type change rebuilds in place — no confirmation, because no data is lost.
	retyped := map[string]any{"collection": name, "field": "tenant", "schema": "integer"}
	rebuilt := runApply(t, indexes, "present", liveParams(t, retyped)).final()
	if rebuilt.GetFailed() || !rebuilt.GetChanged() {
		t.Fatalf("changing the index type must change: failed=%v %s", rebuilt.GetFailed(), rebuilt.GetMessage())
	}
	if got := rebuilt.GetOutput().GetFields()["previous"].GetStringValue(); got != "keyword" {
		t.Errorf("Output.previous = %q, want keyword", got)
	}

	dropped := runApply(t, indexes, "absent", liveParams(t, map[string]any{"collection": name, "field": "tenant"})).final()
	if dropped.GetFailed() || !dropped.GetChanged() {
		t.Fatalf("removing an index must change: %s", dropped.GetMessage())
	}
	if repeat := runApply(t, indexes, "absent", liveParams(t, map[string]any{"collection": name, "field": "tenant"})).final(); repeat.GetChanged() {
		t.Errorf("removing an absent index must be a no-op: %s", repeat.GetMessage())
	}
}

// TestLiveSnapshotFreshnessWindow — max_age_sec is what makes `created` a state, and
// this is the assertion that it actually is one against a real clock and a real
// creation_time (which Qdrant emits with no timezone).
func TestLiveSnapshotFreshnessWindow(t *testing.T) {
	const name = "ss_live_snapshot"
	collections := liveModule().collection()
	snapshots := liveModule().snapshot()
	dropLive(t, name)
	t.Cleanup(func() { dropLive(t, name) })

	if e := runApply(t, collections, "present", liveParams(t, map[string]any{
		"name": name, "vectors": map[string]any{"size": 4, "distance": "Cosine"},
	})).final(); e.GetFailed() {
		t.Fatalf("create: %s", e.GetMessage())
	}

	made := runApply(t, snapshots, "created", liveParams(t, map[string]any{"collection": name})).final()
	if made.GetFailed() || !made.GetChanged() {
		t.Fatalf("taking a snapshot must change: failed=%v %s", made.GetFailed(), made.GetMessage())
	}
	snapshotName := made.GetOutput().GetFields()["name"].GetStringValue()
	if snapshotName == "" {
		t.Fatal("Output.name is empty — it is the only handle on a snapshot")
	}

	// ★ Inside the window it must send nothing. A wrong timezone reading of
	// creation_time would make this fire anyway, which is why it is asserted live.
	fresh := runApply(t, snapshots, "created", liveParams(t, map[string]any{
		"collection": name, "max_age_sec": 3600,
	})).final()
	if fresh.GetFailed() {
		t.Fatalf("max_age_sec run failed: %s", fresh.GetMessage())
	}
	if fresh.GetChanged() {
		t.Errorf("a snapshot inside max_age_sec must be a no-op: %s", fresh.GetMessage())
	}

	gone := runApply(t, snapshots, "absent", liveParams(t, map[string]any{
		"collection": name, "name": snapshotName,
	})).final()
	if gone.GetFailed() || !gone.GetChanged() {
		t.Fatalf("removing a snapshot must change: %s", gone.GetMessage())
	}
	// Qdrant answers 404 to the deletion of a snapshot it does not have, so a second
	// run is exactly where a naive implementation breaks.
	if repeat := runApply(t, snapshots, "absent", liveParams(t, map[string]any{
		"collection": name, "name": snapshotName,
	})).final(); repeat.GetFailed() || repeat.GetChanged() {
		t.Errorf("removing an absent snapshot must be a quiet no-op: failed=%v %s",
			repeat.GetFailed(), repeat.GetMessage())
	}
}

// TestLiveQdrantStillDiscardsAnImmutablePatch pins the PREMISE the `collection` object
// is built on, directly against the server.
//
// Everything this artifact does about immutable fields follows from one observed fact:
// Qdrant answers 200 {"result":true} to a PATCH it throws away. If a future Qdrant
// starts refusing those, this test fails — and that failure is the signal to revisit
// the refusal, not a bug. Pinning it here means the module's model of the server is
// checked against the server rather than against a comment.
func TestLiveQdrantStillDiscardsAnImmutablePatch(t *testing.T) {
	const name = "ss_live_premise"
	obj := liveModule().collection()
	dropLive(t, name)
	t.Cleanup(func() { dropLive(t, name) })

	if e := runApply(t, obj, "present", liveParams(t, map[string]any{
		"name": name, "vectors": map[string]any{"size": 4, "distance": "Cosine"},
	})).final(); e.GetFailed() {
		t.Fatalf("create: %s", e.GetMessage())
	}

	cfg, err := parseConnConfig(mustStruct(t, liveParams(t, nil)))
	if err != nil {
		t.Fatalf("parseConnConfig: %v", err)
	}
	api, err := newHTTPClient(cfg)
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}
	defer api.close()

	res, err := api.do(t.Context(), "PATCH", "/collections/"+name, map[string]any{
		"vectors": map[string]any{"": map[string]any{"size": 8}},
	})
	if err != nil {
		t.Fatalf("PATCH: %v", err)
	}
	if !res.ok() {
		t.Logf("Qdrant now REFUSES an immutable PATCH (%s) — that is an improvement, and this "+
			"module's refusal is no longer the only thing standing between an author and a "+
			"silently ignored update. Revisit collection.go.", res.errorText())
		return
	}

	info, exists, err := readCollection(t.Context(), api, name, "")
	if err != nil || !exists {
		t.Fatalf("read back: %v (exists=%v)", err, exists)
	}
	size, _ := pathValue(info.Config, []string{"params", "vectors", "size"})
	if renderValue(size) != "4" {
		t.Fatalf("the premise changed: Qdrant applied a vector size change in a PATCH (size is now %s). "+
			"collection.go treats that as impossible — revisit it.", renderValue(size))
	}
	t.Log("premise holds: Qdrant answered 200 to a size change and left the size at 4")
}
