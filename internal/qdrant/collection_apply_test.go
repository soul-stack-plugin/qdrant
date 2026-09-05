// Behavioural tests for the `collection` object, through the real [object.Apply] path.
//
// Two of these are the reason the ticket exists, and they assert properties rather than
// messages:
//
//   - a reconciliation that cannot reach the declared shape refuses having sent NOTHING
//     (TestCollectionPresentRefusesBeforeSendingAnything);
//   - an update Qdrant accepted with 200 and then discarded FAILS the step rather than
//     being reported as a change (TestCollectionPresentCatchesASilentlyDiscardedUpdate).
//
// The second is the one that cannot be caught any other way. Every mutating endpoint in
// Qdrant answers `{"result":true}` whether or not it did the thing, so the only evidence
// available is a read-back — and a plugin without one reports success forever.
package qdrant

import (
	"strings"
	"testing"
)

// docsPresent is the declaration the tests reconcile against.
func docsPresent(extra map[string]any) map[string]any {
	params := baseParams(map[string]any{
		"name":    "docs",
		"vectors": map[string]any{"size": 4, "distance": "Cosine"},
	})
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// TestCollectionPresentRefusesBeforeSendingAnything — the invariant that makes the
// refusal worth having. Applying the reachable half first and failing afterwards would
// leave the collection in a shape nobody declared, which is worse than either outcome
// the author asked for.
func TestCollectionPresentRefusesBeforeSendingAnything(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)))

	// A declaration that mixes a REACHABLE change (replication_factor) with an
	// unreachable one (the vector size). The reachable half must not be applied.
	stream := runApply(t, moduleWith(api).collection(), "present", docsPresent(map[string]any{
		"vectors":            map[string]any{"size": 8, "distance": "Cosine"},
		"replication_factor": 3,
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("a collection differing in an immutable field must FAIL, got: %s", event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("the refusal must send nothing, but it sent %v", sent)
	}
	for _, want := range []string{`params.vectors."".size`, "is 4", "declared 8", "recreated", "confirm_destroy"} {
		if !strings.Contains(event.GetMessage(), want) {
			t.Errorf("the refusal must mention %q:\n%s", want, event.GetMessage())
		}
	}
}

// TestCollectionPresentCatchesASilentlyDiscardedUpdate — the signature defect of this
// API, and the one a naive plugin reports as a success on every run forever.
//
// The fake here plays a Qdrant that behaves EXACTLY as the live one measured on
// 2026-09-05: it answers the PATCH with 200 {"result":true} and leaves the collection
// alone. The step must fail.
func TestCollectionPresentCatchesASilentlyDiscardedUpdate(t *testing.T) {
	unchanged := liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))
	api := newFakeAPI(t).
		// Sticky: every read, before and after, reports replication_factor 1.
		on("GET", "/collections/docs", unchanged).
		on("PATCH", "/collections/docs", okTrue(t))

	stream := runApply(t, moduleWith(api).collection(), "present", docsPresent(map[string]any{
		"replication_factor": 2,
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("an update Qdrant discarded must FAIL, got changed=%v: %s", event.GetChanged(), event.GetMessage())
	}
	if event.GetChanged() {
		t.Error("a step that failed must not also report changed")
	}
	for _, want := range []string{"did NOT apply", "replication_factor", "reading the collection back"} {
		if !strings.Contains(event.GetMessage(), want) {
			t.Errorf("the failure must mention %q — an operator has to learn that Qdrant said yes and did nothing:\n%s", want, event.GetMessage())
		}
	}
}

// TestCollectionPresentIsIdempotent — the state contract. A second run sends no
// mutating request AT ALL, which is stronger than "reports changed=false": a PATCH that
// happens to be a no-op still writes, still races another writer, and still costs a
// round trip on every run of every scenario.
func TestCollectionPresentIsIdempotent(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)))

	stream := runApply(t, moduleWith(api).collection(), "present", docsPresent(map[string]any{
		// Declared exactly as the live collection reports it, including a partial
		// nested map whose values match Qdrant's defaults.
		"replication_factor": 1,
		"hnsw_config":        map[string]any{"m": 16},
		"sharding_method":    "auto",
	}))

	event := stream.final()
	if event.GetFailed() {
		t.Fatalf("a matching collection must not fail: %s", event.GetMessage())
	}
	if event.GetChanged() {
		t.Errorf("a matching collection must report changed=false: %s", event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("a converged run must send no mutating request, sent %v", sent)
	}
}

// TestCollectionPresentCreatesWhenMissing — and verifies the result by reading it back
// rather than by trusting the create.
func TestCollectionPresentCreatesWhenMissing(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			notFoundResult("Collection `docs` doesn't exist!"),
			liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("PUT", "/collections/docs", okTrue(t))

	stream := runApply(t, moduleWith(api).collection(), "present", docsPresent(nil))

	event := stream.final()
	if event.GetFailed() {
		t.Fatalf("creating a missing collection must succeed: %s", event.GetMessage())
	}
	if !event.GetChanged() {
		t.Error("creating a collection must report changed=true")
	}
	if !event.GetOutput().GetFields()["exists"].GetBoolValue() {
		t.Error("Output.exists must be true after a create")
	}
}

// TestCollectionCreateThatDidNotTakeFails — a create is read back for the same reason a
// patch is. Qdrant discards a create-body field it does not recognise and still answers
// ok, so a build talking to a Qdrant it was not written for would otherwise land a green
// step and a collection that is not what was asked for.
func TestCollectionCreateThatDidNotTakeFails(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			notFoundResult("Collection `docs` doesn't exist!"),
			// The read-back shows 1 shard, not the 3 that were declared.
			liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("PUT", "/collections/docs", okTrue(t))

	stream := runApply(t, moduleWith(api).collection(), "present", docsPresent(map[string]any{
		"shard_number": 3,
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("a create whose result does not match the declaration must fail: %s", event.GetMessage())
	}
	// The operator has to learn that the collection now EXISTS and is wrong, or
	// they will go looking for one they believe was never made.
	if !strings.Contains(event.GetMessage(), "was created") {
		t.Errorf("the failure must say the collection WAS created:\n%s", event.GetMessage())
	}
}

// TestCollectionRecreatedOnlyDropsOnConflict — the difference between a state and a
// scheduled outage. `recreated` on a collection that already matches must leave it
// alone; recreating unconditionally would destroy the data on every run.
func TestCollectionRecreatedOnlyDropsOnConflict(t *testing.T) {
	t.Run("already in the declared shape", func(t *testing.T) {
		api := newFakeAPI(t).
			on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)))

		stream := runApply(t, moduleWith(api).collection(), "recreated", docsPresent(map[string]any{
			"confirm_destroy": true,
		}))

		event := stream.final()
		if event.GetFailed() || event.GetChanged() {
			t.Fatalf("a matching collection must be left alone: failed=%v changed=%v %s",
				event.GetFailed(), event.GetChanged(), event.GetMessage())
		}
		if sent := api.mutating(); len(sent) != 0 {
			t.Errorf("recreated must not touch a collection that already matches, sent %v", sent)
		}
	})

	t.Run("immutable drift", func(t *testing.T) {
		api := newFakeAPI(t).
			on("GET", "/collections/docs",
				liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)),
				liveCollection(t, defaultConfig(unnamedVector(8, "Cosine"), nil))).
			// The pre-flight: the declared body is built under a throwaway name and
			// removed again before anything real is dropped.
			on("GET", "/collections/docs__ss_precheck", notFoundResult("Collection `docs__ss_precheck` doesn't exist!")).
			on("PUT", "/collections/docs__ss_precheck", okTrue(t)).
			on("DELETE", "/collections/docs__ss_precheck", okTrue(t)).
			on("DELETE", "/collections/docs", okTrue(t)).
			on("PUT", "/collections/docs", okTrue(t))

		stream := runApply(t, moduleWith(api).collection(), "recreated", docsPresent(map[string]any{
			"vectors":         map[string]any{"size": 8, "distance": "Cosine"},
			"confirm_destroy": true,
		}))

		event := stream.final()
		if event.GetFailed() {
			t.Fatalf("recreated with confirm_destroy must rebuild: %s", event.GetMessage())
		}
		if !event.GetChanged() {
			t.Error("a rebuild must report changed=true")
		}

		// Only the calls against the REAL collection: the pre-flight builds and
		// removes a throwaway one first, and that is the point of it.
		var order []string
		for _, c := range api.mutating() {
			if c.path == "/collections/docs" {
				order = append(order, c.method)
			}
		}
		if len(order) != 2 || order[0] != "DELETE" || order[1] != "PUT" {
			t.Errorf("a rebuild is a DELETE then a PUT, got %v", order)
		}
		// The operator must be told what it cost, in the message they actually see.
		if !strings.Contains(strings.ToLower(event.GetMessage()), "destroy") {
			t.Errorf("a rebuild must say the data was destroyed:\n%s", event.GetMessage())
		}
	})
}

// TestCollectionRecreatedRefusesWithoutConfirmDestroy — in BOTH phases. Validate is a
// separate RPC a runner need not call, so an interlock that lives only there is an
// interlock the runtime never reaches (NIM-786).
func TestCollectionRecreatedRefusesWithoutConfirmDestroy(t *testing.T) {
	cases := map[string]map[string]any{
		"omitted": docsPresent(nil),
		"false":   docsPresent(map[string]any{"confirm_destroy": false}),
	}

	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			// Validate refuses.
			reply := runValidate(t, moduleWith(newFakeAPI(t)).collection(), "recreated", params)
			if reply.GetOk() {
				t.Error("Validate must refuse recreated without confirm_destroy: true")
			}

			// And so does Apply, without reaching the instance at all: the fake has
			// no scripted endpoint, so any request would fail the test outright.
			api := newFakeAPI(t)
			stream := runApply(t, moduleWith(api).collection(), "recreated", params)
			if !stream.final().GetFailed() {
				t.Error("Apply must refuse recreated without confirm_destroy: true")
			}
			if len(api.calls) != 0 {
				t.Errorf("the refusal must not reach the instance, called %v", api.pathsHit())
			}
		})
	}
}

// TestCollectionAbsentIsIdempotent — a collection that is not there sends no DELETE.
func TestCollectionAbsentIsIdempotent(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", notFoundResult("Collection `docs` doesn't exist!"))

	stream := runApply(t, moduleWith(api).collection(), "absent",
		baseParams(map[string]any{"name": "docs"}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("an absent collection must be a no-op: failed=%v changed=%v %s",
			event.GetFailed(), event.GetChanged(), event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("absent must send no DELETE for a collection that is not there, sent %v", sent)
	}
}

// TestCollectionAbsentRemovesAndVerifies — and reports the removal only after seeing it.
func TestCollectionAbsentRemovesAndVerifies(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)),
			notFoundResult("Collection `docs` doesn't exist!")).
		on("DELETE", "/collections/docs", okTrue(t))

	stream := runApply(t, moduleWith(api).collection(), "absent",
		baseParams(map[string]any{"name": "docs"}))

	event := stream.final()
	if event.GetFailed() || !event.GetChanged() {
		t.Fatalf("removing an existing collection must report changed=true: failed=%v %s",
			event.GetFailed(), event.GetMessage())
	}
}

// TestCollectionAbsentFailsWhenTheDeleteDidNotTake — Qdrant answers 200
// {"result":false} to a delete that removed nothing, so the read-back is what decides
// here too.
func TestCollectionAbsentFailsWhenTheDeleteDidNotTake(t *testing.T) {
	stillThere := liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))
	api := newFakeAPI(t).
		on("GET", "/collections/docs", stillThere).
		on("DELETE", "/collections/docs", okResult(t, false))

	stream := runApply(t, moduleWith(api).collection(), "absent",
		baseParams(map[string]any{"name": "docs"}))

	if !stream.final().GetFailed() {
		t.Errorf("a delete that left the collection in place must fail, got: %s", stream.final().GetMessage())
	}
}
