// Tests for adding a named vector to a live collection — the one request in this
// artifact whose body shape is dictated by a schema different from the create body's,
// and the one endpoint whose `wait` semantics are easy to miss.
//
// There was no test here, and that gap hid two defects at once: the whole vector spec
// was being sent into an arm that accepts only part of it, and the read-back that
// follows was racing an unwaited update. Both are asserted below.
package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// addVectorBody returns the decoded body of the single PUT to the add-vector endpoint,
// failing the test when there was not exactly one.
func addVectorBody(t *testing.T, api *fakeAPI, vector string) map[string]any {
	t.Helper()
	want := "PUT /collections/docs/vectors/" + vector
	var found []recordedCall
	for _, c := range api.calls {
		if c.key() == want {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one %s, got %d (calls: %v)", want, len(found), api.pathsHit())
	}
	raw, err := json.Marshal(found[0].body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return decoded
}

// ★ TestAddVectorSendsOnlyTheCreationKeys — Qdrant's DenseVectorConfig, the body of
// this endpoint, carries only the keys that define the vector space; its own
// description says "Storage type, index type, and quantization are inferred". Sending
// hnsw_config in it is discarded in silence, and the read-back then fails the step on a
// declaration that was perfectly legal.
func TestAddVectorSendsOnlyTheCreationKeys(t *testing.T) {
	withImage := defaultConfig(map[string]any{
		"text":  map[string]any{"size": float64(8), "distance": "Dot"},
		"image": map[string]any{"size": float64(512), "distance": "Cosine", "on_disk": true},
	}, nil)

	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			liveCollection(t, defaultConfig(map[string]any{
				"text": map[string]any{"size": float64(8), "distance": "Dot"},
			}, nil)),
			liveCollection(t, withImage)).
		on("PUT", "/collections/docs/vectors/image", okTrue(t)).
		on("PATCH", "/collections/docs", okTrue(t))

	stream := runApply(t, moduleWith(api).collection(), "present", baseParams(map[string]any{
		"name": "docs",
		"vectors": map[string]any{
			"text":  map[string]any{"size": 8, "distance": "Dot"},
			"image": map[string]any{"size": 512, "distance": "Cosine", "on_disk": true},
		},
	}))

	if event := stream.final(); event.GetFailed() {
		t.Fatalf("adding a named vector must succeed: %s", event.GetMessage())
	}

	body := addVectorBody(t, api, "image")
	dense, ok := body["dense"].(map[string]any)
	if !ok {
		t.Fatalf("the body must use Qdrant's dense arm, got %v", body)
	}
	if got := sortedKeys(dense); !equalStringSlices(got, []string{"distance", "size"}) {
		t.Errorf("the dense arm must carry ONLY the creation keys, got %v — Qdrant discards the rest in silence", got)
	}
	if _, wrong := dense["on_disk"]; wrong {
		t.Error("on_disk is a tunable key: DenseVectorConfig does not accept it and would drop it")
	}
}

// TestAddVectorTunablesFollowAsAPatch — the other half of the split. `on_disk` is
// declared, cannot ride along with the add, and must therefore be applied afterwards
// through the collection update — otherwise the first run could never converge.
func TestAddVectorTunablesFollowAsAPatch(t *testing.T) {
	plan := planCollection(
		map[string]any{"vectors": map[string]any{
			"text":  map[string]any{"size": float64(8), "distance": "Dot"},
			"image": map[string]any{"size": float64(512), "distance": "Cosine", "on_disk": true},
		}},
		defaultConfig(map[string]any{
			"text": map[string]any{"size": float64(8), "distance": "Dot"},
		}, nil),
	)

	added, ok := plan.addVectors["image"]
	if !ok {
		t.Fatal("the missing vector must be added")
	}
	create, ok := added.(map[string]any)
	if !ok {
		t.Fatalf("the add spec must be a map, got %T", added)
	}
	if _, wrong := create["on_disk"]; wrong {
		t.Error("the add must carry only the creation keys")
	}

	vectors, ok := plan.patch["vectors"].(map[string]any)
	if !ok {
		t.Fatalf("the tunable keys of a new vector must be patched, patch was %v", plan.patch)
	}
	image, ok := vectors["image"].(map[string]any)
	if !ok {
		t.Fatalf("no patch for the new vector: %v", vectors)
	}
	if image["on_disk"] != true {
		t.Errorf("on_disk must be patched after the add, got %v", image["on_disk"])
	}
}

// TestAddVectorWaitsForTheUpdate — every other verified write in this artifact passes
// wait=true, and this endpoint answers `acknowledged` without it. On a busy collection
// the read-back would then beat the update and fail the step intermittently, blaming a
// declaration that was fine.
func TestAddVectorWaitsForTheUpdate(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			liveCollection(t, defaultConfig(map[string]any{
				"text": map[string]any{"size": float64(8), "distance": "Dot"},
			}, nil)),
			liveCollection(t, defaultConfig(map[string]any{
				"text":  map[string]any{"size": float64(8), "distance": "Dot"},
				"image": map[string]any{"size": float64(512), "distance": "Cosine"},
			}, nil))).
		on("PUT", "/collections/docs/vectors/image", okTrue(t))

	runApply(t, moduleWith(api).collection(), "present", baseParams(map[string]any{
		"name": "docs",
		"vectors": map[string]any{
			"text":  map[string]any{"size": 8, "distance": "Dot"},
			"image": map[string]any{"size": 512, "distance": "Cosine"},
		},
	}))

	var path string
	for _, c := range api.calls {
		if strings.HasPrefix(c.path, "/collections/docs/vectors/image") {
			path = c.path
		}
	}
	if !strings.Contains(path, "wait=true") {
		t.Errorf("the add must wait for the update to land, got %q", path)
	}
}

// TestDroppingANamedVectorIsRefused — adding one destroys nothing; removing one
// destroys that vector's data, so it is a conflict rather than a quiet DELETE.
func TestDroppingANamedVectorIsRefused(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(map[string]any{
			"text":  map[string]any{"size": float64(8), "distance": "Dot"},
			"image": map[string]any{"size": float64(512), "distance": "Cosine"},
		}, nil)))

	stream := runApply(t, moduleWith(api).collection(), "present", baseParams(map[string]any{
		"name":    "docs",
		"vectors": map[string]any{"text": map[string]any{"size": 8, "distance": "Dot"}},
	}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("dropping a named vector destroys its data and must be refused: %s", event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("the refusal must send nothing, sent %v", sent)
	}
	if !strings.Contains(event.GetMessage(), "image") {
		t.Errorf("the refusal must name the vector:\n%s", event.GetMessage())
	}
}

// equalStringSlices is a local helper: the shared one lives in a file this may outlive.
func equalStringSlices(a, b []string) bool {
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
