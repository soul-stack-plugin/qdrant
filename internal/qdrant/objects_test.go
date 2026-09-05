// Behavioural tests for the four objects other than `collection`, plus the two rules
// that apply to all five: a wrong-typed parameter is refused rather than coerced
// (NIM-778), and a secret never reaches an event.
package qdrant

import (
	"errors"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// --- instance ---

func TestInstancePingedReportsTheServersOwnLine(t *testing.T) {
	api := newFakeAPI(t).on("GET", "/healthz", rawResult(200, "healthz check passed"))

	stream := runApply(t, moduleWith(api).instance(), "pinged", baseParams(nil))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("a probe must succeed and report changed=false: failed=%v changed=%v",
			event.GetFailed(), event.GetChanged())
	}
	if got := event.GetOutput().GetFields()["result"].GetStringValue(); got != "healthz check passed" {
		t.Errorf("Output.result = %q, want the server's own line", got)
	}
}

// TestInstanceReadyProbedReportsNotReadyWithoutFailing — the distinction a rolling
// restart depends on. Qdrant answers 503 while shards load, and a gate written as
// `until: register.self.ready` can only wait that out if the step SUCCEEDS.
func TestInstanceReadyProbedReportsNotReadyWithoutFailing(t *testing.T) {
	api := newFakeAPI(t).on("GET", "/readyz", rawResult(503, "service unavailable"))

	stream := runApply(t, moduleWith(api).instance(), "ready-probed", baseParams(nil))

	event := stream.final()
	if event.GetFailed() {
		t.Fatalf("a not-ready instance is a READING, not a failure: %s", event.GetMessage())
	}
	if event.GetOutput().GetFields()["ready"].GetBoolValue() {
		t.Error("Output.ready must be false on a 503")
	}
}

// TestInstanceReadyProbedFailsWhenNothingAnswers — the other half of the same
// distinction. A gate that reads "host is gone" as "still starting" burns its whole
// retry budget on an instance that is never coming back.
func TestInstanceReadyProbedFailsWhenNothingAnswers(t *testing.T) {
	api := newFakeAPI(t).failWith("GET", "/readyz", errors.New("dial tcp: connection refused"))

	stream := runApply(t, moduleWith(api).instance(), "ready-probed", baseParams(nil))

	if !stream.final().GetFailed() {
		t.Error("an unreachable instance must fail, not report ready=false")
	}
}

func TestInstanceVersionProbedFailsOnSomethingThatIsNotQdrant(t *testing.T) {
	// A proxy or a login page on the address answers 200 and no version.
	api := newFakeAPI(t).on("GET", "/", rawResult(200, `{"hello":"world"}`))

	stream := runApply(t, moduleWith(api).instance(), "version-probed", baseParams(nil))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatalf("an answer with no version field must fail rather than report an empty version: %s", event.GetMessage())
	}
	if !strings.Contains(event.GetMessage(), "addr") {
		t.Errorf("the failure should point at params.addr:\n%s", event.GetMessage())
	}
}

// --- alias ---

func aliasList(t *testing.T, pairs map[string]string) apiResult {
	t.Helper()
	rows := make([]any, 0, len(pairs))
	for _, alias := range sortedKeys(pairs) {
		rows = append(rows, map[string]any{"alias_name": alias, "collection_name": pairs[alias]})
	}
	return okResult(t, map[string]any{"aliases": rows})
}

func TestAliasPresentIsIdempotent(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/blue", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("GET", "/aliases", aliasList(t, map[string]string{"docs": "blue"}))

	stream := runApply(t, moduleWith(api).alias(), "present",
		baseParams(map[string]any{"name": "docs", "collection": "blue"}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("an alias already pointing at the target must be a no-op: failed=%v changed=%v %s",
			event.GetFailed(), event.GetChanged(), event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("a converged alias must send nothing, sent %v", sent)
	}
}

// TestAliasPresentRePointsAndReportsThePrevious — the blue-green swap. Output.previous
// is what a rollback step needs, and it is only knowable from the read BEFORE.
func TestAliasPresentRePointsAndReportsThePrevious(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/blue", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil))).
		on("GET", "/aliases",
			aliasList(t, map[string]string{"docs": "green"}),
			aliasList(t, map[string]string{"docs": "blue"})).
		on("POST", "/collections/aliases", okTrue(t))

	stream := runApply(t, moduleWith(api).alias(), "present",
		baseParams(map[string]any{"name": "docs", "collection": "blue"}))

	event := stream.final()
	if event.GetFailed() || !event.GetChanged() {
		t.Fatalf("re-pointing an alias must report changed=true: failed=%v %s", event.GetFailed(), event.GetMessage())
	}
	if got := event.GetOutput().GetFields()["previous"].GetStringValue(); got != "green" {
		t.Errorf("Output.previous = %q, want \"green\"", got)
	}
}

// TestAliasPresentRefusesAMissingTarget — with BOTH names in the message. Qdrant's own
// error names only the alias, which leaves an operator guessing which of the two words
// they got wrong.
func TestAliasPresentRefusesAMissingTarget(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/blue", notFoundResult("Collection `blue` doesn't exist!"))

	stream := runApply(t, moduleWith(api).alias(), "present",
		baseParams(map[string]any{"name": "docs", "collection": "blue"}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatal("an alias to a collection that does not exist must fail")
	}
	for _, want := range []string{"docs", "blue"} {
		if !strings.Contains(event.GetMessage(), want) {
			t.Errorf("the failure must name %q:\n%s", want, event.GetMessage())
		}
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("the refusal must send nothing, sent %v", sent)
	}
}

// TestAliasAbsentIsIdempotent — Qdrant answers 200 to the deletion of an alias that
// does not exist, so acting on the response would report a change on every run.
func TestAliasAbsentIsIdempotent(t *testing.T) {
	api := newFakeAPI(t).on("GET", "/aliases", aliasList(t, nil))

	stream := runApply(t, moduleWith(api).alias(), "absent", baseParams(map[string]any{"name": "docs"}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("an absent alias must be a no-op: failed=%v changed=%v", event.GetFailed(), event.GetChanged())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("absent must send nothing for an alias that is not there, sent %v", sent)
	}
}

// --- index ---

// collectionWithSchema is a collection whose payload_schema carries indexed fields.
func collectionWithSchema(t *testing.T, fields map[string]string) apiResult {
	t.Helper()
	schema := map[string]any{}
	for field, dataType := range fields {
		schema[field] = map[string]any{"data_type": dataType, "points": 0}
	}
	return okResult(t, map[string]any{
		"status":                "green",
		"optimizer_status":      "ok",
		"points_count":          0,
		"segments_count":        1,
		"indexed_vectors_count": 0,
		"config":                defaultConfig(unnamedVector(4, "Cosine"), nil),
		"payload_schema":        schema,
	})
}

func TestIndexPresentIsIdempotent(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs", collectionWithSchema(t, map[string]string{"tenant": "keyword"}))

	stream := runApply(t, moduleWith(api).index(), "present",
		baseParams(map[string]any{"collection": "docs", "field": "tenant", "schema": "keyword"}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("an index already of the declared type must be a no-op: failed=%v changed=%v %s",
			event.GetFailed(), event.GetChanged(), event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("a converged index must send nothing, sent %v", sent)
	}
}

// TestIndexPresentRebuildsInPlaceWithoutAsking — the deliberate contrast with
// `collection`. Rebuilding a payload index destroys an INDEX, not data, so it happens
// without an acknowledgement. Keeping the two visibly apart is what makes the refusal
// on `collection` mean something.
func TestIndexPresentRebuildsInPlaceWithoutAsking(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs",
			collectionWithSchema(t, map[string]string{"tenant": "integer"}),
			collectionWithSchema(t, map[string]string{"tenant": "keyword"})).
		on("DELETE", "/collections/docs/index/tenant", okTrue(t)).
		on("PUT", "/collections/docs/index", okTrue(t))

	stream := runApply(t, moduleWith(api).index(), "present",
		baseParams(map[string]any{"collection": "docs", "field": "tenant", "schema": "keyword"}))

	event := stream.final()
	if event.GetFailed() || !event.GetChanged() {
		t.Fatalf("an index of the wrong type must be rebuilt: failed=%v %s", event.GetFailed(), event.GetMessage())
	}
	var order []string
	for _, c := range api.mutating() {
		order = append(order, c.method)
	}
	if len(order) != 2 || order[0] != "DELETE" || order[1] != "PUT" {
		t.Errorf("a rebuild is a DELETE then a PUT, got %v", order)
	}
	if got := event.GetOutput().GetFields()["previous"].GetStringValue(); got != "integer" {
		t.Errorf("Output.previous = %q, want \"integer\"", got)
	}
}

// TestIndexPresentRefusesAnUnknownSchema — a misspelled schema name is refused by
// Validate rather than sent to Qdrant.
func TestIndexPresentRefusesAnUnknownSchema(t *testing.T) {
	reply := runValidate(t, moduleWith(newFakeAPI(t)).index(), "present",
		baseParams(map[string]any{"collection": "docs", "field": "tenant", "schema": "kwyword"}))

	if reply.GetOk() {
		t.Error("a schema outside the managed set must be refused by Validate")
	}
}

// TestIndexPresentRefusesAnObjectSchema — Qdrant's object schema form (a text index
// with a tokenizer) is deliberately out of this version: the read-back reports only
// `data_type`, so this module could not tell one tokenizer from another and would
// report converged on an index that is not. It is refused by the declared type, which
// is a different mechanism from the unknown-name case above — hence its own test.
func TestIndexPresentRefusesAnObjectSchema(t *testing.T) {
	reply := runValidate(t, moduleWith(newFakeAPI(t)).index(), "present",
		baseParams(map[string]any{
			"collection": "docs", "field": "body",
			"schema": map[string]any{"type": "text", "tokenizer": "word"},
		}))

	if reply.GetOk() {
		t.Error("an object-valued schema must be refused: only the plain string forms can be compared exactly")
	}
}

// --- snapshot ---

func snapshotList(t *testing.T, entries ...map[string]any) apiResult {
	t.Helper()
	rows := make([]any, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, e)
	}
	return okResult(t, rows)
}

func snapshotRow(name, creationTime string) map[string]any {
	return map[string]any{"name": name, "creation_time": creationTime, "size": 132608, "checksum": "abc"}
}

// TestSnapshotCreatedIsIdempotentWithinMaxAge — the parameter that turns an operation
// into a state. Without it every run takes another snapshot; with it a run inside the
// window sends nothing.
func TestSnapshotCreatedIsIdempotentWithinMaxAge(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs/snapshots", snapshotList(t, snapshotRow("docs-2026-09-05-07-40-09.snapshot", "2026-09-05T07:40:09")))

	m := moduleWith(api)
	// Ten minutes after the snapshot, with an hour-wide window.
	m.clock = func() time.Time { return time.Date(2026, 9, 5, 7, 50, 9, 0, time.UTC) }

	stream := runApply(t, m.snapshot(), "created",
		baseParams(map[string]any{"collection": "docs", "max_age_sec": 3600}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("a snapshot inside max_age_sec must be a no-op: failed=%v changed=%v %s",
			event.GetFailed(), event.GetChanged(), event.GetMessage())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("no snapshot should have been taken, sent %v", sent)
	}
}

// TestSnapshotCreatedTakesOneWhenTheNewestIsTooOld — the other side of the window.
func TestSnapshotCreatedTakesOneWhenTheNewestIsTooOld(t *testing.T) {
	fresh := snapshotRow("docs-2026-09-05-08-00-00.snapshot", "2026-09-05T08:00:00")
	api := newFakeAPI(t).
		on("GET", "/collections/docs/snapshots",
			snapshotList(t, snapshotRow("docs-2026-09-05-07-00-00.snapshot", "2026-09-05T07:00:00")),
			snapshotList(t, fresh)).
		on("POST", "/collections/docs/snapshots", okResult(t, fresh))

	m := moduleWith(api)
	m.clock = func() time.Time { return time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC) }

	stream := runApply(t, m.snapshot(), "created",
		baseParams(map[string]any{"collection": "docs", "max_age_sec": 600}))

	event := stream.final()
	if event.GetFailed() || !event.GetChanged() {
		t.Fatalf("a stale newest snapshot must trigger a new one: failed=%v %s", event.GetFailed(), event.GetMessage())
	}
	if got := event.GetOutput().GetFields()["name"].GetStringValue(); got != fresh["name"] {
		t.Errorf("Output.name = %q, want the new snapshot's name", got)
	}
}

// TestSnapshotCreatedFailsOnAMissingCollection — a backup step that passes quietly on a
// collection that was never created is worse than one that stops.
func TestSnapshotCreatedFailsOnAMissingCollection(t *testing.T) {
	api := newFakeAPI(t).
		on("GET", "/collections/docs/snapshots", notFoundResult("Collection `docs` doesn't exist!"))

	stream := runApply(t, moduleWith(api).snapshot(), "created", baseParams(map[string]any{"collection": "docs"}))

	if !stream.final().GetFailed() {
		t.Error("snapshotting a collection that does not exist must fail")
	}
}

// TestSnapshotAbsentIsIdempotent — and sends no DELETE, which also avoids the 404
// Qdrant answers to the deletion of a snapshot it does not have. A blind DELETE would
// fail the second run of the very same step.
func TestSnapshotAbsentIsIdempotent(t *testing.T) {
	api := newFakeAPI(t).on("GET", "/collections/docs/snapshots", snapshotList(t))

	stream := runApply(t, moduleWith(api).snapshot(), "absent",
		baseParams(map[string]any{"collection": "docs", "name": "docs-old.snapshot"}))

	event := stream.final()
	if event.GetFailed() || event.GetChanged() {
		t.Fatalf("an absent snapshot must be a no-op: failed=%v changed=%v", event.GetFailed(), event.GetChanged())
	}
	if sent := api.mutating(); len(sent) != 0 {
		t.Errorf("absent must send no DELETE for a snapshot that is not there, sent %v", sent)
	}
}

// --- rules that apply to every object ---

// TestApiKeyNeverReachesAnEvent — ADR-010. The key is in the connection and nowhere
// else; a transport error that happens to carry it is scrubbed before it becomes a
// message an operator, a log or a trace can read.
func TestApiKeyNeverReachesAnEvent(t *testing.T) {
	const key = "s3cr3t-api-key"

	api := newFakeAPI(t).
		failWith("GET", "/collections/docs", errors.New(`Get "http://qdrant-1:6333/collections/docs": bad key `+key))

	stream := runApply(t, moduleWith(api).collection(), "probed",
		baseParams(map[string]any{"name": "docs", "api_key": key}))

	event := stream.final()
	if !event.GetFailed() {
		t.Fatal("a transport error must fail the step")
	}
	assertNoSecret(t, event, key)
}

// assertNoSecret checks every operator-visible surface of one event.
func assertNoSecret(t *testing.T, event *pluginv1.ApplyEvent, secret string) {
	t.Helper()
	if strings.Contains(event.GetMessage(), secret) {
		t.Errorf("the api-key leaked into ApplyEvent.Message:\n%s", event.GetMessage())
	}
	for name, v := range event.GetOutput().GetFields() {
		if strings.Contains(v.String(), secret) {
			t.Errorf("the api-key leaked into Output.%s", name)
		}
	}
}

// TestApiKeyTravelsInTheHeaderNotTheURL — measured against a live 1.18.3, Qdrant
// answers 401 to `?api_key=<key>`, so there is no URL form of the credential. This
// keeps it that way: nothing in a request path may carry it.
func TestApiKeyTravelsInTheHeaderNotTheURL(t *testing.T) {
	const key = "s3cr3t-api-key"

	api := newFakeAPI(t).
		on("GET", "/collections/docs", liveCollection(t, defaultConfig(unnamedVector(4, "Cosine"), nil)))

	runApply(t, moduleWith(api).collection(), "probed",
		baseParams(map[string]any{"name": "docs", "api_key": key}))

	for _, path := range api.pathsHit() {
		if strings.Contains(path, key) {
			t.Errorf("the api-key must never appear in a request path, got %q", path)
		}
	}
}
