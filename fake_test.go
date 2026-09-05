// The L0 harness: a scripted Qdrant and a recording Apply stream.
//
// The fake answers from a QUEUE per endpoint rather than a single canned response,
// because the behaviour under test is precisely the before/after read that this
// artifact does instead of trusting a 200 — a fake that answered the same thing every
// time could not tell a converged run from one that verified nothing.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// --- the Apply stream ---

type applyStream struct {
	grpc.ServerStreamingServer[pluginv1.ApplyEvent]
	sent []*pluginv1.ApplyEvent
}

func (s *applyStream) Send(e *pluginv1.ApplyEvent) error { s.sent = append(s.sent, e); return nil }
func (s *applyStream) Context() context.Context          { return context.Background() }

func (s *applyStream) final() *pluginv1.ApplyEvent {
	if len(s.sent) == 0 {
		return nil
	}
	return s.sent[len(s.sent)-1]
}

// --- the scripted Qdrant ---

type recordedCall struct {
	method string
	path   string
	body   any
}

func (c recordedCall) key() string { return c.method + " " + trimQuery(c.path) }

// bodyJSON renders the request body for an assertion. Marshalling rather than
// reflecting keeps the assertion in the same vocabulary the wire uses.
func (c recordedCall) bodyJSON(t *testing.T) string {
	t.Helper()
	if c.body == nil {
		return ""
	}
	raw, err := json.Marshal(c.body)
	if err != nil {
		t.Fatalf("marshal recorded body: %v", err)
	}
	return string(raw)
}

type fakeAPI struct {
	t *testing.T

	// script maps "METHOD /path" (query stripped) to the answers, in order. The
	// LAST answer is sticky: a queue that ran dry would otherwise turn "this
	// endpoint is polled twice" into an unrelated failure.
	script map[string][]apiResult

	// transportErr maps the same key to an error from the transport itself, for the
	// paths where "nothing answered" has to be told apart from "answered badly".
	transportErr map[string]error

	calls  []recordedCall
	closed bool
}

func newFakeAPI(t *testing.T) *fakeAPI {
	return &fakeAPI{t: t, script: map[string][]apiResult{}, transportErr: map[string]error{}}
}

// on scripts one endpoint. Several results are consumed in order.
func (f *fakeAPI) on(method, path string, results ...apiResult) *fakeAPI {
	f.script[method+" "+trimQuery(path)] = results
	return f
}

func (f *fakeAPI) failWith(method, path string, err error) *fakeAPI {
	f.transportErr[method+" "+trimQuery(path)] = err
	return f
}

func (f *fakeAPI) do(_ context.Context, method, path string, body any) (apiResult, error) {
	call := recordedCall{method: method, path: path, body: body}
	f.calls = append(f.calls, call)

	if err, ok := f.transportErr[call.key()]; ok {
		return apiResult{}, err
	}
	queue, ok := f.script[call.key()]
	if !ok || len(queue) == 0 {
		f.t.Fatalf("fake qdrant: unscripted call %s\n  scripted: %v", call.key(), sortedKeys(f.script))
	}
	res := queue[0]
	if len(queue) > 1 {
		f.script[call.key()] = queue[1:]
	}
	return res, nil
}

func (f *fakeAPI) close() { f.closed = true }

// mutating returns the calls that could have changed something on the instance.
// The refusal tests assert this is EMPTY, which is the property that matters: a
// refusal that happens after the first PATCH is not a refusal.
func (f *fakeAPI) mutating() []recordedCall {
	var out []recordedCall
	for _, c := range f.calls {
		switch c.method {
		case "PUT", "POST", "PATCH", "DELETE":
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeAPI) pathsHit() []string {
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.method+" "+c.path)
	}
	return out
}

func trimQuery(path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		return path[:i]
	}
	return path
}

// --- response constructors ---

// okResult wraps a value in Qdrant's `{"result": …, "status":"ok"}` envelope.
func okResult(t *testing.T, result any) apiResult {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"result": result, "status": "ok", "time": 0.001})
	if err != nil {
		t.Fatalf("marshal fake result: %v", err)
	}
	return apiResult{status: 200, body: raw}
}

// okTrue is the answer Qdrant gives to almost every mutation — including one it
// silently discarded. Named so the tests read as what they are testing.
func okTrue(t *testing.T) apiResult { return okResult(t, true) }

func rawResult(status int, body string) apiResult {
	return apiResult{status: status, body: []byte(body)}
}

func notFoundResult(what string) apiResult {
	return rawResult(404, fmt.Sprintf(`{"status":{"error":"Not found: %s"},"time":0.0}`, what))
}

func errorResult(status int, message string) apiResult {
	return rawResult(status, fmt.Sprintf(`{"status":{"error":%q},"time":0.0}`, message))
}

// --- collection fixtures ---

// liveCollection renders a GET /collections/{name} answer around a config, filling in
// the fields Qdrant always reports so the fixtures stay about the config.
func liveCollection(t *testing.T, config map[string]any) apiResult {
	t.Helper()
	return okResult(t, map[string]any{
		"status":                "green",
		"optimizer_status":      "ok",
		"points_count":          0,
		"segments_count":        7,
		"indexed_vectors_count": 0,
		"config":                config,
		"payload_schema":        map[string]any{},
	})
}

// defaultConfig is a collection as Qdrant actually reports one: every default filled
// in, `sharding_method` absent because it is "auto", `quantization_config` an explicit
// null. Copied from a live 1.18.3 so the subset comparison is tested against the shape
// it will really meet.
func defaultConfig(vectors map[string]any, overrides map[string]any) map[string]any {
	params := map[string]any{
		"vectors":                  vectors,
		"shard_number":             float64(1),
		"replication_factor":       float64(1),
		"write_consistency_factor": float64(1),
		"on_disk_payload":          true,
	}
	cfg := map[string]any{
		"params": params,
		"hnsw_config": map[string]any{
			"m": float64(16), "ef_construct": float64(100), "full_scan_threshold": float64(10000),
			"max_indexing_threads": float64(0), "on_disk": false,
		},
		"optimizer_config": map[string]any{
			"deleted_threshold": 0.2, "vacuum_min_vector_number": float64(1000),
			"default_segment_number": float64(0), "indexing_threshold": float64(10000),
			"flush_interval_sec": float64(5),
		},
		"wal_config": map[string]any{
			"wal_capacity_mb": float64(32), "wal_segments_ahead": float64(0), "wal_retain_closed": float64(1),
		},
		"quantization_config": nil,
	}
	for k, v := range overrides {
		if k == "params" {
			for pk, pv := range v.(map[string]any) {
				params[pk] = pv
			}
			continue
		}
		cfg[k] = v
	}
	return cfg
}

// unnamedVector is the single-vector spelling Qdrant echoes back.
func unnamedVector(size int, distance string) map[string]any {
	return map[string]any{"size": float64(size), "distance": distance}
}

// --- helpers ---

func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	return s
}

// moduleWith returns a QdrantModule wired to the fake.
func moduleWith(api qdrantAPI) *QdrantModule {
	return &QdrantModule{connect: func(context.Context, connConfig) (qdrantAPI, error) { return api, nil }}
}

// runApply dispatches one action through the real [object.Apply] path, so every test
// goes through the type check and the connect that production goes through.
func runApply(t *testing.T, obj *object, state string, params map[string]any) *applyStream {
	t.Helper()
	stream := &applyStream{}
	if err := obj.Apply(&pluginv1.ApplyRequest{State: state, Params: mustStruct(t, params)}, stream); err != nil {
		t.Fatalf("Apply returned a transport error: %v", err)
	}
	if stream.final() == nil {
		t.Fatal("Apply sent no final event")
	}
	return stream
}

// runValidate is the other half of the same call, for the parity assertions.
func runValidate(t *testing.T, obj *object, state string, params map[string]any) *pluginv1.ValidateReply {
	t.Helper()
	reply, err := obj.Validate(context.Background(), &pluginv1.ValidateRequest{
		State: state, Params: mustStruct(t, params),
	})
	if err != nil {
		t.Fatalf("Validate returned an error: %v", err)
	}
	return reply
}

// baseParams is a minimal reachable instance.
func baseParams(extra map[string]any) map[string]any {
	out := map[string]any{"addr": "qdrant-1:6333"}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// mustJSON renders a request body canonically (encoding/json sorts map keys), so an
// assertion compares one string rather than walking a nested map.
func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}
