// The HTTP transport every object talks through, and the envelope Qdrant answers in.
//
// The api-key travels in the `api-key` REQUEST HEADER and nowhere else. That is not
// only convention: measured against a live 1.18.3, Qdrant answers 401 to
// `?api_key=<key>` in the query string, so there is no URL form of the credential to
// leak into a log line, a redirect or a proxy access log — and no argv form either,
// since nothing here spawns a process.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// defaultTimeoutSec bounds every request. Qdrant's collection operations can be slow
// (a create waits for the shards), and an unbounded client would hang a run rather
// than fail it.
const defaultTimeoutSec = 30

// connConfig holds the connection parameters. apiKey and tls.*PEM are kept apart
// from everything that reaches an event (ADR-010).
type connConfig struct {
	addr       string // host:port of the REST API
	apiKey     string
	tls        tlsParams // enabled=false → plaintext http://
	timeoutSec int
}

// baseURL is the scheme+authority every request is built on. The scheme follows
// tls.enabled — there is one place that decides it, and it is the same boolean the
// type check protects.
func (c connConfig) baseURL() string {
	scheme := "http"
	if c.tls.enabled {
		scheme = "https"
	}
	return scheme + "://" + c.addr
}

// parseConnConfig pulls the connection parameters from params.
func parseConnConfig(s *structpb.Struct) (connConfig, error) {
	f := s.GetFields()
	addr := strings.TrimSpace(stringOrEmpty(f["addr"]))
	if addr == "" {
		return connConfig{}, fmt.Errorf("params.addr: must be a non-empty string (host:port of the Qdrant REST API, e.g. %q)", "qdrant-1:6333")
	}
	// A scheme in `addr` is refused rather than accommodated: `tls` decides the
	// scheme, and honouring "https://host:6333" here would give two sources for one
	// answer — with `tls: false` beside it the plugin would have to pick, and either
	// choice silently contradicts the other parameter.
	if strings.Contains(addr, "://") {
		return connConfig{}, fmt.Errorf("params.addr: must be host:port without a scheme (use params.tls to choose http/https), got %q", addr)
	}
	timeout := intOrDefault(f["timeout_sec"], defaultTimeoutSec)
	if timeout <= 0 {
		return connConfig{}, fmt.Errorf("params.timeout_sec: must be a positive integer (seconds), got %d", timeout)
	}
	return connConfig{
		addr:       addr,
		apiKey:     stringOrEmpty(f["api_key"]),
		tls:        parseTLS(f),
		timeoutSec: timeout,
	}, nil
}

// qdrantAPI is the narrow surface the objects use — one request in, one raw answer
// out. Narrow on purpose: an L0 fake implements one method, and every response-shape
// decision stays in [apiResult] where it is tested once.
type qdrantAPI interface {
	// do performs one request. body is marshalled as JSON when non-nil. A non-2xx
	// answer is NOT an error — it is an [apiResult] with that status, because
	// Qdrant reports "no such collection" as a 404 an action often wants to read
	// rather than fail on. An error means the request never completed.
	do(ctx context.Context, method, path string, body any) (apiResult, error)
	close()
}

// apiResult is one raw answer. Qdrant wraps every JSON response in
// `{"result": …, "status": "ok"|{"error": …}, "time": …}`; the health endpoints
// answer in plain text instead, which is why the body is kept raw here and decoded
// by the caller that knows which it asked for.
type apiResult struct {
	status int
	body   []byte
}

// ok reports a 2xx. It says nothing about whether the request CHANGED anything —
// Qdrant answers 200 `{"result":true}` to an update it silently discarded, which is
// why no action in this artifact derives `changed` from this (see impl.go).
func (r apiResult) ok() bool { return r.status >= 200 && r.status < 300 }

// notFound reports the 404 Qdrant uses for an absent collection or snapshot.
func (r apiResult) notFound() bool { return r.status == http.StatusNotFound }

// envelope is the wrapper around every JSON response.
type envelope struct {
	Result json.RawMessage `json:"result"`
	Status json.RawMessage `json:"status"`
}

// errorText renders a failed answer for a human: Qdrant's own message when it sent
// one, the raw body otherwise. Always prefixed with the status, because a 502 from a
// proxy in front of Qdrant has no envelope at all and "unexpected end of JSON input"
// would be a worse thing to show an operator than the status code.
func (r apiResult) errorText() string {
	var env envelope
	if err := json.Unmarshal(r.body, &env); err == nil && len(env.Status) > 0 {
		var detail struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(env.Status, &detail); err == nil && detail.Error != "" {
			return fmt.Sprintf("HTTP %d: %s", r.status, detail.Error)
		}
	}
	body := strings.TrimSpace(string(r.body))
	if body == "" {
		return fmt.Sprintf("HTTP %d", r.status)
	}
	const maxBody = 512
	if len(body) > maxBody {
		body = body[:maxBody] + "…"
	}
	return fmt.Sprintf("HTTP %d: %s", r.status, body)
}

// decodeResult unmarshals the envelope's `result` member into v.
func (r apiResult) decodeResult(v any) error {
	var env envelope
	if err := json.Unmarshal(r.body, &env); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if len(env.Result) == 0 {
		return fmt.Errorf("response carries no result member")
	}
	if err := json.Unmarshal(env.Result, v); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	return nil
}

// text is the trimmed body, for the health endpoints that answer in plain text
// rather than the envelope.
func (r apiResult) text() string { return strings.TrimSpace(string(r.body)) }

// httpClient is the real transport.
type httpClient struct {
	base   string
	apiKey string
	cli    *http.Client
}

// newHTTPClient builds the client. A broken PEM fails here, before any socket.
func newHTTPClient(cfg connConfig) (qdrantAPI, error) {
	tlsCfg, err := buildTLSConfig(cfg.tls)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	return &httpClient{
		base:   cfg.baseURL(),
		apiKey: cfg.apiKey,
		cli: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(cfg.timeoutSec) * time.Second,
		},
	}, nil
}

func (c *httpClient) do(ctx context.Context, method, path string, body any) (apiResult, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return apiResult{}, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return apiResult{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("api-key", c.apiKey)
	}

	resp, err := c.cli.Do(req)
	if err != nil {
		return apiResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiResult{}, fmt.Errorf("read response: %w", err)
	}
	return apiResult{status: resp.StatusCode, body: raw}, nil
}

func (c *httpClient) close() { c.cli.CloseIdleConnections() }
