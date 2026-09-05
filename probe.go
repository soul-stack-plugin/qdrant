// The instance probes — read-only operations against a live Qdrant, entirely over
// its REST API (no qdrant CLI, no shell; the only capability is network_outbound).
// All three are `changed=false` BY CONSTRUCTION: a probe is a measurement, not a
// mutation, and Output carries the reading for a health gate (retry/until/
// failed_when) to decide on.
//
//	pinged         — GET /healthz. Output.result is the server's own line
//	                 ("healthz check passed"), so a gate can compare it the way
//	                 register.self.result is compared on the redis artifact.
//	ready-probed   — GET /readyz. Ready means every shard is loaded and the
//	                 instance can serve, which is what a rolling restart waits on;
//	                 healthz answers long before that.
//	version-probed — GET /. Output.version is what a scenario gates on before using
//	                 a feature a older Qdrant does not have.
//
// Why `pinged` FAILS on a bad answer while `ready-probed` REPORTS one: they are asked
// different questions. `pinged` asks "is this instance answering at all", and a
// negative is an error worth stopping on. `ready-probed` asks "is it ready YET", and
// a not-yet is the normal reading during a restart — returning it as a failure would
// make `until: register.self.ready` unusable, because the run would end before the
// gate ever got to look.
package main

import (
	"context"
	"encoding/json"
	"net/http"

	"google.golang.org/protobuf/types/known/structpb"
)

// applyPinged — liveness probe. GET /healthz needs no api-key on a Qdrant that has
// one configured, so this reaches an instance whose credential a scenario does not
// hold.
func (m *QdrantModule) applyPinged(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	apiKey := stringOrEmpty(params.GetFields()["api_key"])
	res, err := api.do(ctx, http.MethodGet, "/healthz", nil)
	if err != nil {
		return sendFailure(stream, "GET /healthz: "+redactError(err, apiKey))
	}
	if !res.ok() {
		return sendFailure(stream, "GET /healthz: "+redactString(res.errorText(), apiKey))
	}
	return sendOutcome(stream, false, "healthz ok", map[string]any{
		"result": res.text(),
	})
}

// applyReadyProbed — readiness probe. A non-2xx is a READING, not a failure: Qdrant
// answers 503 while shards are still loading, and that is precisely the state a gate
// is waiting out.
func (m *QdrantModule) applyReadyProbed(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	apiKey := stringOrEmpty(params.GetFields()["api_key"])
	res, err := api.do(ctx, http.MethodGet, "/readyz", nil)
	if err != nil {
		// A transport error is not a reading — nothing answered, so there is no
		// "not ready yet" to report. Distinguishing the two matters: a gate that
		// treats an unreachable host as "still starting" waits out its whole
		// retry budget on an instance that is never coming back.
		return sendFailure(stream, "GET /readyz: "+redactError(err, apiKey))
	}
	ready := res.ok()
	message := "ready"
	if !ready {
		message = "not ready: " + redactString(res.errorText(), apiKey)
	}
	return sendOutcome(stream, false, message, map[string]any{
		"ready":  ready,
		"result": res.text(),
	})
}

// versionInfo is the root document. Note it is NOT wrapped in the usual
// `{"result":…}` envelope — the root endpoint answers with the bare object, so this
// is decoded directly rather than through [apiResult.decodeResult].
type versionInfo struct {
	Title   string `json:"title"`
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

// applyVersionProbed — version probe. Output.version is the plain "1.18.3" string, so
// a scenario compares it directly.
func (m *QdrantModule) applyVersionProbed(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	apiKey := stringOrEmpty(params.GetFields()["api_key"])
	res, err := api.do(ctx, http.MethodGet, "/", nil)
	if err != nil {
		return sendFailure(stream, "GET /: "+redactError(err, apiKey))
	}
	if !res.ok() {
		return sendFailure(stream, "GET /: "+redactString(res.errorText(), apiKey))
	}
	var info versionInfo
	if err := json.Unmarshal(res.body, &info); err != nil {
		return sendFailure(stream, "GET /: decode: "+redactError(err, apiKey))
	}
	if info.Version == "" {
		// An answering endpoint with no version field is not this API — most
		// likely a proxy or a login page on the address. Saying so beats handing a
		// scenario an empty string to compare against.
		return sendFailure(stream, "GET /: the response carries no version field — is params.addr a Qdrant REST API?")
	}
	return sendOutcome(stream, false, "version: "+info.Version, map[string]any{
		"version": info.Version,
		"commit":  info.Commit,
		"title":   info.Title,
	})
}
