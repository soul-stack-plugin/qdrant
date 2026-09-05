// qdrant is a SoulModule plugin for Soul Stack: the primary interface to a live
// Qdrant vector database. A service scenario orchestrates order and targeting, the
// plugin performs ONE operation against one instance.
//
// The artifact serves FIVE objects — address level 2 of `qdrant.<object>.<action>`,
// where level 1 is the alias an operator registers it under (ADR-020 amendment
// 2026-09-02):
//
//	instance   — pinged / ready-probed / version-probed (read probes,
//	             changed=false by design, see probe.go)
//	collection — probed (read), present / recreated / absent (collection.go)
//	alias      — present / absent (alias.go)
//	index      — present / absent (payload field index, index.go)
//	snapshot   — created / absent (snapshot.go)
//
// The object tables and the dispatch live in object.go.
//
// Everything goes over Qdrant's own REST API on net/http — no qdrant CLI, no shell,
// and no third-party Qdrant client either. The API is fully expressed in JSON, so a
// gRPC client would add a dependency tree to an artifact whose sha256 an operator
// has to approve, and buy nothing.
//
// # Why every action reads the state back
//
// Qdrant's mutating endpoints do NOT report whether they did anything, and worse,
// they do not report whether they UNDERSTOOD what was asked. Measured against a live
// 1.18.3 (2026-09-05):
//
//	PATCH /collections/c {"vectors":{"":{"size":8}}}    -> 200 {"result":true}, size stayed 4
//	PATCH /collections/c {"params":{"shard_number":4}}  -> 200 {"result":true}, shards stayed 2
//	PATCH /collections/c {"bogus_top":1}                -> 200 {"result":true}
//
// Unknown fields are discarded in silence; only a wrong JSON *type* on a known field
// is a 400. So `{"result":true}` is not evidence that anything happened, and a plugin
// that reported `changed` from the response would report a reconciled collection
// forever while the collection never moved.
//
// Two consequences run through every object here. `changed` is computed by comparing
// the resource BEFORE and AFTER, never from the response; and a field Qdrant cannot
// change on a live collection is refused up front rather than sent and hoped for
// (collection.go).
//
// Intentionally without dry-run preview: the plugin is on BaseModule and does NOT
// implement PlanReadSafe, so the host applies default-deny — on dry_run a task gets an
// honest "drift not supported" instead of a false "no drift" (decision 2026-06-22,
// same as the redis artifact).
//
// CRITICAL SECURITY (ADR-010): params["api_key"] and the TLS PEMs NEVER reach
// ApplyEvent.Message, .Output, error text or stderr. The key travels in the `api-key`
// HTTP header — never in the URL, never in a command argument, so `ps` on the host
// cannot see it. Transport errors are sanitized (redactError).
package main

import (
	"context"
	"time"

	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/protobuf/types/known/structpb"
)

// QdrantModule is the driver every object of this artifact delegates to — one body
// of code behind five serving surfaces (see object.go). It is not itself a
// SoulModule: what the host dispatches to is an [object], which owns the actions it
// serves and calls the applyXxx methods on this value.
//
// BaseModule provides the no-op Plan (without PlanReadSafe → default-deny on
// dry_run) and deliberately does NOT implement ErrandReadSafe (default-deny on
// Errand); [object] embeds BaseModule for the same reason.
type QdrantModule struct {
	module.BaseModule

	// connect is an injection point for L0. nil → the real net/http client
	// ([newHTTPClient]).
	connect func(ctx context.Context, cfg connConfig) (qdrantAPI, error)

	// clock is the second injection point, for the one action that reads the time:
	// snapshot.created compares a snapshot's age against max_age_sec, and a test of
	// that window must not be a race against the wall clock. nil → time.Now.
	clock func() time.Time
}

// nowUTC is the current time, honouring the [QdrantModule.clock] injection point.
// Always UTC, because that is what Qdrant timestamps a snapshot in.
func (m *QdrantModule) nowUTC() time.Time {
	if m.clock != nil {
		return m.clock().UTC()
	}
	return time.Now().UTC()
}

// open returns a client for cfg, honouring the L0 injection point.
func (m *QdrantModule) open(ctx context.Context, cfg connConfig) (qdrantAPI, error) {
	if m.connect != nil {
		return m.connect(ctx, cfg)
	}
	return newHTTPClient(cfg)
}

// validateProbe — the check shared by every action whose only requirement is a
// reachable instance: the three instance probes take no arguments of their own.
func validateProbe(f map[string]*structpb.Value) []string {
	return validateAddr(f)
}
