// The objects this artifact serves — address level 2 of `qdrant.<object>.<action>`
// (ADR-020 amendment 2026-09-02, NIM-765/NIM-766).
//
// One artifact, five objects, one body of REST code. Every object is the same
// [object] value with a different action table; the tables ARE the boundary, so
// `instance` cannot reach a collection action by accident — that state is simply
// unknown to it.
package qdrant

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// eventStream is the Apply server stream, named once so the action tables stay
// readable.
type eventStream = grpc.ServerStreamingServer[pluginv1.ApplyEvent]

// action is one state of an object — address level 3. apply gets a client already
// pointed at `params.addr`; every action of this artifact addresses exactly one
// instance, so unlike the redis artifact there is no multi-node variant.
type action struct {
	validate func(f map[string]*structpb.Value) []string
	apply    func(m *Module, ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error
}

// object is one addressable object of this artifact — the `collection` in
// `qdrant.collection.present`. It serves the actions in its table and nothing else.
//
// It implements SoulModule, so the value goes straight into [module.Def].Impl;
// BaseModule supplies the no-op Plan, which keeps the deliberate default-deny on
// dry_run (no PlanReadSafe) and on Errand (no ErrandReadSafe).
type object struct {
	module.BaseModule

	// impl is the shared REST driver. Five objects, one client.
	impl *Module

	// name is address level 2 — used in diagnostics only; what an operator
	// actually addresses is the registration alias plus this name.
	name string

	// decl is what this object's Def declares about each of its actions — the
	// same map, from the same function, not a copy. Validate and Apply refuse a
	// param whose value is not of the declared type (params.go, NIM-778), so the
	// declaration is load-bearing at runtime and not only in the schema document.
	decl map[string]module.State

	actions map[string]action
}

// Validate performs runtime checks on top of the static ones from soul-lint.
// Returns a ValidateReply with errors (not an error) — that is the Validate
// contract. Error text does NOT contain the api-key.
//
// What it can and cannot cover is worth being exact about (NIM-786): everything
// checkable from the declaration alone is checked HERE, so the run refuses before
// anything happens. What is only knowable against the live instance — whether a
// collection's immutable shape already differs — cannot be, and collection.go
// therefore re-checks it in Apply before the first mutation rather than after.
func (o *object) Validate(_ context.Context, req *pluginv1.ValidateRequest) (*pluginv1.ValidateReply, error) {
	act, ok := o.actions[req.GetState()]
	if !ok {
		return &pluginv1.ValidateReply{Ok: false, Errors: []string{o.unknownState(req.GetState())}}, nil
	}
	// Types before content: an action's own checks read the values, and a value
	// of the wrong type makes whatever they report about it noise.
	if errs := checkParamTypes(o.decl[req.GetState()].Input, req.GetParams().GetFields()); len(errs) > 0 {
		return &pluginv1.ValidateReply{Ok: false, Errors: errs}, nil
	}
	if errs := act.validate(req.GetParams().GetFields()); len(errs) > 0 {
		return &pluginv1.ValidateReply{Ok: false, Errors: errs}, nil
	}
	return &pluginv1.ValidateReply{Ok: true}, nil
}

// Apply dispatches by state within this object. The final event carries
// changed/failed + output (ADR-012). Transport errors are sanitized (redactError) —
// the address is preserved for diagnostics, the api-key and PEMs stripped.
func (o *object) Apply(req *pluginv1.ApplyRequest, stream eventStream) error {
	ctx := stream.Context()

	act, ok := o.actions[req.GetState()]
	if !ok {
		return sendFailure(stream, o.unknownState(req.GetState()))
	}

	// Before anything opens a socket: a param of the wrong type is refused, not
	// coerced (params.go, NIM-778). Here rather than only in Validate because a
	// runner need not call Validate at all — the runtime calls Apply — and the
	// value this protects decides whether the api-key goes out over TLS.
	if errs := checkParamTypes(o.decl[req.GetState()].Input, req.GetParams().GetFields()); len(errs) > 0 {
		return sendFailure(stream, strings.Join(errs, "; "))
	}

	// The action's own static checks run in Apply too, and for the same reason:
	// Validate is a separate RPC a runner need not call, so a check that lives
	// only there is a check that does not run on the path that mutates (NIM-786).
	if errs := act.validate(req.GetParams().GetFields()); len(errs) > 0 {
		return sendFailure(stream, strings.Join(errs, "; "))
	}

	cfg, err := parseConnConfig(req.GetParams())
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	api, err := o.impl.open(ctx, cfg)
	if err != nil {
		// Redact BOTH the api-key and the PEM client-key: a TLS handshake error
		// could theoretically carry the key material (ADR-010).
		return sendFailure(stream, "connect: "+redactError(err, cfg.apiKey, cfg.tls.keyPEM))
	}
	defer api.close()

	return act.apply(o.impl, ctx, stream, api, req.GetParams())
}

// unknownState names the object as well as the state: with five objects in one
// artifact, "unknown state" alone would leave an author guessing whether the word
// is wrong or the object is.
func (o *object) unknownState(state string) string {
	names := make([]string, 0, len(o.actions))
	for name := range o.actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Sprintf("unknown state %q for object %q (expected %s)", state, o.name, strings.Join(names, "|"))
}

// states returns the action names this object serves, for the guard that keeps the
// schema document and the dispatch table from drifting apart.
func (o *object) states() []string {
	names := make([]string, 0, len(o.actions))
	for name := range o.actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
