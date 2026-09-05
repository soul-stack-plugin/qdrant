// The `instance` object — a live Qdrant server as a whole: is it answering, is it
// ready to serve, and what version is it.
//
// All three actions are read probes (changed=false by design, probe.go). There is
// deliberately no `configured` state here, and its absence is a fact about Qdrant
// rather than an omission: Qdrant has no runtime equivalent of redis's CONFIG SET —
// the server settings are read from its configuration file at startup, so a live
// instance cannot be reconfigured through the API at all. What IS reconfigurable at
// runtime belongs to a collection, and lives on the `collection` object.
package qdrant

import "github.com/souls-guild/soul-stack/sdk/module"

// instance binds the object's actions to the shared driver. The table is the
// object's boundary: nothing else in this artifact is reachable through it.
func (m *Module) instance() *object {
	return &object{
		impl: m,
		name: "instance",
		decl: instanceStates(),
		actions: map[string]action{
			"pinged":         {validate: validateProbe, apply: (*Module).applyPinged},
			"ready-probed":   {validate: validateProbe, apply: (*Module).applyReadyProbed},
			"version-probed": {validate: validateProbe, apply: (*Module).applyVersionProbed},
		},
	}
}

// instanceDef is the object's entry in the artifact's bundle.
func instanceDef(m *Module) module.Def {
	return module.Def{
		Name:         "instance",
		Description:  "A live Qdrant server: liveness, readiness and version probes.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "qdrant"}},
		Impl:         m.instance(),
		States:       instanceStates(),
	}
}

// instanceStates declares the parameters of every action this object serves.
// It is lifted out of [instanceDef] because [object] reads it too: the declared type
// of a parameter is what Validate and Apply refuse a wrong-typed value against
// (NIM-778), and a second copy of it would be a second answer.
func instanceStates() map[string]module.State {
	return map[string]module.State{
		"pinged": {
			Description: "Liveness-probe Qdrant with GET /healthz (no qdrant CLI, no shell).\n" +
				"Read-only, changed=false by design — a probe, not a mutation.\n" +
				"Output.result carries the server's own line (\"healthz check passed\"), so\n" +
				"register.self.result works in a health gate (retry/until/failed_when) the\n" +
				"same way it does on the redis artifact.\n" +
				"\n" +
				"/healthz is served WITHOUT authentication even on an instance that has an\n" +
				"api-key configured, so this state reaches a Qdrant whose credential the\n" +
				"scenario does not hold. A non-2xx answer FAILS the step: the question is\n" +
				"\"is it answering at all\", and a no is worth stopping on. No dry-run preview.",
			Input: connectInput(),
		},
		"ready-probed": {
			Description: "Readiness-probe Qdrant with GET /readyz (no qdrant CLI, no shell).\n" +
				"Read-only, changed=false by design. Ready means every shard is loaded and\n" +
				"the instance can serve queries — which is what a rolling restart must wait\n" +
				"on, because /healthz answers long before that point.\n" +
				"\n" +
				"A not-ready answer is a READING, not a failure: Output.ready is false and\n" +
				"the step succeeds, so `until: register.self.ready` can wait it out. Failing\n" +
				"instead would end the run before the gate ever looked. An UNREACHABLE\n" +
				"instance does fail, and the distinction is load-bearing: a gate that reads\n" +
				"\"host is gone\" as \"still starting\" burns its whole retry budget on an\n" +
				"instance that is never coming back. No dry-run preview.",
			Input: connectInput(),
		},
		"version-probed": {
			Description: "Version-probe Qdrant with GET / (no qdrant CLI, no shell). Read-only,\n" +
				"changed=false by design. Output.version is the plain version string\n" +
				"(\"1.18.3\"), Output.commit the build commit, Output.title the server banner.\n" +
				"\n" +
				"Use it to gate a step on a feature the running Qdrant may not have. An\n" +
				"answer with no version field FAILS rather than reporting an empty string:\n" +
				"something is answering on that address, but it is not this API. No dry-run\n" +
				"preview.",
			Input: connectInput(),
		},
	}
}
