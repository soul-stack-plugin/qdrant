// The `alias` object — a second name for a collection, and the thing a blue-green
// swap actually moves (alias.go).
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// alias binds the object's actions to the shared driver.
func (m *QdrantModule) alias() *object {
	return &object{
		impl: m,
		name: "alias",
		decl: aliasStates(),
		actions: map[string]action{
			"present": {validate: validateAliasPresent, apply: (*QdrantModule).applyAliasPresent},
			"absent":  {validate: validateAliasAbsent, apply: (*QdrantModule).applyAliasAbsent},
		},
	}
}

// aliasDef is the object's entry in the artifact's bundle.
func aliasDef(m *QdrantModule) module.Def {
	return module.Def{
		Name:         "alias",
		Description:  "One Qdrant collection alias: the name clients use, pointed at the collection that should serve them.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "qdrant"}},
		Impl:         m.alias(),
		States:       aliasStates(),
	}
}

// aliasStates declares the parameters of every action this object serves.
func aliasStates() map[string]module.State {
	return map[string]module.State{
		"present": {
			Description: "Point ONE alias at ONE collection over Qdrant's REST API (no qdrant CLI,\n" +
				"no shell). Creates the alias when it does not exist and RE-POINTS it when it\n" +
				"points somewhere else — re-pointing is the blue-green swap this object exists\n" +
				"for, it costs no data, and Qdrant applies the change atomically, so clients\n" +
				"never see the alias resolve to nothing.\n" +
				"\n" +
				"The target collection is checked first: an alias to a collection that does\n" +
				"not exist fails with both names in the message, rather than with Qdrant's own\n" +
				"error, which names only the alias.\n" +
				"\n" +
				"Idempotent: an alias already pointing at the declared collection is a no-op\n" +
				"that sends no mutating request and reports changed=false. `changed` comes\n" +
				"from reading the alias list before and after, NEVER from the response —\n" +
				"Qdrant answers 200 to a create that re-points and to one that changes\n" +
				"nothing, so the response cannot tell the two apart. Output.previous carries\n" +
				"what the alias used to point at, which is what a rollback step needs.\n" +
				"No dry-run preview.",
			Input: withConnect(module.Input{
				"name": collectionParam("The alias — the name clients connect to. A URL-unsafe value is refused."),
				"collection": collectionParam(
					"The collection the alias must resolve to. Must already exist."),
			}),
		},
		"absent": {
			Description: "Remove ONE alias (no qdrant CLI, no shell). The collection it pointed at is\n" +
				"untouched — an alias is a name, and removing it removes nothing else.\n" +
				"\n" +
				"Idempotent: an alias that is not there is a no-op that sends no mutating\n" +
				"request and reports changed=false. The list is read first because Qdrant\n" +
				"answers 200 to the deletion of an alias that does not exist, so acting on the\n" +
				"response would report a change on every run. The list is read again\n" +
				"afterwards and a removal that did not take fails the step.\n" +
				"Output.previous carries what it pointed at. No dry-run preview.",
			Input: withConnect(module.Input{
				"name": collectionParam("The alias to remove."),
			}),
		},
	}
}
