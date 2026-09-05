// The `index` object — one payload field index on a collection (index.go).
//
// It is the object that shows where the line actually is. `collection` refuses to
// rebuild because rebuilding destroys points; `index` rebuilds without asking,
// because rebuilding an index destroys an index. Qdrant reads the same payloads back
// to build it. Treating the two the same — either by making this one ask permission
// or by letting that one recreate quietly — would blur the only distinction that
// matters here.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// index binds the object's actions to the shared driver.
func (m *QdrantModule) index() *object {
	return &object{
		impl: m,
		name: "index",
		decl: indexStates(),
		actions: map[string]action{
			"present": {validate: validateIndexPresent, apply: (*QdrantModule).applyIndexPresent},
			"absent":  {validate: validateIndexAbsent, apply: (*QdrantModule).applyIndexAbsent},
		},
	}
}

// indexDef is the object's entry in the artifact's bundle.
func indexDef(m *QdrantModule) module.Def {
	return module.Def{
		Name:         "index",
		Description:  "One payload field index of a Qdrant collection: the index that makes a filter on that field fast.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "qdrant"}},
		Impl:         m.index(),
		States:       indexStates(),
	}
}

// indexStates declares the parameters of every action this object serves.
func indexStates() map[string]module.State {
	subject := module.Input{
		"collection": collectionParam("Collection holding the field. Must already exist — this state does not create one."),
		"field":      collectionParam("Payload field name. Addresses a URL path segment, so it carries no whitespace, slash, \"?\" or \"#\"."),
	}

	present := module.Input{}
	for k, v := range subject {
		present[k] = v
	}
	present["schema"] = module.Param{Type: module.String, Required: true, Enum: payloadSchemaTypes,
		Description: "Field type to index as. Only the plain string schemas are managed here, because they are the ones Qdrant echoes back verbatim as `data_type` and can therefore be compared exactly. Qdrant's OBJECT schema form (a text index with a tokenizer, a parameterised integer index) is NOT supported: the read-back reports only the data type, so this module could not tell one tokenizer from another and would report converged on an index that is not. A half-compared index is worse than an unsupported one.",
	}

	return map[string]module.State{
		"present": {
			Description: "Ensure ONE payload field of a collection is indexed as the declared type\n" +
				"(no qdrant CLI, no shell). Built with wait=true, so the step returns when the\n" +
				"index is actually built rather than when the request was accepted.\n" +
				"\n" +
				"A field already indexed under a DIFFERENT type is dropped and built again,\n" +
				"in place and without asking. That is deliberate and it is the difference\n" +
				"between this object and qdrant.collection: rebuilding a payload index\n" +
				"destroys an INDEX, not data — the points and their payloads are untouched\n" +
				"and Qdrant reads them again. What it does cost is a WINDOW: between the drop\n" +
				"and the rebuild, filters on that field fall back to a full scan, which on a\n" +
				"large collection is slow rather than wrong. Schedule it accordingly.\n" +
				"\n" +
				"Idempotent: a field already indexed as the declared type is a no-op that\n" +
				"sends no request and reports changed=false. The result is read back from the\n" +
				"collection's payload_schema and an index that did not appear fails the step.\n" +
				"Output.previous carries the type it had before. No dry-run preview.",
			Input: withConnect(present),
		},
		"absent": {
			Description: "Remove ONE payload field index (no qdrant CLI, no shell). The field and its\n" +
				"values are untouched — only the index over them goes, so filters on it get\n" +
				"slower and nothing is lost.\n" +
				"\n" +
				"Idempotent: a field that is not indexed is a no-op that sends no request and\n" +
				"reports changed=false. Qdrant answers 200 to the deletion of an index that\n" +
				"does not exist, so the response is not the source of `changed` here either:\n" +
				"the payload_schema is read before and after. Output.previous carries the type\n" +
				"the index had. No dry-run preview.",
			Input: withConnect(subject),
		},
	}
}
