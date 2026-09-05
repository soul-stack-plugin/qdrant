// The `collection` object — one Qdrant collection: its existence and its shape.
//
// Four actions, and the interesting boundary is between two of them. `present`
// reconciles what Qdrant can reconcile on a live collection and REFUSES what it
// cannot. There is deliberately NO state here that rebuilds one: four independent
// reviews each found a fresh way for a drop-then-create to destroy data it could not
// then restore, so v1 refuses and says what it would take instead. The reason the
// boundary exists at all — Qdrant accepts an impossible update and silently discards it
// — is documented in collection.go.
package qdrant

import (
	"fmt"
	"strings"

	"github.com/souls-guild/soul-stack/sdk/module"
)

// distances is Qdrant's closed Distance set. Declared as an Enum on the param so the
// module form offers it, and checked here so a wrong one is refused before the run
// rather than as a 400 in the middle of it.
var distances = []string{"Cosine", "Euclid", "Dot", "Manhattan"}

// collection binds the object's actions to the shared driver.
func (m *Module) collection() *object {
	return &object{
		impl: m,
		name: "collection",
		decl: collectionStates(),
		actions: map[string]action{
			"probed":  {validate: validateCollectionProbed, apply: (*Module).applyCollectionProbed},
			"present": {validate: validateCollectionPresent, apply: (*Module).applyCollectionPresent},
			"absent":  {validate: validateCollectionAbsent, apply: (*Module).applyCollectionAbsent},
		},
	}
}

// collectionDef is the object's entry in the artifact's bundle.
func collectionDef(m *Module) module.Def {
	return module.Def{
		Name:         "collection",
		Description:  "One Qdrant collection: probed, declared with its vector and sharding parameters, or removed.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "qdrant"}},
		Impl:         m.collection(),
		States:       collectionStates(),
	}
}

// validateVectorSpec checks one vector's parameters. size and distance are what
// Qdrant requires and what cannot be changed later, so getting them wrong is worth
// catching before the collection exists.
func validateVectorSpec(spec map[string]any, addr string) []string {
	var errs []string

	switch size := spec["size"].(type) {
	case nil:
		errs = append(errs, addr+".size: is required (the vector dimension). It CANNOT be changed once the collection exists")
	case float64:
		if size <= 0 || size != float64(int64(size)) {
			errs = append(errs, fmt.Sprintf("%s.size: must be a positive integer, got %s", addr, renderValue(size)))
		}
	default:
		errs = append(errs, fmt.Sprintf("%s.size: must be a positive integer, got %T", addr, size))
	}

	switch distance := spec["distance"].(type) {
	case nil:
		errs = append(errs, fmt.Sprintf("%s.distance: is required, one of %s. It CANNOT be changed once the collection exists", addr, strings.Join(distances, "|")))
	case string:
		if !contains(distances, distance) {
			// Case matters to Qdrant, and "cosine" is the likeliest way to get
			// this wrong, so the message shows the accepted spelling rather than
			// only the set.
			errs = append(errs, fmt.Sprintf("%s.distance: must be one of %s (case-sensitive), got %q", addr, strings.Join(distances, "|"), distance))
		}
	default:
		errs = append(errs, fmt.Sprintf("%s.distance: must be a string, one of %s", addr, strings.Join(distances, "|")))
	}

	// A key this module does not manage is refused rather than passed through.
	// Qdrant discards a field it does not recognise and still answers 200, and
	// [planVectors] skips one it has no mutability for — so an unmanaged key would be
	// declared, ignored by both ends, and never once reported. Refusing it here is
	// the only place the author can be told.
	for _, key := range sortedKeys(spec) {
		if _, managed := vectorFields[key]; !managed {
			errs = append(errs, fmt.Sprintf(
				"%s.%s: not a vector setting this module manages (known: %s). Qdrant discards a field it does not recognise and still answers 200, so it would be silently ignored rather than applied",
				addr, key, strings.Join(sortedKeys(vectorFields), ", ")))
		}
	}

	return errs
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// --- declaration ---

// vectorsParam is the shared declaration of the vector layout. Lifted out because
// three states declare it and the description carries the load-bearing warning.
func vectorsParam() module.Param {
	return module.Param{Type: module.Map, Required: true,
		Description: "The collection's vector layout, in either of Qdrant's two spellings: one vector's parameters ({size: 768, distance: Cosine}) or a map of named vectors ({text: {size: 768, distance: Cosine}}). Told apart by a top-level `size`, the same way Qdrant tells them apart. Per vector, `size` and `distance` are required and CANNOT be changed on a live collection — Qdrant accepts such an update and silently discards it, so this module refuses it instead (see the state description). `hnsw_config`, `quantization_config` and `on_disk` inside a vector CAN be changed and are reconciled. `memory` is NOT managed: Qdrant accepts it and never reports it back, and this module decides `changed` by reading the resource back, so a key it cannot read is one it cannot honestly reconcile — it is refused rather than silently ignored. A named vector that is declared and missing is ADDED in place, which destroys nothing; a named vector that exists and is NOT declared is refused rather than dropped, because dropping it destroys that vector's data.",
	}
}

// collectionStates declares the parameters of every action this object serves.
// It is lifted out of [collectionDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value against
// (NIM-778), and a second copy of it would be a second answer.
func collectionStates() map[string]module.State {
	shape := module.Input{
		"name": collectionParam("Collection name. Addresses a URL path segment, so it carries no whitespace, slash, \"?\" or \"#\"."),

		"vectors": vectorsParam(),

		"shard_number": {Type: module.Int,
			Description: "Number of shards. CREATION-ONLY — Qdrant cannot reshard through this API, accepts the field in an update and discards it, so a difference is refused rather than sent.",
		},
		"sharding_method": {Type: module.String, Enum: []any{"auto", "custom"},
			Description: "\"auto\" (default) or \"custom\" (shard keys). CREATION-ONLY. Qdrant omits this from a collection left on \"auto\", which this module treats as \"auto\" rather than as drift.",
		},
		"replication_factor": {Type: module.Int,
			Description: "Replicas per shard. Reconciled on a live collection.",
		},
		"write_consistency_factor": {Type: module.Int,
			Description: "Replicas that must acknowledge a write. Reconciled on a live collection.",
		},
		"on_disk_payload": {Type: module.Bool,
			Description: "Keep payloads on disk rather than in RAM. Reconciled on a live collection.",
		},
		"hnsw_config": {Type: module.Map,
			Description: "Collection-level HNSW index settings (m, ef_construct, full_scan_threshold, max_indexing_threads, on_disk, payload_m). Reconciled on a live collection. Only the keys you declare are compared — Qdrant reports this map fully populated with its defaults, so comparing the whole thing would report drift on every run.",
		},
		"optimizers_config": {Type: module.Map,
			Description: "Optimizer settings (deleted_threshold, vacuum_min_vector_number, default_segment_number, max_segment_size, memmap_threshold, indexing_threshold, flush_interval_sec). A key Qdrant does not know is refused rather than sent — it would be discarded in silence and still answered 200. Reconciled on a live collection. Note Qdrant WRITES this as `optimizers_config` and READS it back as `optimizer_config`; the module handles the two spellings. Only declared keys are compared.",
		},
		"quantization_config": {Type: module.Map,
			Description: "Quantization settings (scalar / product / binary). Reconciled on a live collection. Only declared keys are compared.",
		},
		"wal_config": {Type: module.Map,
			Description: "Write-ahead-log settings (wal_capacity_mb, wal_segments_ahead). CREATION-ONLY — not part of Qdrant's update body, so a difference is refused rather than sent.",
		},
	}

	present := withConnect(shape)

	subject := withConnect(module.Input{
		"name": collectionParam("Collection name. Addresses a URL path segment, so it carries no whitespace, slash, \"?\" or \"#\"."),
	})

	return map[string]module.State{
		"probed": {
			Description: "Read a collection's state over the REST API (no qdrant CLI, no shell).\n" +
				"Read-only, changed=false by design.\n" +
				"\n" +
				"Output.exists says whether it is there at all — an ABSENT collection is a\n" +
				"reading, not a failure, because \"create it only if missing\" is a gate a\n" +
				"scenario has to be able to write. Output.status is Qdrant's own\n" +
				"green/yellow/grey/red, which is what to wait on after a restart or an index\n" +
				"change; Output.optimizer_status, .points_count, .segments_count and\n" +
				".indexed_vectors_count come along for diagnostics. No dry-run preview.",
			Input: subject,
		},
		"present": {
			Description: "Declare ONE collection and reconcile what CAN be reconciled (no qdrant CLI,\n" +
				"no shell). Creates it when missing.\n" +
				"\n" +
				"★ Qdrant divides a collection's settings in two, and this state respects the\n" +
				"line rather than pretending it is not there. `replication_factor`,\n" +
				"`write_consistency_factor`, `on_disk_payload`, `hnsw_config`,\n" +
				"`optimizers_config`, `quantization_config` and the per-vector `hnsw_config`/\n" +
				"`quantization_config`/`on_disk` are reconciled in place. A vector's\n" +
				"`size` and `distance`, plus `shard_number`, `sharding_method` and\n" +
				"`wal_config`, exist ONLY at creation time.\n" +
				"\n" +
				"If a live collection differs in one of those, this state FAILS and names the\n" +
				"field, its live value and the declared one. It does not quietly recreate the\n" +
				"collection — that would destroy every point in it — and it does not quietly\n" +
				"send the update either, because Qdrant ACCEPTS an update to those fields,\n" +
				"answers 200 {\"result\":true} and discards it. Reporting that as a change\n" +
				"would be a lie that repeats on every run.\n" +
				"\n" +
				"Idempotent: a collection already in the declared shape sends no request at\n" +
				"all and reports changed=false. `changed` is decided by comparing the\n" +
				"collection before and after, never from the response — Qdrant's response\n" +
				"does not distinguish an update it applied from one it threw away. Every\n" +
				"write is read back and verified for the same reason.\n" +
				"\n" +
				"Only DECLARED keys are compared, so a partial declaration converges; the\n" +
				"cost is that this state cannot reset a setting back to Qdrant's default.\n" +
				"Points, aliases and payload indexes are not its subject — see qdrant.alias\n" +
				"and qdrant.index. No dry-run preview.",
			Input: present,
		},
		"absent": {
			Description: "Remove ONE collection and everything in it (no qdrant CLI, no shell).\n" +
				"\n" +
				"Idempotent: a collection that is not there is a no-op that sends no DELETE\n" +
				"(changed=false). It does READ first — Qdrant answers 200 {\"result\":false}\n" +
				"to a delete that removed nothing, and this state reports what actually\n" +
				"happened rather than what was asked. The removal is verified by reading the\n" +
				"collection back.\n" +
				"\n" +
				"This destroys the collection's contents, and it carries no confirmation flag\n" +
				"because `absent` IS the declaration — there is no reading of it under which\n" +
				"the data survives. No dry-run preview.",
			Input: subject,
		},
	}
}
