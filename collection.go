// The `collection` object's implementation — and the one place in this artifact
// where getting it wrong costs data.
//
// # What Qdrant can and cannot change on a live collection
//
// Part of a collection's configuration is reconcilable in place through
// PATCH /collections/{name}; the rest exists only at creation time and can be
// reached only by dropping the collection and making a new one, which destroys every
// point in it. The split is not a matter of taste — it is the difference between the
// `UpdateCollection` and `CreateCollection` request bodies in Qdrant's own schema,
// and it is encoded once in [collectionFields] and [vectorFields] below.
//
// # Why this is dangerous rather than merely awkward
//
// Qdrant does not refuse an immutable field in a PATCH. It accepts it, answers
// 200 `{"result":true,"status":"ok"}`, and discards it. Measured on a live 1.18.3
// (2026-09-05), against a collection created with size 4 / Cosine / 2 shards:
//
//	PATCH {"vectors":{"":{"size":8}}}      -> 200 {"result":true}   size stayed 4
//	PATCH {"vectors":{"":{"distance":…}}}  -> 200 {"result":true}   distance stayed Cosine
//	PATCH {"params":{"shard_number":4}}    -> 200 {"result":true}   shards stayed 2
//	PATCH {"bogus_top":1}                  -> 200 {"result":true}
//
// Unknown fields are dropped in silence; only a wrong JSON type on a KNOWN field is a
// 400. So a plugin that sent the whole declared config and trusted the answer would
// report a reconciled collection on every run, forever, while the collection never
// moved — and an operator reading `changed=true` would believe the opposite of what
// happened.
//
// Hence the shape of `present`: the immutable half is compared and REFUSED before
// anything is sent, and the mutable half is verified by reading the collection back
// rather than by believing the response. `recreated` is the same code with the
// authority to drop and rebuild, taken explicitly through confirm_destroy.
//
// # Comparison is by DECLARED keys only
//
// A live config is fully populated with defaults — `hnsw_config` comes back with all
// five of its keys whether or not the author set any — and a nested PATCH merges
// rather than replaces. Comparing whole maps would therefore report drift on every
// run for any collection whose declaration is partial, which is all of them. So
// [subsetEquals] walks only what the declaration mentions. The cost is that this
// object cannot express "unset this back to the default"; that is a real limit and it
// is the right side to err on, because the alternative is a plugin that never
// converges.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// collectionField is one managed top-level setting of a collection: what an author
// declares, where it lives when read back, where it goes when the collection is
// created, and where it goes when it is patched.
//
// patchPath == nil is the whole point of this table: it means Qdrant CANNOT change
// this on a live collection, so a difference is refused rather than sent.
type collectionField struct {
	param string

	// livePath is the path inside `result.config` of GET /collections/{name}.
	livePath []string

	// createPath is the path inside the CreateCollection body.
	createPath []string

	// patchPath is the path inside the UpdateCollection body, or nil when the
	// setting is creation-only.
	patchPath []string

	// liveDefault is what an ABSENT live value means. Qdrant omits a setting left
	// at its default (`sharding_method` is absent on an auto-sharded collection),
	// and without this a declaration that spells the default out would read as
	// permanent drift on a collection that already matches it.
	liveDefault any
}

// collectionFields is the managed surface of a collection, derived from the
// difference between Qdrant's CreateCollection and UpdateCollection bodies.
//
// Two spellings worth noticing, because both are silent traps. The optimizer settings
// are `optimizers_config` when WRITTEN and `optimizer_config` — singular — when READ
// BACK; a table that used one name for both would compare a declaration against a
// missing key and report drift forever. And the four scalars below sit at the TOP
// level of a create body but under `params` in a patch body, so one path cannot serve
// both.
var collectionFields = []collectionField{
	{
		param:      "shard_number",
		livePath:   []string{"params", "shard_number"},
		createPath: []string{"shard_number"},
		patchPath:  nil, // creation-only: resharding is not what this state does
	},
	{
		param:      "sharding_method",
		livePath:   []string{"params", "sharding_method"},
		createPath: []string{"sharding_method"},
		patchPath:  nil, // creation-only
		// Omitted from the live config when auto — the documented default.
		liveDefault: "auto",
	},
	{
		param:      "replication_factor",
		livePath:   []string{"params", "replication_factor"},
		createPath: []string{"replication_factor"},
		patchPath:  []string{"params", "replication_factor"},
	},
	{
		param:      "write_consistency_factor",
		livePath:   []string{"params", "write_consistency_factor"},
		createPath: []string{"write_consistency_factor"},
		patchPath:  []string{"params", "write_consistency_factor"},
	},
	{
		param:      "on_disk_payload",
		livePath:   []string{"params", "on_disk_payload"},
		createPath: []string{"on_disk_payload"},
		patchPath:  []string{"params", "on_disk_payload"},
	},
	{
		param:      "hnsw_config",
		livePath:   []string{"hnsw_config"},
		createPath: []string{"hnsw_config"},
		patchPath:  []string{"hnsw_config"},
	},
	{
		param:      "optimizers_config",
		livePath:   []string{"optimizer_config"}, // singular when read back — not a typo
		createPath: []string{"optimizers_config"},
		patchPath:  []string{"optimizers_config"},
	},
	{
		param:      "quantization_config",
		livePath:   []string{"quantization_config"},
		createPath: []string{"quantization_config"},
		patchPath:  []string{"quantization_config"},
	},
	{
		param:      "wal_config",
		livePath:   []string{"wal_config"},
		createPath: []string{"wal_config"},
		patchPath:  nil, // creation-only
	},
}

// vectorFields classifies the keys INSIDE one vector's parameters. Same rule as
// above: a key absent from this map is not managed, a key mapped to false is
// creation-only and a difference in it is refused.
//
// `size` and `distance` are the two that matter in practice: they are what an author
// changes when the embedding model changes, they are exactly what cannot be changed
// in place, and Qdrant accepts an update to either without doing anything.
var vectorFields = map[string]bool{
	"size":               false,
	"distance":           false,
	"datatype":           false,
	"multivector_config": false,

	"hnsw_config":         true,
	"quantization_config": true,
	"on_disk":             true,
	"memory":              true,
}

// conflict is one declared setting Qdrant cannot reconcile on a live collection.
type conflict struct {
	// path is the operator-facing address of the setting, e.g.
	// `params.vectors."".size`.
	path     string
	live     any
	declared any
}

func (c conflict) String() string {
	return fmt.Sprintf("%s is %s, declared %s", c.path, renderValue(c.live), renderValue(c.declared))
}

// renderValue prints a config value the way an operator wrote it, so the message
// reads like the YAML rather than like Go. A float that is whole prints without the
// decimal point: every number arrives as a float64 through both JSON and structpb,
// and "shard_number is 2" beats "shard_number is 2e+00".
// A map is rendered with its keys SORTED. Go prints a map in a random order, and a
// conflict message is compared in tests and read in run logs — an unstable rendering
// would make a passing assertion a matter of luck.
func renderValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "unset"
	case string:
		return fmt.Sprintf("%q", t)
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%g", t)
	case bool:
		return fmt.Sprintf("%t", t)
	case map[string]any:
		parts := make([]string, 0, len(t))
		for _, key := range sortedKeys(t) {
			parts = append(parts, key+": "+renderValue(t[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case []any:
		parts := make([]string, 0, len(t))
		for _, item := range t {
			parts = append(parts, renderValue(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return fmt.Sprintf("%v", t)
	}
}

// collectionPlan is what one reconciliation intends to do, decided entirely from a
// read of the live collection BEFORE anything is sent.
type collectionPlan struct {
	// conflicts are the settings that cannot be reached on the live collection.
	// Non-empty means `present` refuses and `recreated` rebuilds.
	conflicts []conflict

	// patch is the UpdateCollection body, empty when the mutable half already
	// matches.
	patch map[string]any

	// addVectors are named vectors the declaration has and the collection does
	// not. Adding one is NOT destructive and has its own endpoint, so it is done
	// in place even by `present`.
	addVectors map[string]any
}

// empty reports a collection already in its declared shape — nothing to send.
func (p collectionPlan) empty() bool {
	return len(p.conflicts) == 0 && len(p.patch) == 0 && len(p.addVectors) == 0
}

// planCollection compares the declaration against the live config and decides what,
// if anything, can be done. It sends nothing and is a pure function, so the whole
// mutable/immutable split is testable without a Qdrant.
//
// live is `result.config` from GET /collections/{name}.
func planCollection(declared map[string]any, live map[string]any) collectionPlan {
	plan := collectionPlan{patch: map[string]any{}, addVectors: map[string]any{}}

	for _, f := range collectionFields {
		want, ok := declared[f.param]
		if !ok {
			continue
		}
		got, present := pathValue(live, f.livePath)
		if !present || got == nil {
			got = f.liveDefault
		}
		if subsetEquals(want, got) {
			continue
		}
		if f.patchPath == nil {
			plan.conflicts = append(plan.conflicts, conflict{
				path: "params." + f.param, live: got, declared: want,
			})
			continue
		}
		setPath(plan.patch, f.patchPath, want)
	}

	planVectors(declared, live, &plan)

	sort.Slice(plan.conflicts, func(i, j int) bool { return plan.conflicts[i].path < plan.conflicts[j].path })
	return plan
}

// planVectors is the per-vector half: the set of named vectors, and the mutability of
// the keys inside each.
//
// Three outcomes, and they are deliberately not the same. A declared vector the
// collection does NOT have is ADDED in place — Qdrant has an endpoint for exactly
// that and it destroys nothing. A vector the collection HAS and the declaration does
// not is a CONFLICT rather than a silent drop, because dropping it destroys that
// vector's data. And a vector present on both sides is compared key by key against
// [vectorFields].
func planVectors(declared map[string]any, live map[string]any, plan *collectionPlan) {
	rawWant, ok := declared["vectors"]
	if !ok {
		return
	}
	want := normalizeVectors(rawWant)
	liveVectors, _ := pathValue(live, []string{"params", "vectors"})
	got := normalizeVectors(liveVectors)

	// The FORM is decided before the per-vector walk. An unnamed single vector and a
	// set of named ones are two addressing models, not two spellings of one: points
	// written under either are unreachable through the other, and no endpoint moves a
	// collection between them. Walking the names first would decompose the change into
	// "add these, drop that" — an ordinary set difference, half of which this object
	// would happily perform in place.
	_, wantUnnamed := want[""]
	_, gotUnnamed := got[""]
	if len(want) > 0 && len(got) > 0 && wantUnnamed != gotUnnamed {
		plan.conflicts = append(plan.conflicts, conflict{
			path: "params.vectors", live: vectorNames(got), declared: vectorNames(want),
		})
		return
	}

	for _, name := range sortedKeys(want) {
		wantVec := want[name]
		gotVec, exists := got[name]
		if !exists {
			// A named vector the collection does not have yet is added in place:
			// Qdrant has an endpoint for exactly that and it destroys nothing — the
			// existing points simply hold no value for it.
			plan.addVectors[name] = wantVec
			continue
		}
		for _, key := range sortedKeys(wantVec) {
			mutable, managed := vectorFields[key]
			if !managed {
				continue
			}
			if subsetEquals(wantVec[key], gotVec[key]) {
				continue
			}
			path := fmt.Sprintf("params.vectors.%s.%s", quoteVectorName(name), key)
			if !mutable {
				plan.conflicts = append(plan.conflicts, conflict{
					path: path, live: gotVec[key], declared: wantVec[key],
				})
				continue
			}
			setPath(plan.patch, []string{"vectors", name, key}, wantVec[key])
		}
	}

	for _, name := range sortedKeys(got) {
		if _, declaredHere := want[name]; declaredHere {
			continue
		}
		plan.conflicts = append(plan.conflicts, conflict{
			path:     fmt.Sprintf("params.vectors.%s", quoteVectorName(name)),
			live:     "present",
			declared: "absent",
		})
	}
}

// quoteVectorName renders the unnamed vector as `""` so a message about it reads as
// an address rather than as a missing word.
func quoteVectorName(name string) string {
	if name == "" {
		return `""`
	}
	return name
}

// vectorNames renders a vector set for a conflict message.
func vectorNames(m map[string]map[string]any) string {
	names := sortedKeys(m)
	for i, n := range names {
		names[i] = quoteVectorName(n)
	}
	return "[" + strings.Join(names, " ") + "]"
}

// normalizeVectors brings both spellings of Qdrant's vectors config to one shape:
// a map from vector name to that vector's parameters, where the UNNAMED vector is the
// empty string.
//
// Qdrant's `vectors` is an untagged union — either one vector's parameters
// (`{size: 4, distance: Cosine}`) or a map of named ones (`{txt: {size: 4, …}}`) —
// and it is told apart by the presence of `size`, which is what Qdrant itself does
// and what it echoes back unchanged. Only the COMPARISON is normalized: a create
// sends the author's own spelling, because the two forms are not interchangeable on
// the wire.
func normalizeVectors(v any) map[string]map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]map[string]any{}
	}
	if _, single := m["size"]; single {
		return map[string]map[string]any{"": m}
	}
	out := make(map[string]map[string]any, len(m))
	for name, spec := range m {
		if inner, ok := spec.(map[string]any); ok {
			out[name] = inner
			continue
		}
		// A named entry that is not a map is malformed input; represent it as an
		// empty spec so the comparison reports the vector as present rather than
		// panicking on the assertion.
		out[name] = map[string]any{}
	}
	return out
}

// subsetEquals reports whether live already carries everything declared says.
//
// For maps it recurses over the DECLARED keys only, which is what makes a partial
// declaration converge against a live config Qdrant filled with defaults. For
// anything else it is plain equality — both sides arrive as float64 / string / bool
// through structpb and encoding/json respectively, so there is no numeric-type
// mismatch to accommodate.
func subsetEquals(declared, live any) bool {
	dm, dIsMap := declared.(map[string]any)
	if !dIsMap {
		return declared == live
	}
	lm, lIsMap := live.(map[string]any)
	if !lIsMap {
		return false
	}
	for k, dv := range dm {
		if !subsetEquals(dv, lm[k]) {
			return false
		}
	}
	return true
}

// pathValue reads a nested value out of a decoded JSON object.
func pathValue(m map[string]any, path []string) (any, bool) {
	cur := any(m)
	for _, key := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = obj[key]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// setPath writes a nested value into a request body, creating the intermediate
// objects.
func setPath(m map[string]any, path []string, v any) {
	cur := m
	for _, key := range path[:len(path)-1] {
		next, ok := cur[key].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[key] = next
		}
		cur = next
	}
	cur[path[len(path)-1]] = v
}

// declaredCollection pulls the managed settings out of params into the same
// representation a decoded live config has, so the two can be compared directly.
// A param the author did not write is absent from the result — not nil — because
// "not declared" and "declared empty" are different instructions.
func declaredCollection(f map[string]*structpb.Value) map[string]any {
	out := map[string]any{}
	params := append([]string{"vectors"}, managedParams()...)
	for _, name := range params {
		v, ok := f[name]
		if !ok || v == nil {
			continue
		}
		if _, isNull := v.GetKind().(*structpb.Value_NullValue); isNull {
			continue
		}
		out[name] = v.AsInterface()
	}
	return out
}

// managedParams is the param names of [collectionFields], in table order.
func managedParams() []string {
	out := make([]string, 0, len(collectionFields))
	for _, f := range collectionFields {
		out = append(out, f.param)
	}
	return out
}

// createBody renders the CreateCollection request from the declaration.
func createBody(declared map[string]any) map[string]any {
	body := map[string]any{}
	if v, ok := declared["vectors"]; ok {
		// Sent in the author's own spelling: the single and named forms are
		// different collections, and normalizing here would turn a declaration of
		// the unnamed vector into a collection with a vector literally named "".
		body["vectors"] = v
	}
	for _, f := range collectionFields {
		if v, ok := declared[f.param]; ok {
			setPath(body, f.createPath, v)
		}
	}
	return body
}

// --- the live half ---

// collectionInfo is the part of GET /collections/{name} this object reads.
type collectionInfo struct {
	Status          string         `json:"status"`
	OptimizerStatus any            `json:"optimizer_status"`
	PointsCount     *int64         `json:"points_count"`
	SegmentsCount   *int64         `json:"segments_count"`
	IndexedVectors  *int64         `json:"indexed_vectors_count"`
	Config          map[string]any `json:"config"`
	PayloadSchema   map[string]any `json:"payload_schema"`
}

// readCollection fetches one collection. A 404 is reported as (nil, false, nil) —
// absent is an answer here, not an error.
func readCollection(ctx context.Context, api qdrantAPI, name, apiKey string) (*collectionInfo, bool, error) {
	res, err := api.do(ctx, http.MethodGet, "/collections/"+url.PathEscape(name), nil)
	if err != nil {
		return nil, false, fmt.Errorf("GET collection %q: %s", name, redactError(err, apiKey))
	}
	if res.notFound() {
		return nil, false, nil
	}
	if !res.ok() {
		return nil, false, fmt.Errorf("GET collection %q: %s", name, redactString(res.errorText(), apiKey))
	}
	var info collectionInfo
	if err := res.decodeResult(&info); err != nil {
		return nil, false, fmt.Errorf("GET collection %q: %s", name, redactError(err, apiKey))
	}
	return &info, true, nil
}

// optimizerStatusText renders `optimizer_status`, which is the string "ok" when
// healthy and an object carrying the error when not.
func optimizerStatusText(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

// countOrZero unwraps a nullable counter for Output. Qdrant omits some of them on a
// collection that is still loading.
func countOrZero(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
