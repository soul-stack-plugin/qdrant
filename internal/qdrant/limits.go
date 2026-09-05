// The numeric bounds Qdrant enforces on a create body, checked here BEFORE anything
// is sent.
//
// # Why this file exists, and why it is not merely tidy
//
// `collection.recreated` DROPS the collection and then creates it again. Those are two
// requests, and nothing rolls the first one back. If the create is refused — a
// `shard_number: 0`, a vector `size` above Qdrant's ceiling, an `ef_construct` below
// its floor — the collection and every point in it are already gone, and the run ends
// with a 400 and nothing to rebuild from.
//
// That is the one place where this artifact's own rule (refuse before the first
// mutating request) can be violated by a value that Validate let through. Qdrant has no
// dry-run create, so the only defence is to check what Qdrant documents it will refuse,
// and to check it in Validate AND before the drop.
//
// The bounds below are Qdrant's own, read out of its OpenAPI schema (`minimum` /
// `maximum` on CreateCollection, CollectionParams, VectorParams, HnswConfigDiff and
// OptimizersConfigDiff) and confirmed against 1.18.3. They are deliberately NOT
// invented: a bound this module made up would reject a declaration a future Qdrant
// accepts, which is the opposite failure and just as annoying.
package qdrant

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// bound is one numeric constraint. Absent min/max mean unbounded on that side.
type bound struct {
	min     *float64
	max     *float64
	integer bool
}

func between(lo, hi float64) bound  { return bound{min: &lo, max: &hi, integer: true} }
func fraction(lo, hi float64) bound { return bound{min: &lo, max: &hi} }
func describe(b bound, got float64) string {
	switch {
	case b.min != nil && b.max != nil:
		return fmt.Sprintf("must be between %s and %s, got %s", renderValue(*b.min), renderValue(*b.max), renderValue(got))
	case b.min != nil:
		return fmt.Sprintf("must be at least %s, got %s", renderValue(*b.min), renderValue(got))
	case b.max != nil:
		return fmt.Sprintf("must be at most %s, got %s", renderValue(*b.max), renderValue(got))
	}
	return ""
}

// maxUint32 / maxUint64Safe are the ceilings Qdrant's own integer formats impose.
// Without them a value that merely looks large is a 400 "expected u32" — which after
// `recreated` has already dropped the collection is a destroyed collection.
//
// maxUint64Safe is 2^53, not 2^64: every number arrives here as a float64 through both
// structpb and encoding/json, so anything above that has already lost precision and no
// bound stated in float64 could be honest about it.
const (
	maxUint32     = 4294967295
	maxUint64Safe = 9007199254740992
)

// topLevelBounds are the scalar params of a collection. All three are uint32 in
// Qdrant's schema, so both ends are real.
var topLevelBounds = map[string]bound{
	"shard_number":             between(1, maxUint32),
	"replication_factor":       between(1, maxUint32),
	"write_consistency_factor": between(1, maxUint32),
}

// vectorBounds are the keys inside one vector's parameters.
var vectorBounds = map[string]bound{
	"size": between(1, 65536),
}

// nestedBounds are the keys inside the passthrough config maps.
//
// wal_config is here for the reason this whole file exists rather than for symmetry:
// it is one of the settings Qdrant CANNOT change on a live collection, so a difference
// in it is exactly what sends `recreated` to the DELETE. A bad value there is the
// shortest path from a typo to a destroyed collection —
// `wal_config: {wal_capacity_mb: 0}` answers 422 "must be 1 or larger", measured, and
// that answer arrives after the drop.
var nestedBounds = map[string]map[string]bound{
	"hnsw_config": {
		"m":                    between(0, maxUint64Safe),
		"ef_construct":         between(4, maxUint64Safe),
		"full_scan_threshold":  between(10, maxUint64Safe),
		"max_indexing_threads": between(0, maxUint64Safe),
		"payload_m":            between(0, maxUint64Safe),
	},
	"optimizers_config": {
		"deleted_threshold":        fraction(0, 1),
		"vacuum_min_vector_number": between(100, maxUint64Safe),
		"default_segment_number":   between(0, maxUint64Safe),
		"max_segment_size":         between(1, maxUint64Safe),
		"memmap_threshold":         between(0, maxUint64Safe),
		"indexing_threshold":       between(0, maxUint64Safe),
		// u64 in Qdrant's schema: a fractional value is a 400, not a rounded one.
		"flush_interval_sec": between(0, maxUint64Safe),
	},
	"wal_config": {
		"wal_capacity_mb":    between(1, maxUint32),
		"wal_segments_ahead": between(0, maxUint32),
		// 1, not 0: the floor above comes from Qdrant's UPDATE schema (WalConfigDiff,
		// minimum 0), and the COLLECTION schema is stricter (WalConfig, minimum 1,
		// default 1). Zero is not even a clean rejection — measured on 1.18.3 it
		// panics the server into a 500.
		"wal_retain_closed": between(1, maxUint32),
	},
}

// passthroughKeys is what each managed config map may contain, from Qdrant's own
// schema (HnswConfigDiff, OptimizersConfigDiff, WalConfigDiff).
//
// The reason to check these is not tidiness, it is that Qdrant DISCARDS a key it does
// not recognise and still answers 200. Inside a mutable map that costs a failed
// read-back and a clear message. Inside a CREATION-ONLY map — wal_config — it is far
// worse: the key can never appear in the live config, so it is permanent drift, and
// permanent drift on a creation-only setting means `recreated` drops and rebuilds the
// collection on EVERY run. A typo turns a declared state into a scheduled deletion.
var passthroughKeys = map[string]map[string]bool{
	"hnsw_config": {
		"m": true, "ef_construct": true, "full_scan_threshold": true,
		"max_indexing_threads": true, "on_disk": true, "payload_m": true,
		"inline_storage": true,
	},
	"optimizers_config": {
		"deleted_threshold": true, "vacuum_min_vector_number": true,
		"default_segment_number": true, "max_segment_size": true,
		"memmap_threshold": true, "indexing_threshold": true,
		"flush_interval_sec": true, "max_optimization_threads": true,
		"prevent_unoptimized": true,
	},
	"wal_config": {
		"wal_capacity_mb": true, "wal_segments_ahead": true, "wal_retain_closed": true,
	},
	// Per-vector and CREATION-ONLY: an unknown key here is permanent drift, and
	// permanent drift on a creation-only setting is a rebuild on every run.
	"multivector_config": {
		"comparator": true,
	},
	// The outer arm only. The inside of each is a union this module does not model,
	// and it is mutable — an unknown key there costs a failing read-back, not data.
	"quantization_config": {
		"scalar": true, "product": true, "binary": true,
	},
}

// checkPassthroughKeys reports every key inside a managed config map that Qdrant does
// not know. Deterministic order.
func checkPassthroughKeys(declared map[string]any) []string {
	errs := checkMapKeys(declared, "params")

	// And INSIDE each vector, which is where the worst instance lives:
	// `multivector_config` is creation-only, so an unknown key in it is permanent
	// drift, and permanent drift on a creation-only setting makes `recreated` drop
	// and rebuild on every single run.
	if raw, ok := declared["vectors"]; ok {
		vectors := normalizeVectors(raw)
		for _, name := range sortedKeys(vectors) {
			errs = append(errs, checkMapKeys(vectors[name], "params.vectors."+quoteVectorName(name))...)
		}
	}
	return errs
}

// checkMapKeys applies the table to one level: every managed config map directly
// inside `holder`, addressed under prefix.
func checkMapKeys(holder map[string]any, prefix string) []string {
	var errs []string
	for _, param := range sortedKeys(passthroughKeys) {
		nested, ok := holder[param].(map[string]any)
		if !ok {
			continue
		}
		for _, key := range sortedKeys(nested) {
			if passthroughKeys[param][key] {
				continue
			}
			errs = append(errs, fmt.Sprintf(
				"%s.%s.%s: not a setting Qdrant knows (expected %s) — it would be discarded in silence and still answered 200, so it is refused here instead",
				prefix, param, key, strings.Join(sortedKeys(passthroughKeys[param]), ", ")))
		}
	}
	return errs
}

// checkCollectionBounds reports every declared value outside the range Qdrant accepts,
// addressed as the operator wrote it. Deterministic order.
//
// It reads the SAME declared map [planCollection] reads, so a param added to
// [collectionFields] without a bound here is simply unchecked rather than mis-checked.
func checkCollectionBounds(declared map[string]any) []string {
	var errs []string

	for _, param := range sortedKeys(topLevelBounds) {
		v, ok := declared[param]
		if !ok {
			continue
		}
		errs = append(errs, checkOne("params."+param, v, topLevelBounds[param])...)
	}

	for _, param := range sortedKeys(nestedBounds) {
		nested, ok := declared[param].(map[string]any)
		if !ok {
			continue
		}
		for _, key := range sortedKeys(nestedBounds[param]) {
			v, present := nested[key]
			if !present {
				continue
			}
			errs = append(errs, checkOne(fmt.Sprintf("params.%s.%s", param, key), v, nestedBounds[param][key])...)
		}
	}

	if raw, ok := declared["vectors"]; ok {
		vectors := normalizeVectors(raw)
		names := make([]string, 0, len(vectors))
		for name := range vectors {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			for _, key := range sortedKeys(vectorBounds) {
				v, present := vectors[name][key]
				if !present {
					continue
				}
				addr := fmt.Sprintf("params.vectors.%s.%s", quoteVectorName(name), key)
				errs = append(errs, checkOne(addr, v, vectorBounds[key])...)
			}
			// A vector's own nested maps carry the same floors as the
			// collection-level ones.
			for _, param := range sortedKeys(nestedBounds) {
				nested, ok := vectors[name][param].(map[string]any)
				if !ok {
					continue
				}
				for _, key := range sortedKeys(nestedBounds[param]) {
					v, present := nested[key]
					if !present {
						continue
					}
					addr := fmt.Sprintf("params.vectors.%s.%s.%s", quoteVectorName(name), param, key)
					errs = append(errs, checkOne(addr, v, nestedBounds[param][key])...)
				}
			}
		}
	}

	return errs
}

// checkOne applies one bound. A non-number is left alone: the declared TYPE is
// [checkParamTypes]'s business, and reporting it twice in two vocabularies helps
// nobody.
func checkOne(addr string, v any, b bound) []string {
	n, ok := v.(float64)
	if !ok {
		return nil
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return []string{addr + ": must be a finite number"}
	}
	if b.integer && n != math.Trunc(n) {
		return []string{fmt.Sprintf("%s: must be a whole number, got %s", addr, renderValue(n))}
	}
	if b.min != nil && n < *b.min {
		return []string{addr + ": " + describe(b, n)}
	}
	if b.max != nil && n > *b.max {
		return []string{addr + ": " + describe(b, n)}
	}
	return nil
}
