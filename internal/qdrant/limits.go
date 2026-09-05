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
)

// bound is one numeric constraint. Absent min/max mean unbounded on that side.
type bound struct {
	min     *float64
	max     *float64
	integer bool
}

func atLeast(v float64) bound       { return bound{min: &v, integer: true} }
func between(lo, hi float64) bound  { return bound{min: &lo, max: &hi, integer: true} }
func fraction(lo, hi float64) bound { return bound{min: &lo, max: &hi} }
func atLeastAny(v float64) bound    { return bound{min: &v} }
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

// topLevelBounds are the scalar params of a collection.
var topLevelBounds = map[string]bound{
	"shard_number":             atLeast(1),
	"replication_factor":       atLeast(1),
	"write_consistency_factor": atLeast(1),
}

// vectorBounds are the keys inside one vector's parameters.
var vectorBounds = map[string]bound{
	"size": between(1, 65536),
}

// nestedBounds are the keys inside the passthrough config maps. Qdrant checks these
// too, and a value below the floor is a 400 on the create — which after a drop is a
// destroyed collection.
var nestedBounds = map[string]map[string]bound{
	"hnsw_config": {
		"m":                    atLeast(0),
		"ef_construct":         atLeast(4),
		"full_scan_threshold":  atLeast(10),
		"max_indexing_threads": atLeast(0),
		"payload_m":            atLeast(0),
	},
	"optimizers_config": {
		"deleted_threshold":        fraction(0, 1),
		"vacuum_min_vector_number": atLeast(100),
		"default_segment_number":   atLeast(0),
		"max_segment_size":         atLeast(1),
		"memmap_threshold":         atLeast(0),
		"indexing_threshold":       atLeast(0),
		"flush_interval_sec":       atLeastAny(0),
	},
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
