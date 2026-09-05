// The `snapshot` object — a collection's backups: take one, remove one.
//
// The design decision worth knowing before reading the states is in snapshot.go:
// creating a snapshot is inherently an operation rather than a state, and
// `max_age_sec` is what makes it declarable. Restoring FROM a snapshot is deliberately
// not here — it overwrites a collection, it cannot be made idempotent in any
// meaningful sense, and it deserves its own ticket rather than a corner of this one.
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/protobuf/types/known/structpb"
)

// snapshot binds the object's actions to the shared driver.
func (m *QdrantModule) snapshot() *object {
	return &object{
		impl: m,
		name: "snapshot",
		decl: snapshotStates(),
		actions: map[string]action{
			"created": {validate: validateSnapshotCreated, apply: (*QdrantModule).applySnapshotCreated},
			"absent":  {validate: validateSnapshotAbsent, apply: (*QdrantModule).applySnapshotAbsent},
		},
	}
}

// snapshotDef is the object's entry in the artifact's bundle.
func snapshotDef(m *QdrantModule) module.Def {
	return module.Def{
		Name:         "snapshot",
		Description:  "A snapshot of one Qdrant collection: taken, or removed by name.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "qdrant"}},
		Impl:         m.snapshot(),
		States:       snapshotStates(),
	}
}

// --- validation ---

func validateSnapshotCreated(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	errs = append(errs, validateName(f, "collection")...)
	if v := f["max_age_sec"]; v != nil && intOrDefault(v, 0) < 0 {
		errs = append(errs, "params.max_age_sec: must be an integer >= 0 (seconds); 0 or omitted means always take a snapshot")
	}
	return errs
}

func validateSnapshotAbsent(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	errs = append(errs, validateName(f, "collection")...)
	return append(errs, validateName(f, "name")...)
}

// --- actions ---

// applySnapshotCreated takes a snapshot, unless max_age_sec says a recent enough one
// already exists.
func (m *QdrantModule) applySnapshotCreated(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	collection := strings.TrimSpace(stringOrEmpty(f["collection"]))
	maxAge := intOrDefault(f["max_age_sec"], 0)

	list, exists, err := listSnapshots(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		// Not the same as "no snapshots": there is nothing to take one of, and
		// silently succeeding would let a backup step pass on a collection that was
		// never created.
		return sendFailure(stream, fmt.Sprintf("collection %q does not exist — there is nothing to snapshot", collection))
	}

	if maxAge > 0 {
		if age, newest, ok := newestSnapshotAge(list, m.nowUTC()); ok && age <= time.Duration(maxAge)*time.Second {
			return sendOutcome(stream, false, fmt.Sprintf(
				"snapshot %q of %q is %s old, within max_age_sec=%d — none taken",
				newest.Name, collection, age.Round(time.Second), maxAge),
				snapshotOutput(newest, false, len(list)))
		}
	}

	entry, err := createSnapshot(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}

	// Read back: the create call answers with the entry it made, and this artifact
	// does not take Qdrant's word for a write anywhere else either.
	after, _, err := listSnapshots(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !hasSnapshot(after, entry.Name) {
		return sendFailure(stream, fmt.Sprintf("snapshot %q of %q was reported created but is not in the collection's snapshot list", entry.Name, collection))
	}
	return sendOutcome(stream, true, fmt.Sprintf("snapshot %q of %q taken", entry.Name, collection),
		snapshotOutput(entry, true, len(after)))
}

// applySnapshotAbsent removes one snapshot by name. Idempotent: one that is not there
// sends no DELETE, which also avoids the 404 Qdrant answers to a delete of a snapshot
// it does not have (unlike an alias, where the same call is a quiet 200).
func (m *QdrantModule) applySnapshotAbsent(ctx context.Context, stream eventStream, api qdrantAPI, params *structpb.Struct) error {
	f := params.GetFields()
	apiKey := stringOrEmpty(f["api_key"])
	collection := strings.TrimSpace(stringOrEmpty(f["collection"]))
	name := strings.TrimSpace(stringOrEmpty(f["name"]))

	list, exists, err := listSnapshots(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if !exists {
		// No collection means no snapshot of it. `absent` is satisfied, and saying
		// so beats failing on a cleanup step that runs after the collection is gone.
		return sendOutcome(stream, false, fmt.Sprintf("collection %q does not exist, so snapshot %q is absent", collection, name),
			map[string]any{"name": name, "exists": false, "count": int64(0)})
	}
	if !hasSnapshot(list, name) {
		return sendOutcome(stream, false, fmt.Sprintf("snapshot %q of %q is already absent", name, collection),
			map[string]any{"name": name, "exists": false, "count": int64(len(list))})
	}

	if err := deleteSnapshot(ctx, api, collection, name, apiKey); err != nil {
		return sendFailure(stream, err.Error())
	}
	after, _, err := listSnapshots(ctx, api, collection, apiKey)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if hasSnapshot(after, name) {
		return sendFailure(stream, fmt.Sprintf("snapshot %q of %q still exists after DELETE", name, collection))
	}
	return sendOutcome(stream, true, fmt.Sprintf("snapshot %q of %q removed", name, collection),
		map[string]any{"name": name, "exists": false, "count": int64(len(after))})
}

// snapshotOutput is the reading a scenario carries forward — the name above all, since
// it is the only handle on a snapshot and Qdrant chooses it.
func snapshotOutput(entry snapshotEntry, created bool, count int) map[string]any {
	return map[string]any{
		"name":          entry.Name,
		"created":       created,
		"exists":        true,
		"creation_time": entry.CreationTime,
		"size":          entry.Size,
		"checksum":      entry.Checksum,
		"count":         int64(count),
	}
}

// --- declaration ---

func snapshotStates() map[string]module.State {
	return map[string]module.State{
		"created": {
			Description: "Take a snapshot of ONE collection over the REST API (no qdrant CLI, no\n" +
				"shell). Output.name is the snapshot's name, which is the only handle on it\n" +
				"and which QDRANT chooses — it is built from a timestamp and cannot be set.\n" +
				"\n" +
				"★ Read this before relying on it as a state. Taking a snapshot is an\n" +
				"OPERATION: with max_age_sec omitted, every run takes another one and reports\n" +
				"changed=true, because nothing in the Qdrant API expresses \"make sure a\n" +
				"backup exists\".\n" +
				"\n" +
				"max_age_sec is what makes it declarable: with it the state reads \"a snapshot\n" +
				"of this collection younger than N seconds exists\", and a run inside that\n" +
				"window sends nothing and reports changed=false. Use it whenever the step is\n" +
				"in a scenario that runs more than once.\n" +
				"\n" +
				"A collection that does not exist FAILS rather than passing quietly — a\n" +
				"backup step that succeeds on a missing collection is worse than one that\n" +
				"stops. The snapshot is verified by listing the collection's snapshots\n" +
				"afterwards. No dry-run preview.",
			Input: withConnect(module.Input{
				"collection": collectionParam("Collection to snapshot. Addresses a URL path segment, so it carries no whitespace, slash, \"?\" or \"#\"."),
				"max_age_sec": {Type: module.Int,
					Description: "Take a snapshot only if the newest existing one is OLDER than this many seconds; otherwise do nothing and report changed=false. Omitted or 0 means always take one, which makes this step an operation rather than a state. The comparison uses Qdrant's own creation_time, which is UTC.",
				},
			}),
		},
		"absent": {
			Description: "Remove ONE snapshot of a collection by name (no qdrant CLI, no shell).\n" +
				"\n" +
				"Idempotent: a snapshot that is not there is a no-op that sends no DELETE\n" +
				"(changed=false). It does LIST first, and that is not only for the changed\n" +
				"flag — Qdrant answers 404 to a delete of a snapshot it does not have, so a\n" +
				"blind DELETE would fail the second run of the very same step.\n" +
				"\n" +
				"A collection that does not exist also reads as absent rather than failing: a\n" +
				"cleanup step that runs after the collection is gone has got what it asked\n" +
				"for. The removal is verified by listing afterwards. No dry-run preview.",
			Input: withConnect(module.Input{
				"collection": collectionParam("Collection the snapshot belongs to. Addresses a URL path segment, so it carries no whitespace, slash, \"?\" or \"#\"."),
				"name": {Type: module.String, Required: true,
					Description: "Snapshot name, as Qdrant reports it (e.g. \"c1-6891264595348006-2026-09-05-07-40-09.snapshot\") — typically carried over from a previous qdrant.snapshot.created step through register. Addresses a URL path segment, so it carries no whitespace, slash, \"?\" or \"#\".",
				},
			}),
		},
	}
}
