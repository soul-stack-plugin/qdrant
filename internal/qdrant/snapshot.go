// The `snapshot` object's implementation — a collection's backups.
//
// # The honest position on idempotency
//
// Taking a snapshot is an OPERATION, not a state. Qdrant names each one after the
// moment it was taken (`c1-6891264595348006-2026-09-05-07-40-09.snapshot`), the name
// cannot be chosen, and nothing in the API expresses "make sure a backup exists".
// Run `created` twice and you get two files.
//
// `max_age_sec` is what turns it into something a scenario can declare: with it, the
// state is "a snapshot of this collection younger than N seconds exists", which IS
// convergent — a second run inside the window sends nothing and reports
// changed=false. Without it the action is honestly imperative, and the state
// description says so rather than implying a guarantee it cannot make.
//
// One measured quirk behind the door: two creates within the same SECOND return the
// same snapshot name, because the timestamp has second resolution. That is Qdrant
// deduplicating by name, not this module being idempotent, and it is not something to
// rely on.
package qdrant

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// snapshotEntry is one snapshot as Qdrant lists it.
type snapshotEntry struct {
	Name         string `json:"name"`
	CreationTime string `json:"creation_time"`
	Size         int64  `json:"size"`
	Checksum     string `json:"checksum"`
}

// snapshotTimeLayout is Qdrant's creation_time format. It carries no zone and is
// UTC — verified against `date -u` on a live 1.18.3 — so it is parsed as UTC rather
// than as local time. Getting that wrong would make `max_age_sec` wrong by the host's
// UTC offset, which is the kind of error that only shows up in a timezone nobody
// tested in.
const snapshotTimeLayout = "2006-01-02T15:04:05"

// parseSnapshotTime reads a creation_time. An unparseable or empty value yields the
// zero time and false, and the caller treats that as "age unknown".
func parseSnapshotTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(snapshotTimeLayout, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// listSnapshots returns a collection's snapshots, newest first.
//
// A 404 means the COLLECTION is absent, which is reported as such rather than as an
// empty list: "there are no snapshots" and "there is nothing to snapshot" are
// different answers, and quietly creating one in the second case is not possible
// anyway.
func listSnapshots(ctx context.Context, api qdrantAPI, collection, apiKey string) ([]snapshotEntry, bool, error) {
	path := "/collections/" + url.PathEscape(collection) + "/snapshots"
	res, err := api.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, false, fmt.Errorf("list snapshots of %q: %s", collection, redactError(err, apiKey))
	}
	if res.notFound() {
		return nil, false, nil
	}
	if !res.ok() {
		return nil, false, fmt.Errorf("list snapshots of %q: %s", collection, redactString(res.errorText(), apiKey))
	}
	var out []snapshotEntry
	if err := res.decodeResult(&out); err != nil {
		return nil, false, fmt.Errorf("list snapshots of %q: %s", collection, redactError(err, apiKey))
	}
	sortSnapshots(out)
	return out, true, nil
}

// sortSnapshots orders newest first, falling back to the name so the order is total
// and the output is deterministic even when two snapshots share a second.
func sortSnapshots(in []snapshotEntry) {
	sort.Slice(in, func(i, j int) bool {
		ti, iok := parseSnapshotTime(in[i].CreationTime)
		tj, jok := parseSnapshotTime(in[j].CreationTime)
		if iok && jok && !ti.Equal(tj) {
			return ti.After(tj)
		}
		return in[i].Name > in[j].Name
	})
}

// newestSnapshotAge returns the age of the most recent snapshot whose timestamp could
// be read. The second result is false when there is none to measure.
func newestSnapshotAge(list []snapshotEntry, now time.Time) (time.Duration, snapshotEntry, bool) {
	for _, s := range list {
		t, ok := parseSnapshotTime(s.CreationTime)
		if !ok {
			continue
		}
		age := now.Sub(t)
		if age < 0 {
			// The instance's clock is ahead of ours. Reading that as a negative age
			// would make any max_age_sec window match, so it is clamped to zero:
			// "just taken" is the truthful reading of a snapshot from the future.
			age = 0
		}
		return age, s, true
	}
	return 0, snapshotEntry{}, false
}

// createSnapshot takes one. wait=true so the call returns after the file exists —
// without it the read-back could race the snapshot into existence.
func createSnapshot(ctx context.Context, api qdrantAPI, collection, apiKey string) (snapshotEntry, error) {
	path := "/collections/" + url.PathEscape(collection) + "/snapshots?wait=true"
	res, err := api.do(ctx, http.MethodPost, path, nil)
	if err != nil {
		return snapshotEntry{}, fmt.Errorf("create snapshot of %q: %s", collection, redactError(err, apiKey))
	}
	if !res.ok() {
		return snapshotEntry{}, fmt.Errorf("create snapshot of %q: %s", collection, redactString(res.errorText(), apiKey))
	}
	var out snapshotEntry
	if err := res.decodeResult(&out); err != nil {
		return snapshotEntry{}, fmt.Errorf("create snapshot of %q: %s", collection, redactError(err, apiKey))
	}
	return out, nil
}

// deleteSnapshot removes one by name.
func deleteSnapshot(ctx context.Context, api qdrantAPI, collection, name, apiKey string) error {
	path := "/collections/" + url.PathEscape(collection) + "/snapshots/" + url.PathEscape(name) + "?wait=true"
	res, err := api.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return fmt.Errorf("delete snapshot %q of %q: %s", name, collection, redactError(err, apiKey))
	}
	if !res.ok() {
		return fmt.Errorf("delete snapshot %q of %q: %s", name, collection, redactString(res.errorText(), apiKey))
	}
	return nil
}

// hasSnapshot reports whether a named snapshot is in the list.
func hasSnapshot(list []snapshotEntry, name string) bool {
	for _, s := range list {
		if s.Name == name {
			return true
		}
	}
	return false
}
