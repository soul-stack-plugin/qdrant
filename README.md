# qdrant — a Soul Stack plugin for Qdrant

A [SoulModule](https://github.com/souls-guild/soul-stack) plugin that manages a live
[Qdrant](https://qdrant.tech) vector database: collections and their shape, aliases,
payload indexes, snapshots, and the instance's own health.

It speaks Qdrant's REST API over `net/http` and nothing else — no `qdrant` CLI, no
shell, no third-party Qdrant client. The only dependency outside the standard library
is the Soul Stack SDK, so the artifact an operator approves by sha256 pulls in no
driver tree of its own.

## Address

An operator registers this artifact under an alias, and that alias is address level 1:

```
<alias>.<object>.<action>
```

Registered as `qdrant`, the states below are `qdrant.collection.present`,
`qdrant.instance.pinged` and so on. The artifact itself carries no name — the same
bytes registered twice serve two address spaces.

## Objects

| object | states | what it manages |
|---|---|---|
| `instance` | `pinged` · `ready-probed` · `version-probed` | the server: liveness, readiness, version. Read-only, `changed=false` by design |
| `collection` | `probed` · `present` · `absent` | one collection and its shape |
| `alias` | `present` · `absent` | a second name for a collection — what a blue-green swap moves |
| `index` | `present` · `absent` | one payload field index |
| `snapshot` | `created` · `absent` | a collection's backups |

Every state runs on the Soul side (`side: soul`) and declares one capability,
`network_outbound`.

## ★ The thing to know before using `collection`

**Qdrant accepts an update it cannot perform, answers `200 {"result":true}`, and
discards it.** Measured against a live 1.18.3 on 2026-09-05, on a collection created
with `size: 4` / `Cosine` / 2 shards:

| request | answer | what actually happened |
|---|---|---|
| `PATCH {"vectors":{"":{"size":8}}}` | `200 {"result":true}` | size stayed **4** |
| `PATCH {"vectors":{"":{"distance":"Euclid"}}}` | `200 {"result":true}` | distance stayed **Cosine** |
| `PATCH {"params":{"shard_number":4}}` | `200 {"result":true}` | shards stayed **2** |
| `PATCH {"bogus_top":1}` | `200 {"result":true}` | nothing |
| `PATCH {"params":{"replication_factor":"two"}}` | `400` | — |

Unknown field names are dropped in silence; only a wrong JSON *type* on a known field
is refused. So the response is not evidence, and a plugin that trusted it would report
a reconciled collection on every run, forever, while the collection never moved.

Two rules follow, and they run through every object here:

- **`changed` comes from reading the resource before and after, never from the
  response.** Every write is verified by a read-back, and one that did not take fails
  the step instead of being reported as done.
- **A setting Qdrant can only reach by recreating the collection is refused before the
  first mutating request**, naming the field, its live value and the declared one.

### What can and cannot be changed on a live collection

Derived from the difference between Qdrant's `CreateCollection` and `UpdateCollection`
request bodies, and encoded once in `collectionFields` / `vectorFields`
(`internal/qdrant/collection.go`).

**Reconciled in place:** `replication_factor`, `write_consistency_factor`,
`on_disk_payload`, `hnsw_config`, `optimizers_config`, `quantization_config`, and per
vector `hnsw_config` / `quantization_config` / `on_disk`.

**Creation-only — reaching it destroys the collection's contents:** a vector's `size`,
`distance`, `datatype` and `multivector_config`; `shard_number`; `sharding_method`;
`wal_config`.

**Named vectors are a special case.** Adding one is done in place — Qdrant has an
endpoint for it and it destroys nothing. Dropping one is refused, because it destroys
that vector's data.

A vector's `memory` is deliberately **not** managed, although Qdrant's schema lists it:
a live 1.18.3 accepts it and never reports it back, and this module decides `changed` by
reading the resource back — a key it cannot read is one it cannot honestly reconcile.
The same goes for `hnsw_config.memory`.

### There is no state that rebuilds a collection

`collection.present` refuses on the creation-only half and stops there. v1 ships nothing
that drops and recreates, and that is a decision rather than an omission: four
independent reviews of this module each found a fresh way for a drop-then-create to
destroy data it could not then restore — a bound taken from the wrong schema, a value
that panics the server rather than returning an error, a declared default that read as
drift, and a name occupied by an alias, where Qdrant resolves the read but not the
delete. Each fix closed one route and the next review found another.

Reaching a creation-only change therefore stays an operator's deliberate act:

```
qdrant.snapshot.created   →   qdrant.collection.absent   →   qdrant.collection.present
```

which is three declarations that each say what they do, instead of one that hides a
deletion inside a reconciliation.

`index` is the deliberate contrast: it rebuilds a payload index in place without asking,
because that destroys an *index*, not data. What it costs is a window in which filters
on that field fall back to a full scan.

### Comparison is by declared keys only

Qdrant reports every nested config fully populated with its defaults, and a nested
`PATCH` merges rather than replaces. So this module compares only the keys a
declaration mentions — otherwise every partial declaration, which is all of them, would
drift on every run. The cost: it cannot express "put this back to the default".

## Example

```yaml
- name: the documents collection exists with the right shape
  module: qdrant.collection.present
  params:
    addr: "qdrant-1:6333"
    api_key: "${ vault('secret/qdrant', 'api_key') }"
    tls: true
    tls_ca: "${ vault('secret/qdrant', 'ca') }"
    name: documents
    vectors:
      size: 768
      distance: Cosine
    shard_number: 2
    replication_factor: 2
    hnsw_config:
      m: 32

- name: tenant lookups are indexed
  module: qdrant.index.present
  params:
    addr: "qdrant-1:6333"
    api_key: "${ vault('secret/qdrant', 'api_key') }"
    collection: documents
    field: tenant
    schema: keyword

- name: a backup exists, at most a day old
  module: qdrant.snapshot.created
  params:
    addr: "qdrant-1:6333"
    collection: documents
    max_age_sec: 86400
```

## Secrets

`api_key` is declared `secret: true` and travels in the `api-key` **request header**.
Never in a URL — Qdrant answers 401 to a key in the query string, so no URL form of the
credential exists to leak into a log line or a proxy access log — and never in a command
argument, because nothing here spawns a process for `ps` to read. The TLS PEMs
(`tls_ca`, `tls_cert`, `tls_key`) are secret on the same terms. Transport errors are
scrubbed before they become a message.

A parameter of the wrong type is **refused, not coerced**, in `Apply` as well as
`Validate`: the runtime calls `Apply`, and `tls: "true"` written as a string must not
read as `false` and send the credential in plaintext.

## No user or key management

Qdrant 1.18.3 has no user, key, role or token endpoint at all. Authentication is the
static `api-key` from the server's own configuration file, or a JWT the client mints and
signs with it. There is nothing to manage server-side, so this plugin has no `user`
object — that is a fact about Qdrant, not a gap here.

## Not in this version

Distributed shard operations (moving shards, replicas, sharding keys, resharding),
restoring a collection *from* a snapshot, sparse vectors, `strict_mode_config`, and
Qdrant's object form of a payload index schema (a text index with a tokenizer). The
last one is excluded on purpose: the read-back reports only `data_type`, so this module
could not tell one tokenizer from another and would report converged on an index that
is not.

## Building

```sh
make check                          # gofmt, vet, offline tests, build, stamp, verify
make live QDRANT_ADDR=127.0.0.1:6333    # against a real Qdrant
make verify                         # the stamped schema still matches the code
```

Everything runs from a fresh clone with nothing installed: `soul-mod` is run out of
the SDK this module already depends on. Note the Makefile invokes it **without** an
`@version` suffix and do not add one — the versioned form resolves the tool's module
independently of this one, and the published SDK still carries a `replace` that Go
refuses to run a tool through.

This repository commits **no** `schema.json`. The document is derived from the Go
values in `internal/qdrant`, stamped into the binary by `make build` and written beside
it; `make verify` is what proves the stamp still matches the code, and the release
workflow publishes the document as an asset. A committed copy would be one more thing
that can go stale between an edit and a re-stamp.

The offline suite covers the whole domain model — which settings are reconcilable,
which are refused, and that a refusal sends nothing. The live suite additionally
asserts the *premise* against a real server: that Qdrant still answers 200 to an
immutable-field patch and changes nothing. If a future Qdrant starts rejecting those
properly, that test is what will say so; CI runs it against three Qdrant versions.

## Layout

```
cmd/qdrant/          the entry point: one call to module.ServeBundle
internal/qdrant/     the five objects, one REST driver, and the tests
```

## Delivery

Published as a binary artifact and registered in `keeper.yml`:

```yaml
plugins:
  soul_modules:
    - name: qdrant
      kind: artifact
      base_url: https://github.com/soul-stack-plugin/qdrant/releases/download
      ref: v0.1.0
      artifacts:
        - { os: linux, arch: amd64, path: qdrant-linux-amd64, sha256: "..." }
```

Soul fetches it from the source and checks the sha256 against the signed grant.

## Licence

Apache 2.0 — see [LICENSE](LICENSE).
