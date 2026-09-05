// Guard on the schema document ↔ implementation contract (NIM-206, NIM-525, NIM-766).
//
// `modules[<object>].states.<action>.input` is the ONLY thing param-level strictness
// reads (ADR-0076, NIM-163): a key a state omits has no declaration, so a legitimate
// call carrying it FAILS with module.unknown_param. A prose promise in a comment is
// not a declaration.
//
// The document is GENERATED from the Go value (`qdrantBundle`), not written beside it:
// `soul-mod stamp` runs the artifact's `schema` subcommand and writes those bytes both
// into the binary and to `schema.json`. TestPublishedSchemaMatchesTheBundle is the
// local half of that guard; `make check-plugin-schema` is the other half.
//
// The halves are checked together on purpose. TestConnectParamsAreRead proves the key
// list below is what the Go parse path actually reads (a rename in conn.go/tls.go
// breaks it); TestManifestStatesDeclareWhatTheyAccept proves every state declares
// exactly those plus its own; and TestDeclaredStatesAreDispatched proves the object
// that SERVES a state is the one that DECLARES it.
package main

import (
	"bytes"
	"os"
	"sort"
	"testing"

	"github.com/souls-guild/soul-stack/sdk/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

// connectParams — read by parseConnConfig (which calls parseTLS) for EVERY state of
// every object, because every action of this artifact addresses one instance.
//
// Note what is NOT here, and is not an oversight: there is no `username`/`password`
// pair. Qdrant has no server-side principals at all — no user, role or key endpoint
// exists in its API (checked against 1.18.3) — and authentication is the single
// `api-key` header, statically configured on the server. There is nothing for this
// artifact to manage and nothing else to send.
var connectParams = []string{
	"addr", "api_key", "timeout_sec",
	"tls", "tls_ca", "tls_cert", "tls_key", "tls_skip_verify",
}

// secretParams — params carrying a credential or PEM. Declaring one without
// `secret: true` would leave it unmasked in logs/traces/UI (ADR-010).
var secretParams = map[string]bool{
	"api_key": true,
	"tls_ca":  true, "tls_cert": true, "tls_key": true,
}

// objects — the five objects this artifact serves, paired with their dispatch tables.
// Address level 2 in `qdrant.<object>.<action>`.
func objects(m *QdrantModule) map[string]*object {
	return map[string]*object{
		"alias":      m.alias(),
		"collection": m.collection(),
		"index":      m.index(),
		"instance":   m.instance(),
		"snapshot":   m.snapshot(),
	}
}

// loadDocument reads the PUBLISHED schema document and runs the SDK validator: a
// document keeper would reject at `plugin.allow` must not pass here either.
func loadDocument(t *testing.T) schema.Document {
	t.Helper()
	raw, err := os.ReadFile(schema.SchemaFileName)
	if err != nil {
		t.Fatalf("read %s: %v", schema.SchemaFileName, err)
	}
	doc, err := schema.Unmarshal(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", schema.SchemaFileName, err)
	}
	for _, i := range schema.Validate(doc) {
		if i.Level == schema.LevelError {
			t.Errorf("%s is invalid: %s at %s", schema.SchemaFileName, i, i.Path)
		}
	}
	// The bytes are canonical (sorted keys, no insignificant whitespace) because they
	// are hashed and signed — a hand edit that reformats them is a finding.
	canonical, err := schema.IsCanonical(raw)
	if err != nil || !canonical {
		t.Errorf("%s is not canonical (%v) — regenerate it, do not hand-edit", schema.SchemaFileName, err)
	}
	return doc
}

func loadModule(t *testing.T, name string) schema.Module {
	t.Helper()
	doc := loadDocument(t)
	mod, ok := doc.Module(name)
	if !ok {
		t.Fatalf("%s declares no module %q, only %v", schema.SchemaFileName, name, doc.ModuleNames())
	}
	if len(mod.States) == 0 {
		t.Fatalf("module %q declares no states", name)
	}
	return mod
}

// TestPublishedSchemaMatchesTheBundle — `schema.json` is what `qdrantBundle` renders,
// byte for byte. The moment someone edits a Def without re-stamping, everything
// downstream — soul-lint, `plugin.allow`, the module form — is reading a contract the
// binary no longer implements (NIM-525).
func TestPublishedSchemaMatchesTheBundle(t *testing.T) {
	fromCode, err := qdrantBundle(&QdrantModule{}).Schema()
	if err != nil {
		t.Fatalf("render the bundle: %v", err)
	}
	published, err := os.ReadFile(schema.SchemaFileName)
	if err != nil {
		t.Fatalf("read %s: %v", schema.SchemaFileName, err)
	}
	if !bytes.Equal(published, fromCode) {
		t.Fatalf("%s disagrees with the bundle — rebuild and re-stamp:\n"+
			"  (cd examples/module/qdrant && GOWORK=off go build -o dist/qdrant . )\n"+
			"  go run ./sdk/cmd/soul-mod stamp examples/module/qdrant/dist/qdrant\n"+
			"  cp examples/module/qdrant/dist/schema.json examples/module/qdrant/schema.json\n"+
			"  published: %d bytes\n  code:      %d bytes", schema.SchemaFileName, len(published), len(fromCode))
	}
}

// TestBundleIsValid — the same rules keeper applies at approval time, applied at build
// time. Impl included: a Def with no implementation would serve nothing.
func TestBundleIsValid(t *testing.T) {
	for _, i := range qdrantBundle(&QdrantModule{}).Validate() {
		if i.Level == schema.LevelError {
			t.Errorf("bundle is invalid: %s at %s", i, i.Path)
		}
	}
}

// TestEverySideIsDeclared — `side: soul` is declared explicitly on every object
// (NIM-749). SideSoul is also the zero value, so an object that simply forgot the
// field would look identical in the document; this asserts the intent rather than the
// default.
func TestEverySideIsDeclared(t *testing.T) {
	for _, def := range qdrantBundle(&QdrantModule{}).Modules {
		if def.Side != "soul" {
			t.Errorf("module %q declares side %q, want \"soul\"", def.Name, def.Side)
		}
	}
}

// TestDeclaredStatesAreDispatched — the object that DECLARES a state is the one that
// SERVES it, in both directions. Five objects share one driver, so a state declared on
// `instance` and dispatched only by `collection` would lint clean, pass every param
// check, and fail at apply time with "unknown state" on a live host.
func TestDeclaredStatesAreDispatched(t *testing.T) {
	doc := loadDocument(t)
	served := objects(&QdrantModule{})

	if len(doc.Modules) != len(served) {
		t.Fatalf("the document declares %d modules, the artifact serves %d (%v)",
			len(doc.Modules), len(served), doc.ModuleNames())
	}

	for _, mod := range doc.Modules {
		obj, ok := served[mod.Name]
		if !ok {
			t.Errorf("module %q is declared but no object serves it", mod.Name)
			continue
		}
		declared := make([]string, 0, len(mod.States))
		for state := range mod.States {
			declared = append(declared, state)
		}
		sort.Strings(declared)

		for _, missing := range diff(declared, obj.states()) {
			t.Errorf("%s.%s is declared but nothing dispatches it", mod.Name, missing)
		}
		for _, extra := range diff(obj.states(), declared) {
			t.Errorf("%s.%s is dispatched but not declared — strictness has no contract for it", mod.Name, extra)
		}
	}
}

// with returns base plus extra as a fresh slice — the shared connectParams must never
// be appended into.
func with(base []string, extra ...string) []string {
	out := make([]string, 0, len(base)+len(extra))
	out = append(out, base...)
	return append(out, extra...)
}

// collectionShape is the declared shape of a collection, which `present` and
// `recreated` share.
var collectionShape = []string{
	"name", "vectors",
	"shard_number", "sharding_method", "wal_config",
	"replication_factor", "write_consistency_factor", "on_disk_payload",
	"hnsw_config", "optimizers_config", "quantization_config",
}

// TestManifestStatesDeclareWhatTheyAccept — every state declares EXACTLY the keys its
// implementation reads: the shared connect set plus its own. Both directions matter —
// a missing key is the NIM-206 hole, an extra one promises an operator a param nothing
// reads.
func TestManifestStatesDeclareWhatTheyAccept(t *testing.T) {
	want := map[string]map[string][]string{
		"instance": {
			"pinged":         with(connectParams),
			"ready-probed":   with(connectParams),
			"version-probed": with(connectParams),
		},
		"collection": {
			"probed":  with(connectParams, "name"),
			"absent":  with(connectParams, "name"),
			"present": with(connectParams, collectionShape...),
			// confirm_destroy is the ONLY difference between the two reconciling
			// states, and it is the parameter that stands between a scenario and the
			// loss of a collection. A row that let it drift would be the one place
			// this table has to be exact.
			"recreated": with(connectParams, append(append([]string{}, collectionShape...), "confirm_destroy")...),
		},
		"alias": {
			"present": with(connectParams, "name", "collection"),
			"absent":  with(connectParams, "name"),
		},
		"index": {
			"present": with(connectParams, "collection", "field", "schema"),
			"absent":  with(connectParams, "collection", "field"),
		},
		"snapshot": {
			"created": with(connectParams, "collection", "max_age_sec"),
			"absent":  with(connectParams, "collection", "name"),
		},
	}

	for objName, states := range want {
		t.Run(objName, func(t *testing.T) {
			mod := loadModule(t, objName)
			if len(mod.States) != len(states) {
				t.Fatalf("module %q declares %d states, table covers %d — a new state needs a row here",
					objName, len(mod.States), len(states))
			}

			for state, wantKeys := range states {
				t.Run(state, func(t *testing.T) {
					def, ok := mod.States[state]
					if !ok {
						t.Fatalf("module %q has no state %q", objName, state)
					}
					got := make([]string, 0, len(def.Input))
					for name := range def.Input {
						got = append(got, name)
					}
					sort.Strings(got)
					sorted := append([]string(nil), wantKeys...)
					sort.Strings(sorted)

					for _, missing := range diff(sorted, got) {
						t.Errorf("param %q is read but NOT declared — strictness has no contract for it (NIM-206)", missing)
					}
					for _, extra := range diff(got, sorted) {
						t.Errorf("param %q is declared but nothing reads it", extra)
					}
				})
			}
		})
	}
}

// TestManifestSecretParamsAreMasked — a credential or PEM param must be declared
// secret with the vault-ref pattern, or it reaches logs/traces/UI in the clear.
func TestManifestSecretParamsAreMasked(t *testing.T) {
	for _, mod := range loadDocument(t).Modules {
		for state, def := range mod.States {
			for name, p := range def.Input {
				if !secretParams[name] {
					continue
				}
				if !p.Secret {
					t.Errorf("%s.%s.%s: carries a credential/PEM but is not declared secret", mod.Name, state, name)
				}
				if p.Pattern != "^vault:.*" {
					t.Errorf("%s.%s.%s: secret param must pin pattern ^vault:.* , got %q", mod.Name, state, name, p.Pattern)
				}
			}
		}
	}
}

// TestEveryStateDeclaresTheApiKeyAsSecret — the artifact-wide invariant, stated
// positively rather than as "whatever is in secretParams is masked". Every state opens
// a connection, so every state can carry the credential, and one that declared it
// unmasked would leak it on that one action alone.
func TestEveryStateDeclaresTheApiKeyAsSecret(t *testing.T) {
	for _, mod := range loadDocument(t).Modules {
		for state, def := range mod.States {
			p, ok := def.Input["api_key"]
			if !ok {
				t.Errorf("%s.%s does not declare api_key, but it opens a connection", mod.Name, state)
				continue
			}
			if !p.Secret {
				t.Errorf("%s.%s.api_key is not declared secret", mod.Name, state)
			}
		}
	}
}

// TestConnectParamsAreRead — connectParams is what parseConnConfig actually reads, not
// a list that drifted from it. Each key is fed alone and must land in the resulting
// connConfig; a rename in conn.go/tls.go fails here rather than silently making the
// manifest table above assert the wrong set.
func TestConnectParamsAreRead(t *testing.T) {
	cases := []struct {
		key   string
		value any
		got   func(connConfig) any
		want  any
	}{
		{"addr", "qdrant-1:6333", func(c connConfig) any { return c.addr }, "qdrant-1:6333"},
		{"api_key", "s3cret", func(c connConfig) any { return c.apiKey }, "s3cret"},
		{"timeout_sec", 45, func(c connConfig) any { return c.timeoutSec }, 45},
		{"tls", true, func(c connConfig) any { return c.tls.enabled }, true},
		{"tls_ca", "CA-PEM", func(c connConfig) any { return c.tls.caPEM }, "CA-PEM"},
		{"tls_cert", "CERT-PEM", func(c connConfig) any { return c.tls.certPEM }, "CERT-PEM"},
		{"tls_key", "KEY-PEM", func(c connConfig) any { return c.tls.keyPEM }, "KEY-PEM"},
		{"tls_skip_verify", true, func(c connConfig) any { return c.tls.skipVerify }, true},
	}
	if len(cases) != len(connectParams) {
		t.Fatalf("connectParams has %d keys, table covers %d", len(connectParams), len(cases))
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			fields := map[string]any{"addr": "qdrant-1:6333"}
			fields[tc.key] = tc.value
			s, err := structpb.NewStruct(fields)
			if err != nil {
				t.Fatalf("build params: %v", err)
			}
			cfg, err := parseConnConfig(s)
			if err != nil {
				t.Fatalf("parseConnConfig: %v", err)
			}
			if got := tc.got(cfg); got != tc.want {
				t.Errorf("param %q not read by parseConnConfig: got %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

// diff returns the members of a missing from b; both must be sorted.
func diff(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, s := range b {
		in[s] = true
	}
	var out []string
	for _, s := range a {
		if !in[s] {
			out = append(out, s)
		}
	}
	return out
}
