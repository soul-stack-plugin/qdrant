// The artifact's bundle — the five objects `qdrant` serves, and the schema document
// generated from them.
//
// The Go value is the source of truth (NIM-377), and it is the ONLY copy: this
// repository commits no schema.json. `soul-mod stamp` runs the artifact's own `schema`
// subcommand, checks the document and writes it into the binary as a trailer plus
// beside it in dist/; `make verify` is what proves a stamped artifact still matches
// the code, and the release workflow publishes the document as an asset. Nothing here
// is hand-written JSON, and the guards in manifest_test.go render this value in memory
// rather than reading a file, so they assert the contract the binary will carry.
//
// The document carries NO name of its own — not the artifact's, not a namespace.
// Address level 1 is the alias an operator writes in `keeper.yml::plugins.*[].name`
// and level 2 is a module name below (ADR-020(p), NIM-377).
package qdrant

import "github.com/souls-guild/soul-stack/sdk/module"

// Bundle is the artifact: five objects over one REST driver.
//
// The order is alphabetical and that is load-bearing — `modules` is a JSON array,
// so the canonical bytes keep whatever order this slice has, and they are hashed
// and signed.
func Bundle(m *Module) module.Bundle {
	return module.Bundle{
		Modules: []module.Def{
			aliasDef(m),
			collectionDef(m),
			indexDef(m),
			instanceDef(m),
			snapshotDef(m),
		},
	}
}
