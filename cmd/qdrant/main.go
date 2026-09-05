// Entry point of the qdrant plugin. The host runs this binary as a sub-process, does
// the gRPC-stdio handshake (sdk/handshake) and calls RPC SoulModule.
//
// [module.ServeBundle] dispatches on argv[1]: an object name (`qdrant collection`)
// serves that object over gRPC, and `schema` prints the artifact's own document —
// which is how `soul-mod stamp` derives what it stamps, and why the schema is never
// committed to this repository. The document is a build output, not a source file.
package main

import (
	"github.com/soul-stack-plugin/qdrant/internal/qdrant"
	"github.com/souls-guild/soul-stack/sdk/module"
)

func main() {
	module.ServeBundle(qdrant.Bundle(&qdrant.Module{}))
}
