// The connection parameters every action declares, in one place.
//
// The redis artifact spells this set out inside each state. Here it is a function,
// for a reason that is not only brevity: [object] refuses a wrong-typed value against
// the DECLARATION (params.go, NIM-778), so a state that spelled `tls` as a string by
// accident would silently lose that protection for itself alone, and thirteen hand-
// copied blocks are thirteen chances to do it. The rendered document is identical
// either way — the guard in manifest_test.go checks the rendered form, not this one.
package qdrant

import "github.com/souls-guild/soul-stack/sdk/module"

// connectInput is the set [parseConnConfig] and [parseTLS] read, for every action of
// every object. Returns a fresh map each call: the values end up in the bundle, and a
// shared map would let one state's edit reach another's declaration.
func connectInput() module.Input {
	return module.Input{
		"addr": {Type: module.String, Required: true,
			Description: "Qdrant REST API address as host:port (e.g. \"qdrant-1:6333\"). WITHOUT a scheme — http/https is chosen by `tls`, and a scheme here is refused rather than allowed to contradict it.",
		},
		"api_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
			Description: "Qdrant API key (vault-ref in operator input; keeper resolves it before Apply). Masked in logs/traces/UI (secret). Sent in the `api-key` REQUEST HEADER only — never in the URL (Qdrant answers 401 to a key in the query string, so no URL form of it exists to leak) and never in a command argument, since nothing here spawns a process for `ps` to read.",
		},
		"timeout_sec": {Type: module.Int, Default: defaultTimeoutSec,
			Description: "Per-request timeout in seconds. Bounds every call, so a wedged instance fails the task instead of hanging the run. Raise it for a create over many shards.",
		},
		"tls": {Type: module.Bool, Default: false,
			Description: "Speak https to the API. Default false (plaintext http). A non-boolean value is REFUSED rather than coerced: the coercion would fall back on the side that sends the api-key in the clear.",
		},
		"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
			Description: "PEM CA certificate used to verify the server certificate (RootCAs). Masked (secret). Resolved keeper-side from Vault in the render phase.",
		},
		"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
			Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
		},
		"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
			Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events or errors.",
		},
		"tls_skip_verify": {Type: module.Bool, Default: false,
			Description: "EXPLICIT opt-out of server certificate verification. Default false (verification on — default secure).",
		},
	}
}

// withConnect returns the connection set plus the action's own params. A key in own
// that collides with a connection key is a programming error, not an override — the
// guard in manifest_test.go asserts the exact rendered key set of every state, so a
// collision surfaces there rather than as a quietly reshaped contract.
func withConnect(own module.Input) module.Input {
	in := connectInput()
	for name, p := range own {
		in[name] = p
	}
	return in
}

// collectionParam is the subject param shared by the four objects that address a
// collection by name (collection, index, snapshot address it as `name` or
// `collection` depending on whether the collection IS the subject).
func collectionParam(description string) module.Param {
	return module.Param{Type: module.String, Required: true, Description: description}
}
