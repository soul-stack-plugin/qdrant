// TLS for the qdrant plugin's HTTP transport.
//
// Security model (default secure, insecure is an EXPLICIT opt-out): with tls=true
// the plugin VERIFIES the server certificate against the supplied PEM CA. A client
// certificate (mTLS) is optional. Verification is disabled only by an explicit
// tls_skip_verify=true, which defaults to false.
//
// The PEMs arrive WHOLE in params — the scenario resolves them from Vault in the
// render phase and passes the value; the plugin holds no Vault client of its own
// (its only capability is network_outbound). tls_ca/tls_cert/tls_key are declared
// secret and masked by the output layer on the key name, so they do not appear in
// events, logs or errors.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
)

// tlsParams are the raw TLS parameters, kept separate from *tls.Config so the
// factory stays a pure function and is tested without a live socket.
type tlsParams struct {
	enabled    bool
	caPEM      string // PEM CA for server certificate verification (RootCAs)
	certPEM    string // PEM client cert for mTLS (optional; with keyPEM)
	keyPEM     string // PEM client key for mTLS (optional; with certPEM)
	skipVerify bool   // EXPLICIT opt-out of certificate verification (default false)
}

// parseTLS extracts the TLS parameters. All are optional: a missing or false `tls`
// means a plaintext connection.
//
// ONLY missing or `false` reads as plaintext. A `tls` that is neither — the string
// "true", a number — never reaches here: [object.Apply] refuses it against the
// declared type first (params.go, NIM-778).
func parseTLS(f map[string]*structpb.Value) tlsParams {
	return tlsParams{
		enabled:    boolOrDefault(f["tls"], false),
		caPEM:      stringOrEmpty(f["tls_ca"]),
		certPEM:    stringOrEmpty(f["tls_cert"]),
		keyPEM:     stringOrEmpty(f["tls_key"]),
		skipVerify: boolOrDefault(f["tls_skip_verify"], false),
	}
}

// buildTLSConfig builds a *tls.Config from tlsParams. Returns nil, nil when TLS is
// not enabled (the caller then speaks http://). An error means a broken PEM; the
// error text does NOT contain the PEM.
//
// A pure function, so L0 checks the result directly — RootCAs loaded, skip_verify
// forwarded, client cert attached — with no live Qdrant.
func buildTLSConfig(p tlsParams) (*tls.Config, error) {
	if !p.enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: p.skipVerify, //nolint:gosec // EXPLICIT operator opt-out (tls_skip_verify), false by default — verification is on
	}

	// RootCAs from the supplied CA PEM. Without skip_verify and without a CA the
	// verification falls back to the system trust pool, which for a private PKI is
	// normally a failure — so a CA is in practice required in TLS mode. An empty CA
	// with skip_verify=false is legitimate for a publicly trusted certificate.
	if p.caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(p.caPEM)) {
			return nil, fmt.Errorf("tls_ca: failed to parse PEM CA certificate")
		}
		cfg.RootCAs = pool
	}

	// A client certificate (mTLS) is optional, and only if BOTH halves are sent.
	// One without the other is an operator configuration error.
	switch {
	case p.certPEM != "" && p.keyPEM != "":
		pair, err := tls.X509KeyPair([]byte(p.certPEM), []byte(p.keyPEM))
		if err != nil {
			return nil, fmt.Errorf("tls_cert/tls_key: invalid client certificate pair (mTLS)")
		}
		cfg.Certificates = []tls.Certificate{pair}
	case p.certPEM != "" || p.keyPEM != "":
		return nil, fmt.Errorf("tls_cert and tls_key are specified only TOGETHER (mTLS client certificate)")
	}

	return cfg, nil
}
