package qdrant

import (
	"crypto/tls"
	"strings"
	"testing"
)

// buildTLSConfig is a pure function, so the security posture is checked here rather
// than inferred from a live handshake: default-secure, with the insecure side reachable
// only by an explicit opt-out.

func TestBuildTLSConfigIsNilWhenTLSIsOff(t *testing.T) {
	cfg, err := buildTLSConfig(tlsParams{})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if cfg != nil {
		t.Error("tls disabled must yield a nil *tls.Config (plaintext http)")
	}
}

// TestBuildTLSConfigVerifiesByDefault — the direction of the default is the whole
// point. `tls: true` with nothing else must verify the server certificate; an
// implementation that defaulted the other way would send the api-key to whoever
// answered.
func TestBuildTLSConfigVerifiesByDefault(t *testing.T) {
	cfg, err := buildTLSConfig(tlsParams{enabled: true})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if cfg.InsecureSkipVerify {
		t.Error("verification must be ON unless tls_skip_verify was explicitly set")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
	}
}

func TestBuildTLSConfigHonoursTheExplicitOptOut(t *testing.T) {
	cfg, err := buildTLSConfig(tlsParams{enabled: true, skipVerify: true})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("an explicit tls_skip_verify must be honoured")
	}
}

func TestBuildTLSConfigRejectsABrokenCA(t *testing.T) {
	_, err := buildTLSConfig(tlsParams{enabled: true, caPEM: "not a pem"})
	if err == nil {
		t.Fatal("an unparseable CA PEM must be an error, not a silent fallback to the system pool")
	}
	if !strings.Contains(err.Error(), "tls_ca") {
		t.Errorf("the error must name the parameter: %v", err)
	}
}

// TestBuildTLSConfigRejectsHalfAClientCertPair — one half without the other is an
// operator error, and guessing which they meant is what this artifact does not do.
func TestBuildTLSConfigRejectsHalfAClientCertPair(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    tlsParams
	}{
		{"cert without key", tlsParams{enabled: true, certPEM: "CERT"}},
		{"key without cert", tlsParams{enabled: true, keyPEM: "KEY"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildTLSConfig(tc.p); err == nil {
				t.Error("half an mTLS pair must be refused")
			}
		})
	}
}

// TestBuildTLSConfigErrorsCarryNoKeyMaterial — an error text becomes an event message,
// a log line and a trace. The PEM must not ride along (ADR-010).
func TestBuildTLSConfigErrorsCarryNoKeyMaterial(t *testing.T) {
	const secret = "-----BEGIN PRIVATE KEY-----SECRETKEYMATERIAL-----END PRIVATE KEY-----"
	_, err := buildTLSConfig(tlsParams{enabled: true, certPEM: "not a cert", keyPEM: secret})
	if err == nil {
		t.Fatal("an invalid client certificate pair must be an error")
	}
	if strings.Contains(err.Error(), "SECRETKEYMATERIAL") {
		t.Errorf("the error text carries key material: %v", err)
	}
}

// TestBaseURLFollowsTheTLSFlag — one place decides the scheme, and it is the boolean
// the type check protects (NIM-778). A second source for that answer is how a
// credential ends up in plaintext.
func TestBaseURLFollowsTheTLSFlag(t *testing.T) {
	plain := connConfig{addr: "qdrant-1:6333"}
	if got := plain.baseURL(); got != "http://qdrant-1:6333" {
		t.Errorf("baseURL = %q, want http://qdrant-1:6333", got)
	}
	secure := connConfig{addr: "qdrant-1:6333", tls: tlsParams{enabled: true}}
	if got := secure.baseURL(); got != "https://qdrant-1:6333" {
		t.Errorf("baseURL = %q, want https://qdrant-1:6333", got)
	}
}
