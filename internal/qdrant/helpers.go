package qdrant

import (
	"fmt"
	"sort"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// validateAddr — the connection check shared by every action.
//
// It runs the REAL parse, [parseConnConfig], rather than repeating its rules. That is
// deliberate and it is NIM-786 applied to this one function: the first version checked
// only that `addr` was non-empty, so `addr: "https://qdrant-1:6333"` passed Validate
// and failed in Apply — a clean validation phase followed by a failure partway through
// a run, which is precisely the shape the rule forbids. Two lists of rules drift; one
// function cannot.
func validateAddr(f map[string]*structpb.Value) []string {
	cfg, err := parseConnConfig(&structpb.Struct{Fields: f})
	if err != nil {
		return []string{err.Error()}
	}
	// And the TLS material, for the same reason. buildTLSConfig is a pure function of
	// the declared params — a truncated PEM or half an mTLS pair is knowable here,
	// with nothing yet done to the host. Leaving it to the connect made Validate
	// report Ok on a declaration Apply was certain to refuse, which is precisely the
	// hole this rule closes.
	if _, err := buildTLSConfig(cfg.tls); err != nil {
		return []string{err.Error()}
	}
	return nil
}

// validateName checks one required string param that names a subject (a collection,
// an alias, a field). addr is the full parameter address for the message.
//
// A name carrying whitespace or a slash is refused rather than sent: it goes into
// the request PATH, so a slash would silently address a different endpoint
// (`collections/a/b` is not a collection called "a/b") and trailing whitespace would
// address a collection the operator cannot see in any listing.
func validateName(f map[string]*structpb.Value, key string) []string {
	s, _ := stringValue(f[key])
	switch {
	case strings.TrimSpace(s) == "":
		return []string{fmt.Sprintf("params.%s: must be a non-empty string", key)}
	case s != strings.TrimSpace(s):
		return []string{fmt.Sprintf("params.%s: must not carry leading or trailing whitespace (it addresses a URL path segment)", key)}
	case strings.ContainsAny(s, "/?#"):
		return []string{fmt.Sprintf("params.%s: must not contain %q, %q or %q — the value is a URL path segment, and one of those addresses a different endpoint", key, "/", "?", "#")}
	}
	return nil
}

// redactError strips secrets from error text. net/http reports the URL it failed on
// and never the headers, so the api-key does not reach here by the ordinary path —
// but a TLS handshake error can carry key material, and "does not reach here by the
// ordinary path" is not a property worth betting a credential on (ADR-010). Empty
// secrets are a no-op.
func redactError(err error, secrets ...string) string {
	return redactString(err.Error(), secrets...)
}

// redactString is redactError over an arbitrary message — a response body is echoed
// into diagnostics in places, and Qdrant echoes back what it was sent.
func redactString(msg string, secrets ...string) string {
	for _, s := range secrets {
		if s != "" {
			msg = strings.ReplaceAll(msg, s, "***")
		}
	}
	return msg
}

// sortedKeys — deterministic order for diagnostics built out of a map, so identical
// state produces identical output.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- structpb helpers ---

func stringValue(v *structpb.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	if sv, ok := v.GetKind().(*structpb.Value_StringValue); ok {
		return sv.StringValue, true
	}
	return "", false
}

// stringOrEmpty — the string value or "" for an optional field. Returns the string
// as it is; a non-string or nil reads as "".
func stringOrEmpty(v *structpb.Value) string {
	s, _ := stringValue(v)
	return s
}

// boolOrDefault and intOrDefault read a DECLARED top-level param, and their last arm
// — a value of another type reading as the default — is unreachable through the
// plugin's own entry points: [object.Apply] and [object.Validate] refuse such a value
// against the declaration before either is called (params.go, NIM-778). That arm is
// what made `tls: "true"` a plaintext connection on the redis artifact, so it is left
// here only because a total function still needs one, not because any caller may rely
// on it.
func boolOrDefault(v *structpb.Value, def bool) bool {
	if v == nil {
		return def
	}
	if bv, ok := v.GetKind().(*structpb.Value_BoolValue); ok {
		return bv.BoolValue
	}
	return def
}

func intOrDefault(v *structpb.Value, def int) int {
	if v == nil {
		return def
	}
	if nv, ok := v.GetKind().(*structpb.Value_NumberValue); ok {
		return int(nv.NumberValue)
	}
	return def
}

// structAsMap converts a map-typed param into a plain Go map, the same shape
// encoding/json produces from Qdrant's response. Having both sides in one
// representation is what lets [subsetEquals] compare a declaration against a live
// config without a per-field decoder.
//
// nil / non-map reads as nil — the caller distinguishes "not declared" from "declared
// empty" by the presence of the key, not by this result.
func structAsMap(v *structpb.Value) map[string]any {
	if v == nil {
		return nil
	}
	sv, ok := v.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return nil
	}
	return sv.StructValue.AsMap()
}

// --- ApplyEvent helpers ---

func sendOutcome(stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], changed bool, message string, output map[string]any) error {
	out, err := structpb.NewStruct(output)
	if err != nil {
		return fmt.Errorf("build output struct: %w", err)
	}
	return stream.Send(&pluginv1.ApplyEvent{Message: message, Changed: changed, Output: out})
}

func sendFailure(stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], message string) error {
	return stream.Send(&pluginv1.ApplyEvent{Message: message, Failed: true})
}
