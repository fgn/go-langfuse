package transport

import (
	"net/url"
	"testing"
)

func FuzzNormalizeEndpoint(f *testing.F) {
	for _, seed := range []string{
		"",
		"https://cloud.langfuse.com",
		"http://localhost:3000/api/public/otel",
		"https://example.com/api/public/otel/v1/traces",
		"https://user:secret@example.com",
		"file:///tmp/socket",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		endpoint, err := NormalizeEndpoint(raw)
		if err != nil {
			return
		}
		parsed, err := url.Parse(endpoint)
		if err != nil {
			t.Fatalf("normalized endpoint does not parse: %v", err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			t.Fatalf("unsafe scheme %q", parsed.Scheme)
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			t.Fatalf("normalized endpoint retained unsafe URL components")
		}
		if parsed.Path != tracesPath {
			t.Fatalf("normalized path = %q, want %q", parsed.Path, tracesPath)
		}
	})
}
