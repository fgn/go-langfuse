package langfusegenai

import (
	"net/url"
	"testing"

	"github.com/fgn/go-langfuse"
)

// TestRecognizeRouteGrammar drives the URL grammar with the resource
// shapes the pinned genai SDK constructs: bare Developer models, tuned
// models, Vertex Google and partner publishers, fully qualified
// resources, and reverse-proxy prefixes, across the three API versions.
func TestRecognizeRouteGrammar(t *testing.T) {
	cases := []struct {
		url       string
		wantOK    bool
		wantName  string
		wantModel string
		wantType  langfuse.ObservationType
		streaming bool
		provider  string
	}{
		{
			url:       "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
			wantOK:    true,
			wantName:  "genai.generate_content",
			wantModel: "gemini-2.5-flash",
			wantType:  langfuse.TypeGeneration,
			provider:  "google-genai",
		},
		{
			url:       "https://generativelanguage.googleapis.com/v1beta/tunedModels/my-tuned-1:streamGenerateContent?alt=sse",
			wantOK:    true,
			wantName:  "genai.generate_content_stream",
			wantModel: "my-tuned-1",
			wantType:  langfuse.TypeGeneration,
			streaming: true,
			provider:  "google-genai",
		},
		{
			url:       "https://eu-aiplatform.googleapis.com/v1beta1/projects/proj-1/locations/eu/publishers/google/models/gemini-2.5-pro:generateContent",
			wantOK:    true,
			wantName:  "genai.generate_content",
			wantModel: "gemini-2.5-pro",
			wantType:  langfuse.TypeGeneration,
			provider:  "google-vertex",
		},
		{
			url:       "https://aiplatform.googleapis.com/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude-sonnet-5:streamGenerateContent",
			wantOK:    true,
			wantName:  "genai.generate_content_stream",
			wantModel: "claude-sonnet-5",
			wantType:  langfuse.TypeGeneration,
			streaming: true,
			provider:  "google-vertex",
		},
		{
			url:       "https://gw.example.com/langfuse-proxy/v1beta/models/gemini-2.5-flash:embedContent",
			wantOK:    true,
			wantName:  "genai.embed_content",
			wantModel: "gemini-2.5-flash",
			wantType:  langfuse.TypeEmbedding,
			provider:  "google-genai-compatible",
		},
		{
			url:       "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents",
			wantOK:    true,
			wantName:  "genai.batch_embed_contents",
			wantModel: "text-embedding-004",
			wantType:  langfuse.TypeEmbedding,
			provider:  "google-genai",
		},
		{
			url:       "https://eu-aiplatform.googleapis.com/v1/projects/p/locations/eu/publishers/google/models/text-multilingual-embedding-002:predict",
			wantOK:    true,
			wantName:  "genai.predict",
			wantModel: "text-multilingual-embedding-002",
			wantType:  langfuse.TypeEmbedding,
			provider:  "google-vertex",
		},
		{
			// Escaped segments decode; over-long ones are rejected.
			url:       "https://generativelanguage.googleapis.com/v1beta/models/gemini%2Dflash:generateContent",
			wantOK:    true,
			wantName:  "genai.generate_content",
			wantModel: "gemini-flash",
			wantType:  langfuse.TypeGeneration,
			provider:  "google-genai",
		},
		{
			url:    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:countTokens",
			wantOK: false,
		},
		{
			url:    "https://generativelanguage.googleapis.com/v1beta/models",
			wantOK: false,
		},
		{
			url:    "https://generativelanguage.googleapis.com/upload/v1beta/files",
			wantOK: false,
		},
	}

	proto := protocol{captureCap: 1 << 16}
	for _, tc := range cases {
		parsed, err := url.Parse(tc.url)
		if err != nil {
			t.Fatal(err)
		}
		route, ok := proto.Recognize(parsed)
		if ok != tc.wantOK {
			t.Fatalf("%s: recognized=%v want %v", tc.url, ok, tc.wantOK)
		}
		if !ok {
			continue
		}
		if route.Name != tc.wantName || route.Model != tc.wantModel ||
			route.Type != tc.wantType || route.Streaming != tc.streaming ||
			route.Provider != tc.provider {
			t.Fatalf("%s: route %+v", tc.url, route)
		}
	}
}

// TestRecognizeQualifiedResourceWithoutModel locks the explicit
// handling of accepted resources the grammar cannot map to a bare
// model: observed, model unset, resource recorded as metadata.
func TestRecognizeQualifiedResourceWithoutModel(t *testing.T) {
	parsed, err := url.Parse("https://eu-aiplatform.googleapis.com/v1beta1/projects/p/locations/eu/cachedContents/cc-1:generateContent")
	if err != nil {
		t.Fatal(err)
	}
	route, ok := protocol{}.Recognize(parsed)
	if !ok {
		t.Fatal("qualified resource was not observed")
	}
	if route.Model != "" {
		t.Fatalf("model %q, want empty", route.Model)
	}
	if route.Metadata["resource"] == "" {
		t.Fatal("resource metadata missing")
	}
}
