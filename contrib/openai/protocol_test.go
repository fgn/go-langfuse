package langfuseopenai

import (
	"net/url"
	"testing"

	"github.com/fgn/go-langfuse"
)

func TestRecognizeOpenAIRoutes(t *testing.T) {
	cases := []struct {
		url        string
		wantOK     bool
		wantName   string
		wantType   langfuse.ObservationType
		provider   string
		deployment string
		apiVersion string
	}{
		{
			url:      "https://api.openai.com/v1/chat/completions",
			wantOK:   true,
			wantName: "openai.chat.completions",
			wantType: langfuse.TypeGeneration,
			provider: "openai",
		},
		{
			url:      "https://api.openai.com/v1/completions",
			wantOK:   true,
			wantName: "openai.completions",
			wantType: langfuse.TypeGeneration,
			provider: "openai",
		},
		{
			url:      "https://api.openai.com/v1/embeddings",
			wantOK:   true,
			wantName: "openai.embeddings",
			wantType: langfuse.TypeEmbedding,
			provider: "openai",
		},
		{
			url:        "https://example-resource.openai.azure.com/openai/deployments/prod%2Fgpt/chat/completions?api-version=2024-12-01-preview",
			wantOK:     true,
			wantName:   "openai.chat.completions",
			wantType:   langfuse.TypeGeneration,
			provider:   "azure-openai",
			deployment: "prod/gpt",
			apiVersion: "2024-12-01-preview",
		},
		{
			url:      "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
			wantOK:   true,
			wantName: "openai.chat.completions",
			wantType: langfuse.TypeGeneration,
			provider: "google-openai-compat",
		},
		{
			url:      "https://gw.example.com/proxy/v1/chat/completions",
			wantOK:   true,
			wantName: "openai.chat.completions",
			wantType: langfuse.TypeGeneration,
			provider: "openai-compatible",
		},
		{url: "https://api.openai.com/v1/responses", wantOK: true, wantName: "openai.responses", wantType: langfuse.TypeGeneration, provider: "openai"},
		{url: "https://api.openai.com/v1/responses/resp-1", wantOK: false},
		{url: "https://api.openai.com/v1/responses/resp-1/input_items", wantOK: false},
		{url: "https://api.openai.com/v1/models", wantOK: false},
		{url: "https://api.openai.com/v1/audio/transcriptions", wantOK: false},
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
		if route.Name != tc.wantName || route.Type != tc.wantType || route.Provider != tc.provider {
			t.Fatalf("%s: route %+v", tc.url, route)
		}
		if tc.deployment != "" && route.Metadata["azure.deployment"] != tc.deployment {
			t.Fatalf("%s: deployment metadata %v", tc.url, route.Metadata)
		}
		if route.APIVersion != tc.apiVersion {
			t.Fatalf("%s: api version %q", tc.url, route.APIVersion)
		}
		if route.Model != "" {
			t.Fatalf("%s: OpenAI routes must not derive a model from the URL, got %q", tc.url, route.Model)
		}
	}
}
