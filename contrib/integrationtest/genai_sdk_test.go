package integrationtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/httptransport"
	genai "google.golang.org/genai"

	langfusegenai "github.com/fgn/go-langfuse/contrib/googlegenai"
)

// TestGenAIDeveloperAPIUnary drives the real google.golang.org/genai
// client on the Developer API backend through the instrumented
// transport.
func TestGenAIDeveloperAPIUnary(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"candidates":[{"content":{"role":"model","parts":[{"text":"synthetic answer"}]},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":6,"candidatesTokenCount":2},
			"modelVersion":"gemini-3.6-flash-002"
		}`)
	}))
	t.Cleanup(provider.Close)

	client, err := genai.NewClient(context.Background(), &genai.ClientConfig{
		Backend: genai.BackendGeminiAPI,
		APIKey:  "synthetic-key",
		HTTPClient: &http.Client{
			Transport: langfusegenai.NewTransport(lf, nil),
		},
		HTTPOptions: genai.HTTPOptions{BaseURL: provider.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Models.GenerateContent(context.Background(),
		"gemini-3.6-flash", genai.Text("synthetic question"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Text() != "synthetic answer" {
		t.Fatalf("SDK result altered: %q", response.Text())
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if span.GetName() != "genai.generate_content" {
		t.Fatalf("span name %q", span.GetName())
	}
	if got := attrString(span, "langfuse.observation.model.name"); got != "gemini-3.6-flash-002" {
		t.Fatalf("modelVersion override missing: %q", got)
	}
	if got := attrString(span, "langfuse.observation.usage_details"); got == "" {
		t.Fatal("usage missing")
	}
}

// TestGenAIStreaming locks Gemini's sentinel-free stream semantics
// against the real SDK iterator: clean EOF completes the observation.
func TestGenAIStreaming(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"candidates":[{"content":{"role":"model","parts":[{"text":"streamed "}]}}]}`,
			`data: {"candidates":[{"content":{"role":"model","parts":[{"text":"answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2}}`,
		} {
			_, _ = io.WriteString(w, chunk+"\r\n\r\n")
			flusher.Flush()
		}
	}))
	t.Cleanup(provider.Close)

	client, err := genai.NewClient(context.Background(), &genai.ClientConfig{
		Backend: genai.BackendGeminiAPI,
		APIKey:  "synthetic-key",
		HTTPClient: &http.Client{
			Transport: langfusegenai.NewTransport(lf, nil),
		},
		HTTPOptions: genai.HTTPOptions{BaseURL: provider.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	var assembled string
	for chunk, err := range client.Models.GenerateContentStream(context.Background(),
		"gemini-3.6-flash", genai.Text("synthetic question"), nil) {
		if err != nil {
			t.Fatal(err)
		}
		assembled += chunk.Text()
	}
	if assembled != "streamed answer" {
		t.Fatalf("SDK stream altered: %q", assembled)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := attrString(span, "langfuse.observation.output"); got != "streamed answer" {
		t.Fatalf("stream output %q", got)
	}
	if got := attrString(span, "langfuse.observation.status_message"); got != "" {
		t.Fatalf("clean Gemini stream carries status %q", got)
	}
}

// staticToken implements auth.TokenProvider offline.
type staticToken struct{}

func (staticToken) Token(context.Context) (*auth.Token, error) {
	return &auth.Token{Value: "synthetic-oauth-token", Expiry: time.Now().Add(time.Hour)}, nil
}

// sentinelTransport proves caller transport policy survives the
// documented Vertex composition.
type sentinelTransport struct {
	inner http.RoundTripper
	seen  *bool
}

func (s sentinelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	*s.seen = true
	return s.inner.RoundTrip(req)
}

// TestVertexCompositionPreservesPolicyAndAuth locks the README's
// Vertex wiring end to end with the real genai client: the caller's
// sentinel base transport is used, the OAuth token is attached by the
// inner auth transport, the Langfuse observation is recorded, and the
// adapter never sees the Authorization header (asserted structurally:
// the recorder sits outside the auth layer, which is the only place
// the header is added).
func TestVertexCompositionPreservesPolicyAndAuth(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)

	var authHeader string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"candidates":[{"content":{"role":"model","parts":[{"text":"vertex answer"}]},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1}
		}`)
	}))
	t.Cleanup(provider.Close)

	creds := auth.NewCredentials(&auth.CredentialsOptions{TokenProvider: staticToken{}})
	sentinelSeen := false
	base := &http.Client{
		Timeout:   30 * time.Second,
		Transport: sentinelTransport{inner: http.DefaultTransport, seen: &sentinelSeen},
	}

	// The documented composition: resolve the caller transport, build
	// the authenticated transport over it, layer Langfuse outside.
	baseRT := base.Transport
	if baseRT == nil {
		baseRT = http.DefaultTransport
	}
	authed, err := httptransport.NewClient(&httptransport.Options{
		Credentials:      creds,
		BaseRoundTripper: baseRT,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := *base
	client.Transport = langfusegenai.NewTransport(lf, authed.Transport)

	gemini, err := genai.NewClient(context.Background(), &genai.ClientConfig{
		Backend:     genai.BackendVertexAI,
		Project:     "synthetic-project",
		Location:    "eu",
		Credentials: creds,
		HTTPClient:  &client,
		HTTPOptions: genai.HTTPOptions{BaseURL: provider.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := gemini.Models.GenerateContent(context.Background(),
		"gemini-3.6-flash", genai.Text("synthetic question"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Text() != "vertex answer" {
		t.Fatalf("SDK result altered: %q", response.Text())
	}
	if authHeader != "Bearer synthetic-oauth-token" {
		t.Fatalf("OAuth composition broken: Authorization = %q", authHeader)
	}
	if !sentinelSeen {
		t.Fatal("caller transport policy dropped: sentinel base transport never used")
	}
	if client.Timeout != 30*time.Second {
		t.Fatal("caller client Timeout lost")
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if span.GetName() != "genai.generate_content" {
		t.Fatalf("span name %q", span.GetName())
	}
	if got := attrString(span, "langfuse.observation.output"); got == "" {
		t.Fatal("output missing through Vertex composition")
	}
}

// TestGenAITransformerModelForms drives the pinned genai transformer
// with every model-name form it accepts and asserts the adapter's URL
// grammar classifies each as reviewed: bare and tuned Developer models
// and Vertex publisher models yield a model; other fully qualified
// project resources are observed with the model unset.
func TestGenAITransformerModelForms(t *testing.T) {
	cases := []struct {
		backend   genai.Backend
		model     string
		wantModel string
	}{
		{genai.BackendGeminiAPI, "gemini-3.6-flash", "gemini-3.6-flash"},
		{genai.BackendGeminiAPI, "models/gemini-3.6-flash", "gemini-3.6-flash"},
		{genai.BackendGeminiAPI, "tunedModels/my-tuned-1", "my-tuned-1"},
		{genai.BackendVertexAI, "gemini-3.6-flash", "gemini-3.6-flash"},
		{genai.BackendVertexAI, "publishers/anthropic/models/claude-sonnet-5", "claude-sonnet-5"},
		// A fully qualified project-model resource is deliberately not
		// collapsed to a bare model (reviewed design).
		{genai.BackendVertexAI, "projects/p/locations/eu/models/custom-1", ""},
	}
	for _, tc := range cases {
		receiver := newOTLPReceiver(t)
		lf := newTestClient(t, receiver)
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
		}))
		cfg := &genai.ClientConfig{
			Backend:     tc.backend,
			HTTPClient:  &http.Client{Transport: langfusegenai.NewTransport(lf, nil)},
			HTTPOptions: genai.HTTPOptions{BaseURL: provider.URL},
		}
		if tc.backend == genai.BackendVertexAI {
			cfg.Project = "p"
			cfg.Location = "eu"
			cfg.Credentials = auth.NewCredentials(&auth.CredentialsOptions{TokenProvider: staticToken{}})
		} else {
			cfg.APIKey = "synthetic-key"
		}
		client, err := genai.NewClient(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Models.GenerateContent(context.Background(),
			tc.model, genai.Text("q"), nil); err != nil {
			t.Fatalf("%s %q: %v", tc.backend, tc.model, err)
		}
		flush(t, lf)
		span := receiver.nextSpan(t)
		got := attrString(span, "langfuse.observation.model.name")
		if got != tc.wantModel {
			t.Fatalf("%s %q: adapter model %q, want %q", tc.backend, tc.model, got, tc.wantModel)
		}
		provider.Close()
	}
}
