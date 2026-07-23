//go:build validation

package validation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/httptransport"
	genai "google.golang.org/genai"

	langfusegenai "github.com/fgn/go-langfuse/contrib/googlegenai"
)

// vertexClient builds the genai client through the documented auth
// composition, now validated against the real OAuth flow. Credentials
// come from VERTEX_CREDENTIALS_JSON (a path outside the repository, or
// inline JSON) or ambient application default credentials; a detection
// failure skips, like any other missing credential.
func vertexClient(t *testing.T, r *run, env map[string]string) *genai.Client {
	t.Helper()
	options := &credentials.DetectOptions{
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
	}
	if configured := os.Getenv("VERTEX_CREDENTIALS_JSON"); configured != "" {
		if strings.HasPrefix(strings.TrimSpace(configured), "{") {
			options.CredentialsJSON = []byte(configured)
		} else {
			rejectRepoPath(t, "VERTEX_CREDENTIALS_JSON", configured)
			raw, err := os.ReadFile(configured)
			if err != nil {
				t.Fatalf("read VERTEX_CREDENTIALS_JSON: %v", err)
			}
			options.CredentialsJSON = raw
		}
	}
	creds, err := credentials.DetectDefault(options)
	if err != nil {
		t.Skipf("skipped; no usable Google credentials: set VERTEX_CREDENTIALS_JSON or application default credentials")
	}

	authed, err := httptransport.NewClient(&httptransport.Options{
		Credentials:      creds,
		BaseRoundTripper: http.DefaultTransport,
	})
	if err != nil {
		t.Fatalf("build authenticated transport: %v", err)
	}
	client := &http.Client{Transport: langfusegenai.NewTransport(r.lf, authed.Transport)}

	gemini, err := genai.NewClient(context.Background(), &genai.ClientConfig{
		Backend:     genai.BackendVertexAI,
		Project:     env["VERTEX_PROJECT"],
		Location:    env["VERTEX_LOCATION"],
		Credentials: creds,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("create genai client: %v", err)
	}
	return gemini
}

// noThinking disables thought parts so text output stays a plain,
// exactly comparable string and reasoning buckets stay empty.
func noThinking() *genai.GenerateContentConfig {
	return &genai.GenerateContentConfig{
		Temperature:     genai.Ptr[float32](0),
		MaxOutputTokens: 16,
		CandidateCount:  1,
		ThinkingConfig:  &genai.ThinkingConfig{ThinkingBudget: genai.Ptr[int32](0)},
	}
}

// vertexUsage projects UsageMetadata into the exclusive readback
// buckets after the core's documented inclusive mapping.
func vertexUsage(usage *genai.GenerateContentResponseUsageMetadata) map[string]int64 {
	prompt := int64(usage.PromptTokenCount)
	candidates := int64(usage.CandidatesTokenCount)
	cached := int64(usage.CachedContentTokenCount)
	thoughts := int64(usage.ThoughtsTokenCount)
	toolUse := int64(usage.ToolUsePromptTokenCount)
	buckets := map[string]int64{
		"input":  prompt - cached,
		"output": candidates,
		"total":  prompt + toolUse + candidates + thoughts,
	}
	if cached > 0 {
		buckets["input_cached_tokens"] = cached
	}
	if thoughts > 0 {
		buckets["output_reasoning_tokens"] = thoughts
	}
	if toolUse > 0 {
		buckets["input_tool_use_tokens"] = toolUse
	}
	return buckets
}

// vertexModelExpectation applies the adapter contract confirmed by
// real-provider runs: the URL-derived model is recorded at start
// (trusted, caller-controlled), a response modelVersion overrides it,
// and request_model metadata appears only when the two differ.
func vertexModelExpectation(want *expectedObservation, modelVersion, urlModel string) {
	if modelVersion == "" {
		want.Model = urlModel
		return
	}
	want.Model = modelVersion
	want.RequestModel = urlModel
}

func TestVertexUnary(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "VERTEX_PROJECT", "VERTEX_LOCATION", "VERTEX_MODEL")
	gemini := vertexClient(t, r, env)

	var response *genai.GenerateContentResponse
	traceID, err := r.call(t, "vertex-unary", func(ctx context.Context) error {
		var callErr error
		response, callErr = gemini.Models.GenerateContent(ctx, env["VERTEX_MODEL"],
			genai.Text("Reply with one short word. Marker: "+r.marker), noThinking())
		return callErr
	})
	if err != nil {
		t.Fatalf("vertex unary call: %v", err)
	}
	if response.UsageMetadata == nil || len(response.Candidates) == 0 ||
		response.Candidates[0].Content == nil || len(response.Candidates[0].Content.Parts) == 0 {
		t.Fatal("inconclusive: the SDK response carries no comparable output or usage")
	}

	parts := make([]any, 0, len(response.Candidates[0].Content.Parts))
	for _, part := range response.Candidates[0].Content.Parts {
		if part.Thought {
			parts = append(parts, map[string]any{"thought": true, "text": part.Text})
		} else {
			parts = append(parts, map[string]any{"text": part.Text})
		}
	}
	want := expectedObservation{
		Name:    "genai.generate_content",
		Type:    "GENERATION",
		TraceID: traceID,
		Usage:   vertexUsage(response.UsageMetadata),
		Output: map[string]any{
			"role":  response.Candidates[0].Content.Role,
			"parts": parts,
		},
		InputMarker: r.marker,
		Metadata: map[string]string{
			"provider":      "google-vertex",
			"finish_reason": string(response.Candidates[0].FinishReason),
		},
	}
	vertexModelExpectation(&want, response.ModelVersion, env["VERTEX_MODEL"])

	got := r.observation(t, traceID, "genai.generate_content", ingested)
	checkObservation(t, got, want)
}

func TestVertexStreaming(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "VERTEX_PROJECT", "VERTEX_LOCATION", "VERTEX_MODEL")
	gemini := vertexClient(t, r, env)

	var aggregated strings.Builder
	var lastUsage *genai.GenerateContentResponseUsageMetadata
	var lastModelVersion, finishReason string
	traceID, err := r.call(t, "vertex-stream", func(ctx context.Context) error {
		for chunk, iterErr := range gemini.Models.GenerateContentStream(ctx, env["VERTEX_MODEL"],
			genai.Text("Reply with one short word. Marker: "+r.marker), noThinking()) {
			if iterErr != nil {
				return iterErr
			}
			if chunk.UsageMetadata != nil {
				lastUsage = chunk.UsageMetadata
			}
			if chunk.ModelVersion != "" {
				lastModelVersion = chunk.ModelVersion
			}
			for _, candidate := range chunk.Candidates {
				if candidate.FinishReason != "" {
					finishReason = string(candidate.FinishReason)
				}
				if candidate.Content == nil {
					continue
				}
				for _, part := range candidate.Content.Parts {
					if !part.Thought {
						aggregated.WriteString(part.Text)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("vertex streaming call: %v", err)
	}
	if aggregated.Len() == 0 || lastUsage == nil {
		t.Fatal("inconclusive: the stream carried no comparable output or usage")
	}

	want := expectedObservation{
		Name:        "genai.generate_content_stream",
		Type:        "GENERATION",
		TraceID:     traceID,
		Usage:       vertexUsage(lastUsage),
		Output:      aggregated.String(),
		InputMarker: r.marker,
		Stream:      true,
		Metadata: map[string]string{
			"provider":      "google-vertex",
			"finish_reason": finishReason,
		},
	}
	vertexModelExpectation(&want, lastModelVersion, env["VERTEX_MODEL"])

	got := r.observation(t, traceID, "genai.generate_content_stream", ingested)
	checkObservation(t, got, want)
}

// TestVertexError is a token-free authenticated request for a model
// that cannot exist; the expected status comes from the SDK-reported
// code.
func TestVertexError(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "VERTEX_PROJECT", "VERTEX_LOCATION")
	gemini := vertexClient(t, r, env)
	invalid := "validation-invalid-" + r.marker

	traceID, err := r.call(t, "vertex-error", func(ctx context.Context) error {
		_, callErr := gemini.Models.GenerateContent(ctx, invalid,
			genai.Text("marker "+r.marker), noThinking())
		return callErr
	})
	if err == nil {
		t.Fatal("invalid model unexpectedly succeeded")
	}
	var apiErr genai.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected genai.APIError, got %T: %v", err, err)
	}

	got := r.observation(t, traceID, "genai.generate_content")
	checkObservation(t, got, expectedObservation{
		Name:        "genai.generate_content",
		Type:        "GENERATION",
		Model:       invalid, // the URL model is recorded even for failures
		TraceID:     traceID,
		InputMarker: r.marker,
		Status:      fmt.Sprintf("http %d", apiErr.Code),
		Metadata:    map[string]string{"provider": "google-vertex"},
	})
}
