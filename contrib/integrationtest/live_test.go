package integrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	openai "github.com/sashabaranov/go-openai"
	genai "google.golang.org/genai"

	"github.com/fgn/go-langfuse"
	langfusegenai "github.com/fgn/go-langfuse/contrib/googlegenai"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

// TestLiveAdapters is the opt-in end-to-end gate: synthetic provider
// servers, a real Langfuse deployment. It exports adapter-recorded
// observations through the configured Langfuse instance and reads them
// back through the public REST API. Run with LANGFUSE_BASE_URL,
// LANGFUSE_PUBLIC_KEY, and LANGFUSE_SECRET_KEY exported.
func TestLiveAdapters(t *testing.T) {
	if os.Getenv("LANGFUSE_PUBLIC_KEY") == "" || os.Getenv("LANGFUSE_SECRET_KEY") == "" {
		t.Skip("live Langfuse credentials not configured")
	}
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		t.Fatal(err)
	}

	marker := fmt.Sprintf("contrib-live-%d", time.Now().UTC().UnixNano())
	traceCtx := lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name: marker, Tags: []string{"contrib-live"},
	})

	// Instrumented go-openai streaming call against a synthetic server.
	openaiProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"choices":[{"index":0,"delta":{"content":"live "}}],"model":"synthetic-model-001"}`,
			`data: {"choices":[{"index":0,"delta":{"content":"answer"}}]}`,
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":6,"completion_tokens":2}}`,
			`data: [DONE]`,
		} {
			_, _ = io.WriteString(w, chunk+"\n\n")
			flusher.Flush()
		}
	}))
	defer openaiProvider.Close()

	oaiCfg := openai.DefaultConfig("sk-synthetic")
	oaiCfg.BaseURL = openaiProvider.URL + "/v1"
	oaiCfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	oaiClient := openai.NewClientWithConfig(oaiCfg)

	err = lf.Observe(traceCtx, marker, langfuse.TypeSpan, langfuse.ObservationAttributes{},
		func(ctx context.Context, _ *langfuse.Observation) error {
			stream, err := oaiClient.CreateChatCompletionStream(ctx, openai.ChatCompletionRequest{
				Model: "synthetic-model", Stream: true,
				StreamOptions: &openai.StreamOptions{IncludeUsage: true},
				Messages: []openai.ChatCompletionMessage{
					{Role: openai.ChatMessageRoleUser, Content: "synthetic live question"},
				},
			})
			if err != nil {
				return err
			}
			defer stream.Close()
			for {
				if _, err := stream.Recv(); err != nil {
					return nil //nolint:nilerr // io.EOF ends the stream
				}
			}
		})
	if err != nil {
		t.Fatal(err)
	}

	// Instrumented genai call against a synthetic Developer API server.
	genaiProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"candidates":[{"content":{"role":"model","parts":[{"text":"live gemini answer"}]},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":3},
			"modelVersion":"synthetic-gemini-001"
		}`)
	}))
	defer genaiProvider.Close()

	gemini, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend: genai.BackendGeminiAPI,
		APIKey:  "synthetic-key",
		HTTPClient: &http.Client{
			Transport: langfusegenai.NewTransport(lf, nil),
		},
		HTTPOptions: genai.HTTPOptions{BaseURL: genaiProvider.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gemini.Models.GenerateContent(traceCtx, "gemini-3.6-flash",
		genai.Text("synthetic live question"), nil); err != nil {
		t.Fatal(err)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := lf.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}

	// Read the trace back through the public REST API.
	observations := fetchObservations(t, marker)
	byName := map[string]map[string]any{}
	for _, observation := range observations {
		name, _ := observation["name"].(string)
		byName[name] = observation
	}
	attempt := byName["openai.chat.completions"]
	if attempt == nil {
		t.Fatalf("openai attempt missing; observations: %v", names(observations))
	}
	if attempt["model"] != "synthetic-model-001" || attempt["output"] != "live answer" {
		t.Fatalf("openai attempt fields: model=%v output=%v", attempt["model"], attempt["output"])
	}
	if attempt["completionStartTime"] == nil {
		t.Fatal("completion start time missing on streamed attempt")
	}
	usage, _ := attempt["usageDetails"].(map[string]any)
	if usage["input"] != 6.0 || usage["output"] != 2.0 {
		t.Fatalf("openai usage %v", usage)
	}
	logical := byName[marker]
	if logical == nil || logical["type"] != "SPAN" {
		t.Fatalf("logical span wrong: %v", logical)
	}
	if logical["usageDetails"] != nil {
		if usage, ok := logical["usageDetails"].(map[string]any); ok && len(usage) > 0 {
			t.Fatalf("logical span carries usage: %v", usage)
		}
	}
	geminiAttempt := byName["genai.generate_content"]
	if geminiAttempt == nil {
		t.Fatalf("genai attempt missing; observations: %v", names(observations))
	}
	if geminiAttempt["model"] != "synthetic-gemini-001" {
		t.Fatalf("genai model %v", geminiAttempt["model"])
	}
}

func names(observations []map[string]any) []string {
	var all []string
	for _, observation := range observations {
		name, _ := observation["name"].(string)
		all = append(all, name)
	}
	return all
}

// fetchObservations polls the public REST API until the marker trace
// and its observations are queryable (ClickHouse ingestion is
// asynchronous).
func fetchObservations(t *testing.T, marker string) []map[string]any {
	t.Helper()
	base := os.Getenv("LANGFUSE_BASE_URL")
	deadline := time.Now().Add(90 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("trace %s not queryable within 90s", marker)
		}
		time.Sleep(3 * time.Second)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			base+"/api/public/traces?name="+marker, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(os.Getenv("LANGFUSE_PUBLIC_KEY"), os.Getenv("LANGFUSE_SECRET_KEY"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var list struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &list) != nil || len(list.Data) == 0 {
			continue
		}
		// The genai attempt runs with no active parent span and
		// therefore correctly roots its own trace; merge observations
		// across every trace carrying the marker name.
		var merged []map[string]any
		for _, item := range list.Data {
			req, err = http.NewRequestWithContext(context.Background(), http.MethodGet,
				base+"/api/public/traces/"+item.ID, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.SetBasicAuth(os.Getenv("LANGFUSE_PUBLIC_KEY"), os.Getenv("LANGFUSE_SECRET_KEY"))
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				continue
			}
			body, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var trace struct {
				Observations []map[string]any `json:"observations"`
			}
			if json.Unmarshal(body, &trace) == nil {
				merged = append(merged, trace.Observations...)
			}
		}
		// The logical span plus two attempts; wait until all arrived.
		if len(merged) >= 3 {
			return merged
		}
	}
}
