//go:build live

package langfuse_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/internal/transport"
)

// TestLiveCompatibility is opt-in, writes one synthetic trace to the
// configured project, and reads it back through the public REST API. It
// deliberately never uses production-derived content.
// Run with: go test -count=1 -tags=live -run TestLiveCompatibility -v .
func TestLiveCompatibility(t *testing.T) {
	if os.Getenv("LANGFUSE_PUBLIC_KEY") == "" || os.Getenv("LANGFUSE_SECRET_KEY") == "" {
		t.Fatal("live Langfuse credentials are required; refusing to pass without exporting")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := langfuse.ConfigFromEnv()
	if config.Disabled {
		t.Fatal("LANGFUSE_TRACING_ENABLED disables tracing; refusing to pass without exporting")
	}
	if config.DisableContentCapture {
		t.Fatal("LANGFUSE_CONTENT_CAPTURE_ENABLED disables content; the live compatibility fixture must exercise content ingestion")
	}
	client, err := langfuse.New(ctx, config)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := client.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
	}()

	runMarker := fmt.Sprintf("go-langfuse-live-%d", time.Now().UTC().UnixNano())
	ctx = client.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name:      runMarker,
		UserID:    "synthetic-live-user",
		SessionID: runMarker,
		Tags:      []string{"go-langfuse", "live-compatibility"},
		Metadata:  map[string]any{"synthetic": true, "sdk": "go-langfuse", "run_marker": runMarker},
		Version:   "v0.1-live",
	})
	ctx, root := client.StartObservation(ctx, "live-root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{Input: "synthetic question"})
	if root.TraceID() == "" || root.ID() == "" {
		t.Fatal("live root is not recording; refusing to pass without exporting")
	}
	// The generation reproduces the retroactive-instrumentation timeline:
	// explicit start, first-token, and EndAt times.
	generationStart := time.Now().UTC().Add(-3 * time.Second).Truncate(time.Millisecond)
	completionStart := generationStart.Add(500 * time.Millisecond)
	generationEnd := generationStart.Add(1500 * time.Millisecond)
	_, generation := client.StartObservation(ctx, "live-generation", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{
			Input:               "synthetic prompt",
			Model:               "synthetic-model",
			Prompt:              &langfuse.PromptRef{Name: "go-langfuse-live-prompt", Version: 1},
			StartTime:           generationStart,
			CompletionStartTime: completionStart,
		})
	if generation.TraceID() == "" || generation.ID() == "" {
		t.Fatal("live generation is not recording; refusing to pass without exporting")
	}
	generation.Update(langfuse.ObservationAttributes{
		Output: "synthetic answer",
		Usage: &langfuse.Usage{
			InputTokens:           12,
			OutputTokens:          7,
			CacheReadInputTokens:  2,
			ReasoningOutputTokens: 1,
			Details:               map[string]int64{"input_audio_tokens": 1},
		},
		CostDetails: map[string]float64{"input": 0.0001, "output": 0.0002},
	})
	generation.EndAt(generationEnd)
	root.Update(langfuse.ObservationAttributes{Output: "synthetic answer"})
	root.End()

	scoreValue := 1.0
	backdated := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	if err := client.RecordScore(ctx, langfuse.Score{
		Name:         "go-langfuse-live-score",
		TraceID:      root.TraceID(),
		NumericValue: &scoreValue,
		Comment:      "synthetic feedback",
		Timestamp:    backdated,
	}); err != nil {
		t.Fatalf("RecordScore(): %v", err)
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	t.Logf("synthetic trace exported; run_marker=%s trace_id=%s root_observation_id=%s", runMarker, root.TraceID(), root.ID())

	api := newLiveAPI(t, config.BaseURL)
	deadline := time.Now().Add(90 * time.Second)

	readBack := api.awaitGeneration(t, deadline, root.TraceID(), "live-generation")
	if !strings.EqualFold(readBack.Type, "generation") {
		t.Errorf("read-back observation type = %q, want generation", readBack.Type)
	}
	if readBack.Model != "synthetic-model" {
		t.Errorf("read-back model = %q, want synthetic-model", readBack.Model)
	}
	assertSDKMetadata(t, "observation", readBack.Metadata)
	// The SDK normalizes the inclusive Usage fields sent above (input 12 with
	// 2 cached and 1 audio, output 7 with 1 reasoning) to exclusive buckets.
	for bucket, want := range map[string]float64{"input": 9, "output": 6, "total": 19} {
		got, exists := readBack.UsageDetails[bucket]
		if !exists || got != want {
			t.Errorf("read-back usage_details[%q] = (%v, %v), want %v", bucket, got, exists, want)
		}
	}

	assertLiveTime(t, "generation completion start", readBack.CompletionStartTime, completionStart)
	assertLiveTime(t, "generation end", readBack.EndTime, generationEnd)

	trace := api.awaitTrace(t, deadline, root.TraceID())
	if got, want := trace.Input, any("synthetic question"); got != want {
		t.Errorf("read-back trace input = %#v, want root observation input %#v", got, want)
	}
	if got, want := trace.Output, any("synthetic answer"); got != want {
		t.Errorf("read-back trace output = %#v, want root observation output %#v", got, want)
	}
	assertSDKMetadata(t, "trace", trace.Metadata)

	score := api.awaitScore(t, deadline, root.TraceID(), "go-langfuse-live-score")
	if score.Value != scoreValue {
		t.Errorf("read-back score value = %v, want %v", score.Value, scoreValue)
	}
	// The ingestion event envelope carries the score timestamp, so the
	// backdated time must survive the round trip exactly.
	assertLiveTime(t, "score timestamp", score.Timestamp, backdated)
}

type liveObservation struct {
	ID                  string             `json:"id"`
	TraceID             string             `json:"traceId"`
	Name                string             `json:"name"`
	Type                string             `json:"type"`
	Model               string             `json:"model"`
	CompletionStartTime string             `json:"completionStartTime"`
	EndTime             string             `json:"endTime"`
	UsageDetails        map[string]float64 `json:"usageDetails"`
	Metadata            map[string]any     `json:"metadata"`
}

type liveScore struct {
	ID        string  `json:"id"`
	TraceID   string  `json:"traceId"`
	Name      string  `json:"name"`
	Value     float64 `json:"value"`
	Timestamp string  `json:"timestamp"`
}

// assertLiveTime compares a read-back ISO timestamp against the exact time
// the SDK exported; both sides are millisecond-precision.
func assertLiveTime(t *testing.T, subject, got string, want time.Time) {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Errorf("read-back %s time = %q, want a parsable timestamp: %v", subject, got, err)
		return
	}
	if !parsed.Equal(want) {
		t.Errorf("read-back %s time = %v, want %v", subject, parsed, want)
	}
}

type liveTrace struct {
	ID       string         `json:"id"`
	Input    any            `json:"input"`
	Output   any            `json:"output"`
	Metadata map[string]any `json:"metadata"`
}

func assertSDKMetadata(t *testing.T, subject string, metadata map[string]any) {
	t.Helper()
	if _, duplicated := metadata["attributes"]; duplicated {
		t.Errorf("read-back %s metadata redundantly contains semantic attributes", subject)
	}
	scope, ok := metadata["scope"].(map[string]any)
	if !ok {
		t.Errorf("read-back %s metadata scope = %#v, want object", subject, metadata["scope"])
		return
	}
	if got := scope["name"]; got != "langfuse-sdk.go" {
		t.Errorf("read-back %s metadata scope name = %#v, want langfuse-sdk.go", subject, got)
	}
}

type liveAPI struct {
	baseURL       string
	authorization string
	client        *http.Client
}

// newLiveAPI derives the REST base URL from the same configuration the
// exporter uses so read-back always targets the deployment that ingested.
func newLiveAPI(t *testing.T, baseURL string) *liveAPI {
	t.Helper()
	endpoint, err := transport.NormalizeEndpoint(baseURL)
	if err != nil {
		t.Fatalf("normalize LANGFUSE_BASE_URL: %v", err)
	}
	return &liveAPI{
		baseURL: strings.TrimSuffix(endpoint, "/api/public/otel/v1/traces"),
		authorization: "Basic " + base64.StdEncoding.EncodeToString(
			[]byte(os.Getenv("LANGFUSE_PUBLIC_KEY")+":"+os.Getenv("LANGFUSE_SECRET_KEY")),
		),
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// awaitGeneration polls GET /api/public/observations?traceId=... until the
// named generation is ingested or the deadline passes.
func (api *liveAPI) awaitGeneration(t *testing.T, deadline time.Time, traceID, name string) liveObservation {
	t.Helper()
	route := api.baseURL + "/api/public/observations?traceId=" + url.QueryEscape(traceID)
	for {
		var page struct {
			Data []liveObservation `json:"data"`
		}
		status, err := api.getJSON(route, &page)
		if err == nil && status == http.StatusOK {
			for _, observation := range page.Data {
				if observation.Name == name {
					return observation
				}
			}
		}
		if err == nil && status != http.StatusOK && status != http.StatusNotFound {
			t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", route, status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("observation %q for trace %s was not visible through %s within the read-back deadline (last status %d, err %v)",
				name, traceID, route, status, err)
		}
		time.Sleep(3 * time.Second)
	}
}

// awaitScore polls GET /api/public/v3/scores?traceId=... until the named
// score is ingested or the deadline passes.
func (api *liveAPI) awaitScore(t *testing.T, deadline time.Time, traceID, name string) liveScore {
	t.Helper()
	route := api.baseURL + "/api/public/v3/scores?traceId=" + url.QueryEscape(traceID)
	for {
		var page struct {
			Data []liveScore `json:"data"`
		}
		status, err := api.getJSON(route, &page)
		if err == nil && status == http.StatusOK {
			for _, score := range page.Data {
				if score.Name == name {
					return score
				}
			}
		}
		if err == nil && status != http.StatusOK && status != http.StatusNotFound {
			t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", route, status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("score %q for trace %s was not visible through %s within the read-back deadline (last status %d, err %v)",
				name, traceID, route, status, err)
		}
		time.Sleep(3 * time.Second)
	}
}

// awaitTrace polls GET /api/public/traces/{traceID} until the trace carries
// the root observation's input and output as trace IO.
func (api *liveAPI) awaitTrace(t *testing.T, deadline time.Time, traceID string) liveTrace {
	t.Helper()
	route := api.baseURL + "/api/public/traces/" + url.PathEscape(traceID)
	for {
		var trace liveTrace
		status, err := api.getJSON(route, &trace)
		if err == nil && status == http.StatusOK && trace.Input != nil && trace.Output != nil {
			return trace
		}
		if err == nil && status != http.StatusOK && status != http.StatusNotFound {
			t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", route, status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace %s IO was not visible through %s within the read-back deadline (last status %d, err %v)",
				traceID, route, status, err)
		}
		time.Sleep(3 * time.Second)
	}
}

func (api *liveAPI) getJSON(route string, into any) (int, error) {
	request, err := http.NewRequest(http.MethodGet, route, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", api.authorization)
	response, err := api.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return response.StatusCode, err
	}
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, nil
	}
	return response.StatusCode, json.Unmarshal(body, into)
}
