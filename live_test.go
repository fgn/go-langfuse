//go:build live

package langfuse_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
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

	// The prompt referenced by the generation must round-trip through the
	// prompt-management read API, proving endpoint shape, decoding, and the
	// linkage between GetPrompt and PromptRef against a real deployment.
	livePrompt, err := client.GetPrompt(ctx, "go-langfuse-live-prompt",
		langfuse.PromptQuery{Version: 1})
	if err != nil {
		t.Fatalf("GetPrompt(): %v", err)
	}
	if livePrompt.Name != "go-langfuse-live-prompt" || livePrompt.Version != 1 {
		t.Fatalf("GetPrompt() = %+v, want the seeded live prompt version 1", livePrompt)
	}
	if livePrompt.Type == langfuse.PromptTypeText && livePrompt.Text == "" {
		t.Fatal("GetPrompt() returned an empty text prompt body")
	}
	if ref := livePrompt.Ref(); ref == nil ||
		*ref != (langfuse.PromptRef{Name: "go-langfuse-live-prompt", Version: 1}) {
		t.Fatalf("GetPrompt().Ref() = %+v, want the generation's prompt reference", livePrompt.Ref())
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
	if got := readBack.modelName(); got != "synthetic-model" {
		t.Errorf("read-back model = %q, want synthetic-model", got)
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

	trace := api.awaitTrace(t, deadline, root.TraceID(), root.ID())
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
	ProvidedModelName   string             `json:"providedModelName"`
	CompletionStartTime string             `json:"completionStartTime"`
	EndTime             string             `json:"endTime"`
	UsageDetails        map[string]float64 `json:"usageDetails"`
	Metadata            map[string]any     `json:"metadata"`
	Input               any                `json:"input"`
	Output              any                `json:"output"`
	IsRootObservation   bool               `json:"isRootObservation"`
}

func (observation liveObservation) modelName() string {
	if observation.ProvidedModelName != "" {
		return observation.ProvidedModelName
	}
	return observation.Model
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
	if scope, ok := metadata["scope"].(map[string]any); ok {
		if got := scope["name"]; got != "langfuse-sdk.go" {
			t.Errorf("read-back %s metadata scope name = %#v, want langfuse-sdk.go", subject, got)
		}
		return
	}
	if got := metadata["scope.name"]; got != "langfuse-sdk.go" {
		t.Errorf("read-back %s metadata scope name = %#v, want langfuse-sdk.go", subject, got)
	}
}

type liveAPI struct {
	baseURL       string
	authorization string
	client        *http.Client
	pollInterval  time.Duration
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

// awaitGeneration polls the v4 observations API until the named generation is
// ingested or the deadline passes. A 404 before the first successful v2 read
// selects the legacy observation route for the rest of the poll.
func (api *liveAPI) awaitGeneration(t *testing.T, deadline time.Time, traceID, name string) liveObservation {
	t.Helper()
	v2Route := api.observationsRoute(traceID)
	legacyRoute := api.baseURL + "/api/public/observations?traceId=" + url.QueryEscape(traceID)
	route := v2Route
	routeSelected := false
	for {
		var page struct {
			Data []liveObservation `json:"data"`
		}
		status, err := api.getJSON(deadline, route, &page)
		if route == v2Route && status == http.StatusOK {
			routeSelected = true
		}
		if err == nil && status == http.StatusOK {
			for _, observation := range page.Data {
				if observation.Name == name {
					return observation
				}
			}
		}
		if err == nil && status == http.StatusNotFound && route == v2Route && !routeSelected {
			route = legacyRoute
			routeSelected = true
			continue
		}
		if err == nil && status != http.StatusOK && status != http.StatusNotFound {
			t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", route, status)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("observation %q for trace %s was not visible through %s within the read-back deadline (last status %d, err %v)",
				name, traceID, route, status, err)
		}
		api.waitForNextPoll()
	}
}

func (api *liveAPI) observationsRoute(traceID string) string {
	query := url.Values{}
	query.Set("traceId", traceID)
	query.Set("fields", "core,basic,time,io,metadata,model,usage,prompt,trace_context")
	return api.baseURL + "/api/public/v2/observations?" + query.Encode()
}

// awaitScore polls the current scores API until the named score is ingested or
// the deadline passes. A 404 before the first successful v3 read selects the
// legacy v2 route for the rest of the poll.
func (api *liveAPI) awaitScore(t *testing.T, deadline time.Time, traceID, name string) liveScore {
	t.Helper()
	v3Route := api.baseURL + "/api/public/v3/scores?traceId=" + url.QueryEscape(traceID)
	v2Route := api.baseURL + "/api/public/v2/scores?traceId=" + url.QueryEscape(traceID)
	route := v3Route
	routeSelected := false
	for {
		var page struct {
			Data []liveScore `json:"data"`
		}
		status, err := api.getJSON(deadline, route, &page)
		if route == v3Route && status == http.StatusOK {
			routeSelected = true
		}
		if err == nil && status == http.StatusOK {
			for _, score := range page.Data {
				if score.Name == name {
					return score
				}
			}
		}
		if err == nil && status == http.StatusNotFound && route == v3Route && !routeSelected {
			route = v2Route
			routeSelected = true
			continue
		}
		if err == nil && status != http.StatusOK && status != http.StatusNotFound {
			t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", route, status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("score %q for trace %s was not visible through %s within the read-back deadline (last status %d, err %v)",
				name, traceID, route, status, err)
		}
		api.waitForNextPoll()
	}
}

// awaitTrace reads trace IO from the root observation on v4. A 404 before the
// first successful v2 read selects the legacy trace route for the rest of the
// poll.
func (api *liveAPI) awaitTrace(t *testing.T, deadline time.Time, traceID, rootID string) liveTrace {
	t.Helper()
	observationRoute := api.observationsRoute(traceID)
	legacyRoute := api.baseURL + "/api/public/traces/" + url.PathEscape(traceID)
	v2Selected := false
	legacySelected := false
	for {
		if legacySelected {
			var trace liveTrace
			status, err := api.getJSON(deadline, legacyRoute, &trace)
			if err == nil && status == http.StatusOK && trace.Input != nil && trace.Output != nil {
				return trace
			}
			if err == nil && status != http.StatusOK && status != http.StatusNotFound {
				t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", legacyRoute, status)
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("trace %s IO was not visible through %s within the read-back deadline (last status %d, err %v)",
					traceID, legacyRoute, status, err)
			}
			api.waitForNextPoll()
			continue
		}

		var page struct {
			Data []liveObservation `json:"data"`
		}
		status, err := api.getJSON(deadline, observationRoute, &page)
		if status == http.StatusOK {
			v2Selected = true
		}
		if err == nil && status == http.StatusOK {
			for _, observation := range page.Data {
				if observation.ID == rootID {
					if observation.Input != nil && observation.Output != nil {
						if !observation.IsRootObservation {
							t.Fatalf("root observation %s for trace %s is not marked as an application root", rootID, traceID)
						}
						return liveTrace{
							ID:       traceID,
							Input:    observation.Input,
							Output:   observation.Output,
							Metadata: observation.Metadata,
						}
					}
				}
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("trace %s IO was not visible through %s within the read-back deadline (last status %d, err %v)",
					traceID, observationRoute, status, err)
			}
			api.waitForNextPoll()
			continue
		} else if err == nil && status != http.StatusNotFound {
			t.Fatalf("GET %s returned unexpected status %d; check credentials and deployment", observationRoute, status)
		}
		if err == nil && status == http.StatusNotFound && !v2Selected {
			legacySelected = true
			continue
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("trace %s IO was not visible through %s within the read-back deadline (last status %d, err %v)",
				traceID, observationRoute, status, err)
		}
		api.waitForNextPoll()
	}
}

func (api *liveAPI) waitForNextPoll() {
	interval := api.pollInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	time.Sleep(interval)
}

func TestLiveReadbackLocksV2AfterSuccess(t *testing.T) {
	var v2Calls atomic.Int32
	var legacyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/public/v2/observations":
			switch v2Calls.Add(1) {
			case 1:
				_, _ = io.WriteString(writer, `{"data":[]}`)
			case 2:
				_, _ = io.WriteString(writer, `{`)
			default:
				_, _ = io.WriteString(writer, `{"data":[{"name":"generation","providedModelName":"synthetic-model"}]}`)
			}
		case "/api/public/observations":
			legacyCalls.Add(1)
			_, _ = io.WriteString(writer, `{"data":[{"name":"generation","model":"legacy-model"}]}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	api := &liveAPI{baseURL: server.URL, client: server.Client(), pollInterval: time.Millisecond}
	observation := api.awaitGeneration(t, time.Now().Add(time.Second), "trace-id", "generation")
	if got := observation.modelName(); got != "synthetic-model" {
		t.Fatalf("modelName() = %q, want synthetic-model", got)
	}
	if got := v2Calls.Load(); got != 3 {
		t.Errorf("v2 calls = %d, want 3", got)
	}
	if got := legacyCalls.Load(); got != 0 {
		t.Errorf("legacy calls = %d, want 0", got)
	}
}

func TestLiveReadbackFallsBackAfterInitialV2NotFound(t *testing.T) {
	var v2Calls atomic.Int32
	var legacyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/public/v2/observations":
			v2Calls.Add(1)
			http.NotFound(writer, request)
		case "/api/public/observations":
			legacyCalls.Add(1)
			_, _ = io.WriteString(writer, `{"data":[{"name":"generation","model":"legacy-model"}]}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	api := &liveAPI{baseURL: server.URL, client: server.Client(), pollInterval: time.Millisecond}
	observation := api.awaitGeneration(t, time.Now().Add(time.Second), "trace-id", "generation")
	if got := observation.modelName(); got != "legacy-model" {
		t.Fatalf("modelName() = %q, want legacy-model", got)
	}
	if got := v2Calls.Load(); got != 1 {
		t.Errorf("v2 calls = %d, want 1", got)
	}
	if got := legacyCalls.Load(); got != 1 {
		t.Errorf("legacy calls = %d, want 1", got)
	}
}

func TestLiveReadbackFallsBackToV2Scores(t *testing.T) {
	var v3Calls atomic.Int32
	var v2Calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/public/v3/scores":
			v3Calls.Add(1)
			http.NotFound(writer, request)
		case "/api/public/v2/scores":
			v2Calls.Add(1)
			_, _ = io.WriteString(writer, `{"data":[{"name":"score","value":1,"timestamp":"2026-08-14T00:00:00Z"}]}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	api := &liveAPI{baseURL: server.URL, client: server.Client(), pollInterval: time.Millisecond}
	score := api.awaitScore(t, time.Now().Add(time.Second), "trace-id", "score")
	if score.Value != 1 {
		t.Fatalf("score value = %v, want 1", score.Value)
	}
	if got := v3Calls.Load(); got != 1 {
		t.Errorf("v3 calls = %d, want 1", got)
	}
	if got := v2Calls.Load(); got != 1 {
		t.Errorf("v2 calls = %d, want 1", got)
	}
}

func TestLiveReadbackLocksV2TraceAndMatchesExactRoot(t *testing.T) {
	var v2Calls atomic.Int32
	var legacyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/public/v2/observations":
			switch v2Calls.Add(1) {
			case 1:
				_, _ = io.WriteString(writer, `{"data":[]}`)
			case 2:
				_, _ = io.WriteString(writer, `{`)
			default:
				_, _ = io.WriteString(writer, `{"data":[{"id":"other-root","isRootObservation":true,"input":"wrong","output":"wrong"},{"id":"root-id","isRootObservation":true,"input":"synthetic question","output":"synthetic answer"}]}`)
			}
		case "/api/public/traces/trace-id":
			legacyCalls.Add(1)
			_, _ = io.WriteString(writer, `{"id":"trace-id","input":"legacy","output":"legacy"}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	api := &liveAPI{baseURL: server.URL, client: server.Client(), pollInterval: time.Millisecond}
	trace := api.awaitTrace(t, time.Now().Add(time.Second), "trace-id", "root-id")
	if trace.Input != "synthetic question" || trace.Output != "synthetic answer" {
		t.Fatalf("trace IO = (%#v, %#v), want exact root IO", trace.Input, trace.Output)
	}
	if got := v2Calls.Load(); got != 3 {
		t.Errorf("v2 calls = %d, want 3", got)
	}
	if got := legacyCalls.Load(); got != 0 {
		t.Errorf("legacy calls = %d, want 0", got)
	}
}

func TestLiveReadbackFallsBackToLegacyTrace(t *testing.T) {
	var v2Calls atomic.Int32
	var legacyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/public/v2/observations":
			v2Calls.Add(1)
			http.NotFound(writer, request)
		case "/api/public/traces/trace-id":
			legacyCalls.Add(1)
			_, _ = io.WriteString(writer, `{"id":"trace-id","input":"synthetic question","output":"synthetic answer"}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	api := &liveAPI{baseURL: server.URL, client: server.Client(), pollInterval: time.Millisecond}
	trace := api.awaitTrace(t, time.Now().Add(time.Second), "trace-id", "root-id")
	if trace.Input != "synthetic question" || trace.Output != "synthetic answer" {
		t.Fatalf("trace IO = (%#v, %#v), want legacy trace IO", trace.Input, trace.Output)
	}
	if got := v2Calls.Load(); got != 1 {
		t.Errorf("v2 calls = %d, want 1", got)
	}
	if got := legacyCalls.Load(); got != 1 {
		t.Errorf("legacy calls = %d, want 1", got)
	}
}

func (api *liveAPI) getJSON(deadline time.Time, route string, into any) (int, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, route, nil)
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
