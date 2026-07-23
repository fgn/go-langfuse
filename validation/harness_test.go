//go:build validation

// Package validation is the opt-in real-provider round-trip suite: a
// real model call through the adapters, a real Langfuse ingestion, and
// assertions on the trace read back through the public API, with the
// provider SDK's own response as ground truth. No provider mocks. Runs
// only via `task validate`, never in `task ci`.
package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// run owns one normalized Langfuse configuration, used by both the
// exporter and the readback poller so they can never disagree.
type run struct {
	lf        *langfuse.Client
	baseURL   string
	publicKey string
	secretKey string
	marker    string
}

// newRun skips (listing every missing variable) unless the Langfuse
// configuration is complete, and fails by name on settings that would
// make validation nondeterministic or impossible.
func newRun(t *testing.T) *run {
	t.Helper()
	var missing []string
	for _, name := range []string{"LANGFUSE_BASE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Skipf("validation skipped; missing %s", strings.Join(missing, ", "))
	}
	// One normalized configuration feeds both the exporter and the
	// readback poller: typed values, not raw environment strings, so
	// case variants and endpoint forms cannot make them disagree.
	cfg := langfuse.ConfigFromEnv()
	if cfg.Disabled {
		t.Fatal("LANGFUSE_TRACING_ENABLED disables tracing; validation is impossible")
	}
	if cfg.DisableContentCapture {
		t.Fatal("LANGFUSE_CONTENT_CAPTURE_ENABLED removes the fields under validation")
	}
	if cfg.SampleRate != nil && *cfg.SampleRate != 1 {
		t.Fatalf("LANGFUSE_SAMPLE_RATE=%v makes validation nondeterministic; unset it or use 1", *cfg.SampleRate)
	}

	lf, err := langfuse.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create Langfuse client: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = lf.Shutdown(ctx)
	})
	return &run{
		lf:        lf,
		baseURL:   publicAPIBase(cfg.BaseURL),
		publicKey: cfg.PublicKey,
		secretKey: cfg.SecretKey,
		marker:    fmt.Sprintf("validation-%d", time.Now().UTC().UnixNano()),
	}
}

// publicAPIBase derives the public-API host root from any base URL
// form the core accepts: a host root, /api/public/otel, or the full
// traces endpoint.
func publicAPIBase(baseURL string) string {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	base = strings.TrimSuffix(base, "/v1/traces")
	base = strings.TrimSuffix(base, "/api/public/otel")
	return strings.TrimRight(base, "/")
}

// requireEnv skips the calling test with every missing name listed.
func requireEnv(t *testing.T, names ...string) map[string]string {
	t.Helper()
	values := map[string]string{}
	var missing []string
	for _, name := range names {
		value := os.Getenv(name)
		if value == "" {
			missing = append(missing, name)
			continue
		}
		values[name] = value
	}
	if len(missing) > 0 {
		t.Skipf("skipped; missing %s", strings.Join(missing, ", "))
	}
	return values
}

// rejectRepoPath fails when a configured credential path resolves
// inside the repository checkout: credentials must live outside it.
func rejectRepoPath(t *testing.T, name, path string) {
	t.Helper()
	// Absolute-then-resolved on BOTH sides, failing closed: a Rel
	// error must reject, never accept.
	abs, err := filepath.Abs(path)
	if err == nil {
		abs, err = filepath.EvalSymlinks(abs)
	}
	if err != nil {
		t.Fatalf("%s cannot be resolved; configure an existing path outside the checkout", name)
	}
	repo, err := filepath.Abs("..")
	if err == nil {
		repo, err = filepath.EvalSymlinks(repo)
	}
	if err != nil {
		t.Fatal("repository root cannot be resolved")
	}
	rel, err := filepath.Rel(repo, abs)
	if err != nil || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		t.Fatalf("%s resolves inside the repository (or could not be proven outside); keep credentials outside the checkout", name)
	}
}

// call wraps one provider invocation in a uniquely named core span and
// returns that span's trace ID: readback correlation is always by
// trace ID, never by trace-name merging. The same wrapper serves
// success and error cases; fn's error is returned, not fatal, so
// error-path tests can assert on it.
func (r *run) call(t *testing.T, name string, fn func(ctx context.Context) error) (string, error) {
	t.Helper()
	caseName := fmt.Sprintf("%s-%s", name, r.marker)
	ctx := r.lf.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:      caseName,
		UserID:    "validation",
		SessionID: r.marker,
		Tags:      []string{"validation"},
	})
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var traceID string
	err := r.lf.Observe(ctx, caseName, langfuse.TypeSpan, langfuse.ObservationAttributes{},
		func(ctx context.Context, span *langfuse.Observation) error {
			traceID = span.TraceID()
			return fn(ctx)
		})
	if traceID == "" {
		t.Fatal("no trace ID; is the Langfuse client disabled?")
	}
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFlush()
	if flushErr := r.lf.Flush(flushCtx); flushErr != nil {
		t.Fatalf("flush: %v", flushErr)
	}
	return traceID, err
}

// observation is the minimal typed readback projection.
type observation struct {
	ID                  string             `json:"id"`
	TraceID             string             `json:"traceId"`
	Name                string             `json:"name"`
	Type                string             `json:"type"`
	Model               *string            `json:"model"`
	Input               json.RawMessage    `json:"input"`
	Output              json.RawMessage    `json:"output"`
	StartTime           time.Time          `json:"startTime"`
	EndTime             *time.Time         `json:"endTime"`
	CompletionStartTime *time.Time         `json:"completionStartTime"`
	StatusMessage       string             `json:"statusMessage"`
	Level               string             `json:"level"`
	UsageDetails        map[string]int64   `json:"usageDetails"`
	CostDetails         map[string]float64 `json:"costDetails"`
	TotalCost           float64            `json:"totalCost"`
	CalculatedTotalCost float64            `json:"calculatedTotalCost"`
	Metadata            map[string]any     `json:"metadata"`
}

// observation polls the public API for the trace until exactly one
// adapter observation with the wanted name is present, ended, and
// satisfying every supplied readiness predicate (ingestion fills
// usage, output, and cost asynchronously). Budget: one 90-second
// deadline governing everything, per-request timeout min(15s,
// remaining), immediate first attempt then 3-second waits capped by
// the remaining budget; no request ever starts after expiry. Any
// status other than 200 or 404 fails immediately.
func (r *run) observation(t *testing.T, traceID, name string, ready ...func(observation) bool) observation {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	lastState := "no attempt"
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			wait := 3 * time.Second
			if remaining := time.Until(deadline); remaining < wait {
				t.Fatalf("observation %q in trace %s not ready within 90s; last state: %s", name, traceID, lastState)
			}
			time.Sleep(wait)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("observation %q in trace %s not ready within 90s; last state: %s", name, traceID, lastState)
		}
		client := &http.Client{Timeout: min(15*time.Second, remaining)}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/api/public/traces/"+traceID, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(r.publicKey, r.secretKey)
		resp, err := client.Do(req)
		if err != nil {
			lastState = "request error"
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
		case http.StatusNotFound:
			lastState = "404 not ingested yet"
			continue
		default:
			t.Fatalf("readback status %d for trace %s", resp.StatusCode, traceID)
		}
		var trace struct {
			Observations []observation `json:"observations"`
		}
		if err := json.Unmarshal(body, &trace); err != nil {
			t.Fatalf("decode trace: %v", err)
		}
		var matches []observation
		for _, obs := range trace.Observations {
			if obs.Name == name {
				matches = append(matches, obs)
			}
		}
		switch len(matches) {
		case 0:
			lastState = fmt.Sprintf("trace present, %d observations, none named %q", len(trace.Observations), name)
			continue
		case 1:
			if matches[0].EndTime == nil {
				lastState = "observation present but not ended"
				continue
			}
			pending := false
			for _, predicate := range ready {
				if !predicate(matches[0]) {
					pending = true
					break
				}
			}
			if pending {
				lastState = "observation ended but required fields not yet ingested"
				continue
			}
			return matches[0]
		default:
			t.Fatalf("expected exactly one observation named %q in trace %s, found %d", name, traceID, len(matches))
		}
	}
}

// expectedObservation is the single cross-provider assertion contract,
// populated by each provider test from its own SDK response.
type expectedObservation struct {
	Name         string
	Type         string
	Model        string // "" asserts the model field is absent or null
	RequestModel string // expected metadata only when != Model
	TraceID      string // identity: the correlated trace
	// Usage is compared deeply in BOTH directions: extra readback
	// buckets fail exactly like missing ones.
	Usage map[string]int64
	// Output is the normalized SDK output. Strings compare exactly;
	// objects compare whole after normalization (null values and empty
	// collections stripped from both sides), so extra meaningful
	// fields fail while wire filler like refusal:null does not.
	Output      any
	InputMarker string // must appear in the recorded input
	Status      string // "" for success
	Metadata    map[string]string
	Stream      bool // requires start <= completionStart <= end
}

// checkObservation compares a readback against the SDK-derived
// expectation. Timing checks are ordering and presence only.
func checkObservation(t *testing.T, got observation, want expectedObservation) {
	t.Helper()
	if got.Name != want.Name {
		t.Errorf("name = %q, want %q", got.Name, want.Name)
	}
	if !strings.EqualFold(got.Type, want.Type) {
		t.Errorf("type = %q, want %q", got.Type, want.Type)
	}
	if got.ID == "" {
		t.Error("observation ID missing from readback")
	}
	if want.TraceID != "" && got.TraceID != want.TraceID {
		t.Errorf("traceId = %q, want correlated %q", got.TraceID, want.TraceID)
	}
	gotModel := ""
	if got.Model != nil {
		gotModel = *got.Model
	}
	if gotModel != want.Model {
		t.Errorf("model = %q, want %q", gotModel, want.Model)
	}
	if want.RequestModel != "" && want.RequestModel != want.Model {
		if meta, _ := got.Metadata["request_model"].(string); meta != want.RequestModel {
			t.Errorf("metadata request_model = %q, want %q", meta, want.RequestModel)
		}
	}
	if want.Usage != nil {
		for bucket, wantCount := range want.Usage {
			gotCount, present := got.UsageDetails[bucket]
			if !present {
				t.Errorf("usage[%s] missing (want %d)", bucket, wantCount)
				continue
			}
			if gotCount != wantCount {
				t.Errorf("usage[%s] = %d, want %d", bucket, gotCount, wantCount)
			}
		}
		for bucket, gotCount := range got.UsageDetails {
			if _, expected := want.Usage[bucket]; !expected {
				t.Errorf("unexpected usage bucket %s = %d (want exactly %v)", bucket, gotCount, want.Usage)
			}
		}
	}
	if want.Output != nil {
		var gotValue any
		if err := json.Unmarshal(got.Output, &gotValue); err != nil {
			t.Errorf("output decode: %v", err)
		} else {
			gotNorm, _ := json.Marshal(stripEmpty(gotValue))
			wantJSON, err := json.Marshal(want.Output)
			if err != nil {
				t.Fatal(err)
			}
			var wantValue any
			_ = json.Unmarshal(wantJSON, &wantValue)
			wantNorm, _ := json.Marshal(stripEmpty(wantValue))
			if string(gotNorm) != string(wantNorm) {
				t.Errorf("output = %s, want %s", gotNorm, wantNorm)
			}
		}
	}
	if want.InputMarker != "" && !strings.Contains(string(got.Input), want.InputMarker) {
		t.Errorf("input %s does not contain marker %q", got.Input, want.InputMarker)
	}
	if got.StatusMessage != want.Status {
		t.Errorf("status = %q, want %q", got.StatusMessage, want.Status)
	}
	for key, wantValue := range want.Metadata {
		if gotValue := fmt.Sprintf("%v", got.Metadata[key]); gotValue != wantValue {
			t.Errorf("metadata[%s] = %q, want %q", key, gotValue, wantValue)
		}
	}
	if got.EndTime == nil {
		t.Error("end time missing")
	} else if !got.StartTime.Before(*got.EndTime) && !got.StartTime.Equal(*got.EndTime) {
		t.Errorf("start %v not <= end %v", got.StartTime, got.EndTime)
	}
	if want.Stream {
		switch {
		case got.CompletionStartTime == nil:
			t.Error("stream lacks completion start time")
		case got.CompletionStartTime.Before(got.StartTime):
			t.Errorf("completion start %v before start %v", got.CompletionStartTime, got.StartTime)
		case got.EndTime != nil && got.CompletionStartTime.After(*got.EndTime):
			t.Errorf("completion start %v after end %v", got.CompletionStartTime, got.EndTime)
		}
	}
}

// stripEmpty removes null values and empty collections recursively so
// whole-object comparison ignores wire filler (refusal: null,
// annotations: []) while every meaningful field still participates.
func stripEmpty(value any) any {
	switch value := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for key, item := range value {
			cleaned := stripEmpty(item)
			if cleaned == nil {
				continue
			}
			if m, ok := cleaned.(map[string]any); ok && len(m) == 0 {
				continue
			}
			if a, ok := cleaned.([]any); ok && len(a) == 0 {
				continue
			}
			out[key] = cleaned
		}
		return out
	case []any:
		out := make([]any, 0, len(value))
		for _, item := range value {
			out = append(out, stripEmpty(item))
		}
		return out
	default:
		return value
	}
}

// TestHarnessSelfCheck proves the flush/poll/decode plumbing with one
// core-SDK observation and no provider call, so a provider failure is
// never confused with a harness failure.
func TestHarnessSelfCheck(t *testing.T) {
	r := newRun(t)
	traceID, err := r.call(t, "selfcheck", func(ctx context.Context) error {
		return r.lf.Observe(ctx, "selfcheck-inner", langfuse.TypeGeneration,
			langfuse.ObservationAttributes{
				Input: "selfcheck input " + r.marker,
				Model: "selfcheck-model",
			},
			func(ctx context.Context, o *langfuse.Observation) error {
				o.Update(langfuse.ObservationAttributes{
					Output: "selfcheck output",
					Usage:  &langfuse.Usage{InputTokens: 3, OutputTokens: 2},
				})
				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}
	got := r.observation(t, traceID, "selfcheck-inner", func(obs observation) bool {
		return len(obs.UsageDetails) > 0 && len(obs.Output) > 0
	})
	checkObservation(t, got, expectedObservation{
		Name:        "selfcheck-inner",
		Type:        "GENERATION",
		Model:       "selfcheck-model",
		TraceID:     traceID,
		Usage:       map[string]int64{"input": 3, "output": 2, "total": 5},
		Output:      "selfcheck output",
		InputMarker: r.marker,
	})
}
