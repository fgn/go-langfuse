package langfuse_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/fgn/go-langfuse"
)

func ingestionRequestCount(receiver *scoreWireReceiver) int {
	count := 0
	for _, request := range receiver.all() {
		if strings.HasSuffix(request.path, "/api/public/ingestion") {
			count++
		}
	}
	return count
}

func recordDeliveredScore(t *testing.T, client *langfuse.Client, receiver *scoreWireReceiver, ctx context.Context, score langfuse.Score, why string) {
	t.Helper()
	before := ingestionRequestCount(receiver)
	value := 1.0
	score.Name = "delivery-check"
	score.NumericValue = &value
	if err := client.RecordScore(ctx, score); err != nil {
		t.Fatalf("RecordScore (%s) error = %v", why, err)
	}
	flushClient(t, client)
	if got := ingestionRequestCount(receiver); got != before+1 {
		t.Fatalf("ingestion requests = %d after %s, want %d: this score must be delivered", got, why, before+1)
	}
}

func TestScoreSuppressedOnSampledOutAuthoritativePathWithOneDiagnostic(t *testing.T) {
	var diagnostics atomic.Int64
	restore := langfuse.SetTestErrorHandler(func(error) { diagnostics.Add(1) })
	defer restore()

	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0)
	})
	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	defer root.End()

	value := 1.0
	for range 2 {
		err := client.RecordScore(rootCtx, langfuse.Score{
			Name:         "quality",
			TraceID:      root.TraceID(),
			NumericValue: &value,
		})
		if err != nil {
			t.Fatalf("RecordScore() error = %v, want nil for a suppressed score", err)
		}
	}
	flushClient(t, client)
	if got := ingestionRequestCount(receiver); got != 0 {
		t.Fatalf("ingestion requests = %d, want 0: both scores target the sampled-out authoritative trace", got)
	}
	if got := diagnostics.Load(); got != 1 {
		t.Fatalf("suppression diagnostics = %d, want exactly 1 for the first suppression only", got)
	}
}

func TestScoreDeliveryOutsideTheSuppressionConditions(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0)
	})
	foreign := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = foreign.Shutdown(ctx)
	})

	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	defer root.End()

	recordDeliveredScore(t, client, receiver, rootCtx,
		langfuse.Score{SessionID: "session-1"},
		"a session-only score in a sampled-out context")
	recordDeliveredScore(t, client, receiver, rootCtx,
		langfuse.Score{TraceID: "0af7651916cd43dd8448eb211c80319c"},
		"an explicit score for a different trace")
	recordDeliveredScore(t, client, receiver, context.Background(),
		langfuse.Score{TraceID: root.TraceID()},
		"an out-of-context score from an offline job")

	// A trace with a remote parent is not SDK-originated: never authoritative.
	remote := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    oteltrace.TraceID{0x0a, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		SpanID:     oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})
	remoteRootCtx, remoteRoot := client.StartObservation(
		oteltrace.ContextWithSpanContext(context.Background(), remote), "remote-root",
		langfuse.TypeAgent, langfuse.ObservationAttributes{})
	defer remoteRoot.End()
	recordDeliveredScore(t, client, receiver, remoteRootCtx,
		langfuse.Score{TraceID: remoteRoot.TraceID()},
		"a score on a remote-parent trace the SDK did not originate")

	// A foreign hop breaks path authority: the foreign producer may have
	// exported part of the trace, so the score must be delivered.
	foreignChildCtx, foreignChild := foreign.Tracer("foreign.local").Start(rootCtx, "foreign-child")
	defer foreignChild.End()
	recordDeliveredScore(t, client, receiver, foreignChildCtx,
		langfuse.Score{TraceID: root.TraceID()},
		"a score recorded from a foreign child's context")

	// The downgrade is sticky: an SDK descendant under the foreign hop still
	// delivers, even though it is now the last SDK observation on its path.
	grandchildCtx, grandchild := client.StartObservation(foreignChildCtx, "grandchild",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{})
	defer grandchild.End()
	recordDeliveredScore(t, client, receiver, grandchildCtx,
		langfuse.Score{TraceID: root.TraceID()},
		"a score from an SDK descendant below a foreign hop")
}

func TestScoreValidationPrecedesSuppression(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) {
		config.SampleRate = rate(0)
	})
	rootCtx, root := client.StartObservation(context.Background(), "root", langfuse.TypeAgent,
		langfuse.ObservationAttributes{})
	defer root.End()

	err := client.RecordScore(rootCtx, langfuse.Score{
		Name:    "invalid",
		TraceID: root.TraceID(),
		// Exactly one of NumericValue or StringValue is required; neither is set.
	})
	if err == nil {
		t.Fatal("RecordScore() error = nil for an invalid score in a suppressed context, want the validation error")
	}
	flushClient(t, client)
	if got := ingestionRequestCount(receiver); got != 0 {
		t.Fatalf("ingestion requests = %d, want 0", got)
	}
}

func TestBorrowedModeNeverSuppressesScores(t *testing.T) {
	t.Parallel()
	receiver := &scoreWireReceiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)

	// A per-span application sampler that drops the SDK span while keeping
	// the foreign root: the round-1 false-positive scenario. The Langfuse
	// processor exports the gen_ai-attributed foreign root, so the trace
	// exists remotely and its score must be delivered.
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(dropSDKSpansSampler{}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	})
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:        server.URL,
		PublicKey:      "pk-lf-borrowed-scores",
		SecretKey:      "sk-lf-borrowed-scores",
		TracerProvider: provider,
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})

	rootCtx, foreignRoot := provider.Tracer("app.instrumentor").Start(context.Background(),
		"llm-request", oteltrace.WithAttributes(attribute.String("gen_ai.system", "test")))
	childCtx, child := client.StartObservation(rootCtx, "dropped-child",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{})
	defer child.End()
	foreignRoot.End()
	if child.Sampled() {
		t.Fatal("test setup: the application sampler was expected to drop the SDK span")
	}
	if !foreignRoot.SpanContext().IsSampled() {
		t.Fatal("test setup: the application sampler was expected to keep the foreign root")
	}

	recordDeliveredScore(t, client, receiver, childCtx,
		langfuse.Score{TraceID: child.TraceID()},
		"a borrowed-mode score under a per-span application sampler")

	otlpRequests := 0
	for _, request := range receiver.all() {
		if strings.HasSuffix(request.path, "/v1/traces") {
			otlpRequests++
		}
	}
	if otlpRequests == 0 {
		t.Fatal("the Langfuse processor exported no OTLP request, want the foreign root exported: the trace this score targets exists")
	}
}

type dropSDKSpansSampler struct{}

func (dropSDKSpansSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	decision := sdktrace.RecordAndSample
	if strings.HasPrefix(p.Name, "dropped-") {
		decision = sdktrace.Drop
	}
	return sdktrace.SamplingResult{
		Decision:   decision,
		Tracestate: oteltrace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (dropSDKSpansSampler) Description() string { return "dropSDKSpans" }
