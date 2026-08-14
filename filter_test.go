package langfuse_test

import (
	"context"
	"testing"

	"github.com/fgn/go-langfuse"
	otelattr "go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestPublicSpanFilterHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		scope        string
		attributes   []otelattr.KeyValue
		wantLangfuse bool
		wantGenAI    bool
		wantKnown    bool
		wantDefault  bool
	}{
		{name: "go SDK", scope: "langfuse-sdk.go", wantLangfuse: true, wantKnown: true, wantDefault: true},
		{name: "GenAI semantic convention", scope: "unknown", attributes: []otelattr.KeyValue{otelattr.String("gen_ai.request.model", "model")}, wantGenAI: true, wantDefault: true},
		{name: "GenAI prefix without namespace boundary", scope: "unknown", attributes: []otelattr.KeyValue{otelattr.String("gen_ai", "value")}},
		{name: "raw observation marker", scope: "unknown", attributes: []otelattr.KeyValue{otelattr.String("langfuse.observation.type", "generation")}},
		{name: "Vercel AI scope", scope: "ai", wantKnown: true, wantDefault: true},
		{name: "generic ai descendant", scope: "ai.worker"},
		{name: "known instrumentor descendant", scope: "openinference.instrumentation", wantKnown: true, wantDefault: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			span := endedFilterTestSpan(t, test.scope, test.attributes...)
			if got := langfuse.IsLangfuseSpan(span); got != test.wantLangfuse {
				t.Errorf("IsLangfuseSpan = %v, want %v", got, test.wantLangfuse)
			}
			if got := langfuse.IsGenAISpan(span); got != test.wantGenAI {
				t.Errorf("IsGenAISpan = %v, want %v", got, test.wantGenAI)
			}
			if got := langfuse.IsKnownLLMInstrumentor(span); got != test.wantKnown {
				t.Errorf("IsKnownLLMInstrumentor = %v, want %v", got, test.wantKnown)
			}
			if got := langfuse.IsDefaultExportSpan(span); got != test.wantDefault {
				t.Errorf("IsDefaultExportSpan = %v, want %v", got, test.wantDefault)
			}
		})
	}
}

func endedFilterTestSpan(t *testing.T, scope string, attributes ...otelattr.KeyValue) sdktrace.ReadOnlySpan {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	_, span := provider.Tracer(scope).Start(
		context.Background(),
		"filter-test",
		oteltrace.WithAttributes(attributes...),
	)
	span.End()
	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended span count = %d, want 1", len(ended))
	}
	return ended[0]
}
