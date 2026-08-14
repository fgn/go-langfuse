package langfuse

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	lfprocessor "github.com/fgn/go-langfuse/internal/processor"
)

// IsDefaultExportSpan applies the default Langfuse LLM-focused span filter.
// It accepts SDK observations, spans with OpenTelemetry GenAI semantic
// convention attributes under gen_ai.*, and spans from known LLM
// instrumentation scopes.
func IsDefaultExportSpan(span sdktrace.ReadOnlySpan) bool {
	return lfprocessor.IsDefaultExportSpan(span)
}

// IsLangfuseSpan reports whether span was created by this SDK's tracer.
func IsLangfuseSpan(span sdktrace.ReadOnlySpan) bool {
	return lfprocessor.IsLangfuseSpan(span)
}

// IsGenAISpan reports whether span has an OpenTelemetry GenAI semantic
// convention attribute under gen_ai.*.
func IsGenAISpan(span sdktrace.ReadOnlySpan) bool {
	return lfprocessor.IsGenAISpan(span)
}

// IsKnownLLMInstrumentor reports whether span comes from a known LLM
// instrumentation scope.
func IsKnownLLMInstrumentor(span sdktrace.ReadOnlySpan) bool {
	return lfprocessor.IsKnownLLMInstrumentor(span)
}
