package manualspan

import (
	"context"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// StartChat starts a manual GenAI span using only OTel semantic conventions.
// End the returned span after the response stream has been fully consumed.
func StartChat(
	ctx context.Context,
	tracer trace.Tracer,
	model string,
) (context.Context, trace.Span) {
	return tracer.Start(ctx, "chat", trace.WithAttributes(
		semconv.GenAIOperationNameChat,
		semconv.GenAIProviderNameOpenAI,
		semconv.GenAIRequestModel(model),
	))
}
