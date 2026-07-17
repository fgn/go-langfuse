// Package langfuse provides observation-centric Langfuse tracing on top of
// OpenTelemetry.
//
// It exports OTLP/HTTP protobuf traces to Langfuse and can either own an
// isolated tracer provider or attach a Langfuse processor to an existing
// OpenTelemetry SDK tracer provider. The package never changes global
// OpenTelemetry state.
package langfuse
