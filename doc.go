// Package langfuse connects an existing OpenTelemetry tracer provider to
// Langfuse.
//
// NewSpanProcessor returns a standard OpenTelemetry SpanProcessor. The
// application remains responsible for its tracer provider, resources,
// sampling, global registration, flushing, and shutdown.
package langfuse
