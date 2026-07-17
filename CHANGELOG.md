# Changelog

All notable changes will be documented here. This project follows Semantic
Versioning once the first release is tagged.

## Unreleased

The intended content of the first release, v0.1.0:

- Observation-first tracing over OTLP/HTTP protobuf with Langfuse ingestion
  version 4: `StartObservation`, `Observe`, `Event`, and the `Update`,
  `RecordError`, `End`, `TraceID`, and `ID` observation methods.
- `RecordScore` submits evaluations and user feedback through the Langfuse
  REST scores endpoint using the client's credentials and environment.
- Request-scoped trace identity through `WithTraceAttributes`: name, user,
  session, tags, metadata, and version.
- Isolated (SDK-owned) and borrowed (caller-owned) tracer-provider modes; the
  client never changes global OpenTelemetry state.
- Smart export filtering: SDK observations, `gen_ai.*` spans, known LLM
  instrumentation scopes, and application-root marking.
- Privacy controls: a content-capture switch, the `Mask` hook, payload-free
  diagnostics, and bounded payload and metadata budgets.
- Deterministic transport and batching (a 2048-span queue, 512-span batches,
  and 4 MiB request splitting) isolated from ambient `OTEL_*` environment
  variables, with optional blocking backpressure through `Config.MaxQueueSize`
  and `Config.BlockOnQueueFull`.
