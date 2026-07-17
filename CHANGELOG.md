# Changelog

All notable changes will be documented here. This project follows Semantic
Versioning once the first release is tagged.

## Unreleased

- Transport now uses the official `otlptracehttp` exporter with every
  environment-sensitive option pinned explicitly, wrapped for secret
  redaction and 4 MiB request-size splitting; the hand-rolled HTTP
  retry/redirect client is removed.
- Standard OpenTelemetry batch geometry in both provider modes: a 2048-span
  queue and 512-span batches replace the previous 64-span queue with 16-span
  (owned) and single-span (borrowed) requests.
- New `Config.MaxQueueSize` and `Config.BlockOnQueueFull` fields size the
  export buffer and opt into blocking backpressure instead of the default
  drop-on-full behavior.
- Live compatibility test reads the exported generation and trace IO back
  through the public REST API before passing.
- CI additionally builds and tests with the minimum supported Go toolchain
  (1.25.0).
- Initial observation-centric tracing SDK implementation.
- OTLP/HTTP protobuf ingestion with Langfuse ingestion version 4.
- Isolated and borrowed OpenTelemetry tracer-provider modes.
- Context propagation, smart span filtering, application-root marking,
  privacy controls, and exclusive usage normalization.
- Project-scoped SDK-span routing, bounded serialization/metadata budgets, and
  callback-safe concurrent lifecycle handling.
- Deterministic transport, batch, and owned-span limits isolated from ambient
  generic OpenTelemetry exporter/provider environment settings.
- Compiled quickstart and borrowed-provider examples, decoded OTLP goldens,
  API-surface locking, and gated release automation.
