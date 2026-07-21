# Changelog

All notable changes will be documented here. This project follows Semantic
Versioning once the first release is tagged.

## [Unreleased]

- Add prompt management reads: `Client.GetPrompt` fetches text and chat
  prompts from `/api/public/v2/prompts` with client-side caching
  (60-second default TTL, stale-while-revalidate background refresh with
  failure cooldown, singleflight cache misses, LRU-bounded entries), an
  optional local `Fallback` for guaranteed availability, `Prompt.Compile`
  for `{{variable}}` substitution and chat placeholder filling, and
  `Prompt.Ref` for linking generations to the exact prompt version.
  404 responses wrap the new `ErrPromptNotFound`.
- Add `Observation.EndAt` to end an observation at an explicit time, so
  instrumentation that records already-finished work can reproduce the
  observed timeline together with `StartTime` and `CompletionStartTime`.
- Document and test-lock the detached-trace pattern for background work that
  outlives its request: clearing the parent span context with the standard
  OpenTelemetry helper starts a new application-root trace while propagated
  trace attributes (user, session, tags, metadata, version) survive the
  handoff. No new API.
- Add `Score.Timestamp` to backdate scores and `Score.ConfigID` to bind a
  score to a Langfuse score config.
- **Behavior**: scores are now delivered as single-event `score-create`
  batches to the JSON ingestion endpoint `/api/public/ingestion` instead of
  `/api/public/scores`, matching the official SDKs; the event envelope
  carries the score timestamp. Per-item ingestion errors with status 408,
  429, or 5xx — and item errors without a status, or 207 responses whose
  body cannot be read or does not account for the submitted event — are
  retried like their HTTP counterparts; other item errors drop the score
  with a payload-free diagnostic.

## [0.2.0] - 2026-07-20

- **Breaking**: `RecordScore` now validates synchronously and delivers
  asynchronously with bounded backoff retry instead of performing one
  blocking request. Returned errors are validation and
  lifecycle errors only; transport failures are retried (network errors and
  HTTP 408/429/5xx, honoring `Retry-After`) and, once the retry budget is
  exhausted, dropped with a payload-free diagnostic instead of returning to
  the caller.
- `Flush` and `Shutdown` drain the new bounded score queue (256 scores), and
  `Config.BlockOnQueueFull` now also applies to it.
- The SDK generates the idempotent upsert ID when `Score.ID` is empty, so
  retried deliveries cannot create duplicate scores.

## [0.1.1] - 2026-07-17

- Add a unified `task ci` development and continuous-integration gate covering
  formatting, static analysis, builds, tests, fuzz smoke tests, vulnerability
  scanning, and repository policy checks.

## [0.1.0] - 2026-07-17

The first release:

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
