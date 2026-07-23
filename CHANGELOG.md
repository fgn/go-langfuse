# Changelog

All notable changes will be documented here. This project follows Semantic
Versioning once the first release is tagged.

## [Unreleased]

- Add opt-in provider instrumentation as independent contrib modules:
  `contrib/openai` (package `langfuseopenai`) records OpenAI-wire chat
  completion, completion, and embedding calls, and `contrib/googlegenai`
  (package `langfusegenai`) records Gemini generate, stream, and
  embedding calls on both the Developer API and Vertex AI. The adapters
  attach as an `http.RoundTripper` at client construction, parse the
  provider wire format (no provider SDK dependencies; the core module
  gains no dependencies), record one generation or embedding
  observation per HTTP attempt with model, content, usage,
  time-to-first-token, and wire-provable status, and route everything
  through the core masking, capture, sampling, and limit controls. A
  never-released `contrib/integrationtest` module exercises the real
  provider SDKs, including the documented Vertex credentials
  composition; `task ci` gains contrib sync, test, and purity gates.
  Runnable end-to-end adapter examples that need no provider
  credentials live in `contrib/integrationtest/examples/`.
- Add runnable examples for prompt management (`examples/prompts`), scores
  (`examples/scores`), and deterministic trace sampling with a correlated
  LLM-judge gate (`examples/sampling`), and link them from the README
  feature sections.

## [0.4.0] - 2026-07-21

- Add deterministic trace sampling for isolated mode: `Config.SampleRate`
  (`LANGFUSE_SAMPLE_RATE`, `[0, 1]`) sets the default fraction and
  `Client.WithSampleRate` overrides it per context path, so one process can
  export different kinds of traces at different rates. Decisions are a
  deterministic threshold on the trace ID matching OpenTelemetry's
  `TraceIDRatioBased`, made once per trace on the deciding context path and
  inherited by every SDK observation in it, including across foreign middle
  spans. The new `TraceSampledAt` exposes the same predicate for correlated
  application-level sampling (for example, run an LLM-judge evaluation on 2%
  of traces, guaranteed to be a subset of the traces kept for export), and
  `Observation.Sampled` reports the sampling decision, not a delivery
  guarantee.
- Sampled-out observations keep their trace and span IDs and become cheap
  no-ops: `Update` and `RecordError` return before serialization, `Mask`, or
  `Error()` calls. A score recorded directly on a sampled-out, SDK-originated
  context path targeting that trace is suppressed with a once-per-client
  diagnostic, so sampled-out traces do not accumulate orphaned scores; all
  other scores are delivered unchanged. Borrowed-mode sampling is unchanged:
  the application's sampler remains authoritative and the new controls are
  ignored there with a diagnostic.
- **Behavior**: a start that misses processor admission or observes the
  shutdown transition immediately after span start now returns the no-op
  observation instead of a live handle whose export path is already torn
  down. Admission is confirmed by the Langfuse processor during span start.
  A start whose checks pass at the instant teardown begins can still return
  a live handle that never exports, within `Sampled`'s documented
  non-guarantee of delivery.

## [0.3.0] - 2026-07-21

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
  429, or 5xx are retried like their HTTP counterparts, as are item errors
  without a status and 207 responses whose body cannot be read or does not
  account for the submitted event; other item errors drop the score with a
  payload-free diagnostic.

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
