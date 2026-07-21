# Compatibility

Last verified: 2026-07-17.

| Target | Status | Notes |
| --- | --- | --- |
| Langfuse Cloud observation-first/v4 UI | Primary target | OTLP/HTTP protobuf, trace endpoint, Basic auth, and ingestion header `4` are locked by wire tests and verified against a live Langfuse deployment before each release. |
| Langfuse self-hosted OTLP endpoint | Protocol compatible | Langfuse documents the OTLP endpoint for self-hosted releases beginning with v3.22.0. Observation-first ingestion version 4 must be verified against the exact deployed release before production use. |
| OpenTelemetry Go SDK v1.44 | Tested | Both isolated and caller-owned `*sdktrace.TracerProvider` modes are covered under the race detector. |
| OTLP/HTTP protobuf | Supported | This is the only transport emitted by this module. |
| OTLP/HTTP JSON | Not emitted | Langfuse accepts it, but this SDK deliberately has one wire format. |
| OTLP/gRPC | Unsupported | Langfuse's native endpoint does not support gRPC. |

The SDK sends `x-langfuse-ingestion-version: 4` unconditionally. A server that
does not recognize this version is incompatible; upgrade that deployment
rather than weakening the header.

Scores are submitted as single-event `score-create` batches to the JSON
ingestion endpoint `/api/public/ingestion` on the same host, with the same
Basic authentication; the event envelope carries the score timestamp, matching
the official SDKs. This endpoint is independent of the OTLP ingestion version.
Its 207 multi-status body is part of the delivery contract; how per-item
results are classified for retry is described in the
[reference](reference.md#buffering-and-backpressure).

Prompts are read from the REST endpoint `GET /api/public/v2/prompts/{name}`
on the same host with the same Basic authentication, selected by `version` or
`label`. Responses are validated against the request (name, version, label,
and type) before they are cached; caching and retry semantics are described
in the [reference](reference.md#prompt-management).

go-langfuse uses the instrumentation scope `langfuse-sdk.go`. Langfuse treats
the `langfuse-sdk` prefix as an ingestion marker that prevents semantic
attributes from being copied into generic `metadata.attributes`; the `.go`
suffix and `x-langfuse-sdk-name: go` identify the independent client.

The base URL must be a host root, `/api/public/otel`, or the full
`/api/public/otel/v1/traces` endpoint. Path-prefixed reverse-proxy base URLs
(for example `https://gw.example.com/langfuse/api/public/otel`) are not
supported in v0.1.

Tests never require Langfuse credentials; compatibility is re-verified
against a live Langfuse deployment before each release, as described in
[RELEASING.md](../RELEASING.md).
