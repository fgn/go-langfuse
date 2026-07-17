# Compatibility

Research snapshot: 2026-07-16.

| Target | Status | Notes |
| --- | --- | --- |
| Langfuse Cloud observation-first/v4 UI | Intended v0.1 target | OTLP/HTTP protobuf, trace endpoint, Basic auth, and ingestion header `4` are locked by local wire tests. A credentialed live gate is still required before tagging. |
| Langfuse self-hosted OTLP endpoint | Protocol compatible | Langfuse documents the OTLP endpoint for self-hosted releases beginning with v3.22.0. Observation-first ingestion version 4 must be verified against the exact deployed release before production use. |
| OpenTelemetry Go SDK v1.44 | Tested | Both isolated and caller-owned `*sdktrace.TracerProvider` modes are covered under the race detector. |
| OTLP/HTTP protobuf | Supported | This is the only transport emitted by this module. |
| OTLP/HTTP JSON | Not emitted | Langfuse accepts it, but this SDK deliberately has one wire format. |
| OTLP/gRPC | Unsupported | Langfuse's native endpoint does not support gRPC. |

The SDK sends `x-langfuse-ingestion-version: 4` unconditionally. A server that
does not recognize this version is incompatible; upgrade that deployment
rather than weakening the header.

go-langfuse uses the instrumentation scope `langfuse-sdk.go`. Langfuse treats
the `langfuse-sdk` prefix as an ingestion marker that prevents semantic
attributes from being copied into generic `metadata.attributes`; the `.go`
suffix and `x-langfuse-sdk-name: go` identify the independent client.

The base URL must be a host root, `/api/public/otel`, or the full
`/api/public/otel/v1/traces` endpoint. Path-prefixed reverse-proxy base URLs
(for example `https://gw.example.com/langfuse/api/public/otel`) are not
supported in v0.1.

The live release gate uses a dedicated, non-production Langfuse project and
must verify UI visibility, environment/user/session/tag/metadata filters,
application roots, generation usage/cost, prompt links, and observation-level
evaluators. Before the gate runs, that project must contain prompt
`go-langfuse-live-prompt` version 1 and the evaluator named by the release
checklist. Invoke the Go test with `-count=1`; every run includes a unique
marker in its trace name, session, metadata, and test log so reviewers cannot
mistake cached or old data for the current run. Live credentials are never
required by the ordinary unit suite.
