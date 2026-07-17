# Using an existing OpenTelemetry provider

Use borrowed-provider mode when the application already owns an
`*sdktrace.TracerProvider` and needs the same spans sent to a generic backend
and Langfuse.

```go
cfg := langfuse.ConfigFromEnv()
cfg.TracerProvider = provider

lf, err := langfuse.New(ctx, cfg)
if err != nil {
	return err
}
```

The client registers one additional processor. It does not replace the global
provider, sampler, resource, or existing processors. The application's sampler
remains authoritative: non-recording or record-only spans are not exported to
Langfuse.

Call `WithTraceAttributes` before starting provider spans that need trace name,
user, session, tags, metadata, or version. The Langfuse processor writes these
attributes, plus configured environment and release, at span start. Because
processors share an SDK span, existing exporters see those annotations too.

The smart filter forwards SDK observations, spans with `gen_ai.*` attributes,
and spans from known LLM instrumentation scopes. It does not prevent another
processor from receiving unrelated spans. Attributes added at span end can make
a span exportable, but cannot retroactively change its start-time application-
root decision.

Content controls on `Config` are not provider-wide scrubbers.
`DisableContentCapture` drops only SDK-supplied observation input/output.
`Mask` receives only SDK-supplied observation input/output and observation or
trace metadata. It does not receive names, IDs, tags, model parameters,
status/error text, or provider/framework attributes and events. In particular,
`RecordError` exports `err.Error()` without masking. Configure third-party
instrumentation independently and use payload-free error/status values.

Only one active client is supported on a borrowed provider. A
duplicate construction emits an OTel diagnostic and returns a true no-op
client so unscoped AI spans cannot fan out to two projects.

Borrowed mode batches accepted spans with the standard OpenTelemetry
geometry: a 2048-span queue and up to 512 spans per export. One Langfuse HTTP
request is capped at 4 MiB; oversized batches are split across requests so an
oversized third-party span cannot discard otherwise-valid spans, and only a
span that alone exceeds the cap is dropped. The SDK does not sanitize or copy
arbitrary third-party attributes and events; their size and custom
serialization remain caller-owned. The queue drops newly ended spans when
full by default; `Config.MaxQueueSize` resizes it and
`Config.BlockOnQueueFull` opts into blocking backpressure, so configure
instrumentor/provider limits and watch for sustained export saturation.

Path-prefixed reverse-proxy base URLs (for example
`https://gw.example.com/langfuse/api/public/otel`) are not supported in v0.1.

At graceful shutdown, stop the Langfuse client before the application-owned provider:

```go
langfuseCtx, cancelLangfuse := context.WithTimeout(context.Background(), 5*time.Second)
langfuseErr := lf.Shutdown(langfuseCtx)
cancelLangfuse()

providerCtx, cancelProvider := context.WithTimeout(context.Background(), 5*time.Second)
providerErr := provider.Shutdown(providerCtx)
cancelProvider()

return errors.Join(langfuseErr, providerErr)
```

Create each timeout context immediately before its lifecycle call. Reusing the
Langfuse client's context for provider shutdown can leave the provider no time if the
first shutdown consumes the deadline.

`Client.Shutdown` first stops its batch processor with the supplied context,
then unregisters it. It never shuts down another exporter. End all active spans
before shutdown; `Flush` can export only spans that have ended.

See the compiled [existing-provider example](../examples/existingotel/main.go).
