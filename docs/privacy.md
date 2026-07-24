# Content and sensitive data

The SDK never inspects function arguments, HTTP bodies, or model clients and
captures no provider content automatically. It does export fields explicitly
supplied by the caller. Input/output are the obvious content fields, but
metadata, model parameters, status messages, and errors can also contain
sensitive data.

Set `LANGFUSE_CONTENT_CAPTURE_ENABLED=false`, or configure
`DisableContentCapture`, to drop SDK-supplied `Input` and `Output` while still
recording every other field. The privacy boundary is deliberately narrow:

| Data source | Dropped by `DisableContentCapture` | Passed to `Mask` |
| --- | --- | --- |
| `ObservationAttributes.Input` and `Output` | Yes | Yes, unless content capture is disabled |
| `ObservationAttributes.Metadata` | No | Yes, once as the complete `map[string]any` |
| `TraceAttributes.Metadata` | No | Yes, once as the complete `map[string]any` |
| Observation name/type, trace name, user/session IDs, tags, version, level, `StatusMessage`, model/parameters, usage, costs, prompt, and completion time | No | No |
| `RecordError(err)` text and exception event | No | No |
| `Score` comment, value, and metadata | No | No |
| OpenTelemetry resource attributes (`resource.Default`/`OTEL_RESOURCE_ATTRIBUTES` in isolated mode; caller resource in borrowed mode) | No | No |
| Third-party OTel span attributes and events | No | No |

**Cross-process propagation sends attributes to every destination.**
`WithBaggagePropagation` places user ID, session ID, trace name, version,
request-scoped environment, string metadata values, and the 32-hex trace ID
of the current application root into W3C baggage on its context branch. Baggage is delivered by whatever propagator the
application has installed, so these values travel with **every** outbound
request that carries the context — third-party APIs and services that have
nothing to do with Langfuse included — until the branch ends. Inbound
metadata accepted by `WithTraceAttributesFromBaggage` passes through the
configured `Mask` exactly once, like local trace metadata; the other
propagated fields are identifiers and are never masked. Enable propagation
only on paths where that disclosure is intended. Baggage diagnostics name
fixed protocol members only; metadata key suffixes and unknown member names
are user- or wire-controlled and appear in diagnostics as counts, never as
text.

Disabling content capture does not make metadata, model parameters, status
messages, or errors safe. `RecordError` exports `err.Error()` as the OTel
status description, Langfuse status message, and exception-event message. Use
payload-free error values or sanitize an error before passing it to
`RecordError`; never put credentials, PHI, prompts, or completions in an error
or `StatusMessage`.

`Mask` can transform only the SDK values shown in the table. A metadata masker
must return a `map[string]any`; returning another type omits that metadata.
Copy and recursively redact maps and slices rather than mutating caller-owned
data:

```go
cfg := langfuse.ConfigFromEnv()
cfg.DisableContentCapture = true
cfg.Mask = redactSDKValue

func redactSDKValue(value any) any {
	switch value := value.(type) {
	case string:
		return strings.ReplaceAll(value, "secret", "[redacted]")
	case map[string]any:
		redacted := make(map[string]any, len(value))
		for key, item := range value {
			switch strings.ToLower(key) {
			case "email", "customer_id", "authorization":
				redacted[key] = "[redacted]"
			default:
				redacted[key] = redactSDKValue(item)
			}
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for index, item := range value {
			redacted[index] = redactSDKValue(item)
		}
		return redacted
	default:
		return value
	}
}
```

The example assumes JSON-like `map[string]any` and `[]any` shapes. A
production masker must cover every concrete value type the application
supplies, must be concurrency-safe, and should have tests proving its
redaction policy.

These controls apply **only to data supplied through this client**; they never
rewrite third-party OTel instrumentation, so configure or sanitize those
instrumentors independently in borrowed mode (the client warns when content
capture is disabled there). Resource attributes are also untouched: isolated
mode preserves `resource.Default` (including `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES`) and borrowed mode preserves the caller's resource,
so audit them before export.
