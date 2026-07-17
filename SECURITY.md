# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately to the repository owner
using GitHub's private vulnerability reporting feature. Do not open a public
issue containing exploit details, credentials, telemetry payloads, or personal
data.

Include the affected version, impact, reproduction steps using synthetic data,
and any proposed mitigation. You should receive an acknowledgement within
seven days.

## Telemetry privacy boundary

`DisableContentCapture` drops only SDK-supplied
`ObservationAttributes.Input` and `ObservationAttributes.Output`, before
masking or serialization. It does not drop metadata, identifiers, model
parameters, usage, costs, prompt references, status messages, or errors.

`Mask` is called only for SDK-supplied observation input, observation output,
the complete `ObservationAttributes.Metadata` map, and the complete
`TraceAttributes.Metadata` map. It is not called for observation or trace
names, user/session IDs, tags, versions, level/status, model parameters, usage,
costs, prompt references, completion timestamps, or third-party OpenTelemetry
attributes and events. Applications using a borrowed tracer provider must
separately configure or sanitize third-party instrumentors.

`Score` fields are likewise explicit content: neither `DisableContentCapture`
nor `Mask` applies to a score's comment, value, or metadata. Sanitize them
before calling `RecordScore`.

OpenTelemetry resource attributes are not masked. The isolated provider uses
`resource.Default`, including `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES`; borrowed mode uses the caller's resource. Treat
those environment/configuration values as exported telemetry and audit them
for credentials or personal data.

`RecordError(err)` deliberately exports `err.Error()` in the span status,
Langfuse status message, and exception event. `StatusMessage` is also exported
verbatim. Neither value passes through `Mask`, even when content capture is
disabled. Use payload-free error/status values or sanitize them before calling
the SDK; never include credentials, PHI, prompts, or completions in them.

Maskers may run concurrently. They should make defensive copies and recursively
redact metadata instead of mutating caller-owned maps or slices. A masker is a
transformer for the allowlisted fields above, not an exporter-wide data-loss-
prevention control.

`Mask` and caller-defined JSON/text marshaling methods are trusted application
callbacks. The SDK bounds their returned telemetry values, but cannot bound
CPU, memory, blocking, or side effects inside caller code. Do not serialize
untrusted types with custom methods directly; convert them to plain bounded
maps, slices, and scalar values first.

Use HTTPS for any connection leaving a trusted host, protect both API keys as
credentials, and never attach the secret key to span attributes or logs. The
SDK does attach the project public key to its OpenTelemetry instrumentation
scope to prevent cross-project routing; every exporter on a borrowed provider
can see that identifier. Use an isolated provider if that disclosure is not
acceptable for another telemetry backend.
