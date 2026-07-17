# Contributing

Thanks for helping improve go-langfuse.

## Development

This module requires Go 1.25 or newer and suggests the patched Go 1.25.12
toolchain recorded in `go.mod`. Before submitting a change, run:

```sh
gofmt -w <changed-go-files>
go test -race ./...
go vet ./...
```

Changes to the public root-package API, Langfuse attribute names, endpoint,
authentication headers, ingestion version, usage normalization, filtering, or
provider lifecycle require focused tests. Wire changes must be verified by
decoding OTLP protobuf—not by inspecting implementation state.

Keep pull requests narrow and explain any compatibility or privacy impact.
Never include Langfuse credentials, production telemetry, or end-user
content in fixtures, diagnostics, issues, or pull requests.

## Design boundaries

The root module is intentionally small: observations, trace attributes, and
scores. Prompt management, datasets, and administrative APIs are currently out
of scope. Before proposing a new exported concept, explain why the same result
cannot be achieved through `ObservationAttributes`, `TraceAttributes`,
`Score`, or the standard OpenTelemetry span escape hatch.

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 in [LICENSE](LICENSE).
