# Contributing

Thanks for helping improve go-langfuse.

## Development

This module requires Go 1.25 or newer and suggests the patched Go 1.25.13
toolchain recorded in `go.mod`. Before submitting a change, run:

```sh
gofmt -w <changed-go-files>
go test -race ./...
go vet ./...
```

Changes to the public root-package API, Langfuse attribute names, endpoint,
authentication headers, ingestion version, usage normalization, filtering, or
provider lifecycle require focused tests. Wire changes must be verified by
decoding OTLP protobuf, not by inspecting implementation state.

Keep pull requests narrow and explain any compatibility or privacy impact.
Never include Langfuse credentials, production telemetry, or end-user
content in fixtures, diagnostics, issues, or pull requests.

## Design boundaries

The root package owns observations, trace attributes, queued scores, and cached
runtime prompt retrieval. Synchronous prompt authoring and project REST APIs
belong in the separate `api` package; constructing its client must not create a
tracer provider, queue, or worker. Provider integrations remain in contrib
modules, with no provider SDK dependencies added to the root module.

Resource APIs must be checked against the pinned OpenAPI contract, with tests
for exact routes, request presence, pagination, retry safety, and payload-free
errors. Run `python3 scripts/index-api-contract.py --check` after API changes.
Update [API coverage](docs/api-coverage.md) and the
[delivery ledger](docs/gap-closure-status.md) instead of exposing empty service
placeholders or claiming unimplemented operations are supported.

Before adding a root observation concept, explain why the existing attributes,
queued score API, or standard OpenTelemetry span escape hatch are insufficient.
Changes to the root public surface must update its golden and reflection tests.

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 in [LICENSE](LICENSE).
