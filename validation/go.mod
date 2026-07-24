// Module validation holds the opt-in validation suite: slow,
// deliberate, credentialed real-provider checks (never in task ci)
// plus the credential-free cross-SDK baggage interop corpus (the one
// piece task ci does run). Excluded from the released Go modules.
// See validation/README.md.
module github.com/fgn/go-langfuse/validation

go 1.25.0

toolchain go1.25.12

require (
	cloud.google.com/go/auth v0.22.0
	github.com/fgn/go-langfuse v0.4.0
	github.com/fgn/go-langfuse/contrib/googlegenai v0.0.0-00010101000000-000000000000
	github.com/fgn/go-langfuse/contrib/openai v0.0.0-00010101000000-000000000000
	github.com/openai/openai-go v1.12.0
	github.com/sashabaranov/go-openai v1.41.2
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/sdk v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
	google.golang.org/genai v1.59.0
)

require (
	cloud.google.com/go v0.116.0 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.17 // indirect
	github.com/googleapis/gax-go/v2 v2.23.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/api v0.287.1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/grpc v1.82.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/fgn/go-langfuse => ../

replace github.com/fgn/go-langfuse/contrib/openai => ../contrib/openai

replace github.com/fgn/go-langfuse/contrib/googlegenai => ../contrib/googlegenai
