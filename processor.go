package langfuse

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	defaultBaseURL      = "https://cloud.langfuse.com"
	observationTypeKey  = "langfuse.observation.type"
	appRootKey          = "langfuse.internal.is_app_root"
	traceIDBaggageKey   = "langfuse_trace_id"
	ingestionVersionKey = "x-langfuse-ingestion-version"
)

// Config contains the endpoint and project credentials needed for Langfuse
// trace ingestion. BaseURL defaults to https://cloud.langfuse.com and may be a
// host root, an /api/public/otel endpoint, or the full traces endpoint.
type Config struct {
	// BaseURL is the Langfuse host or OTLP traces endpoint. An empty value uses
	// https://cloud.langfuse.com.
	BaseURL string
	// PublicKey identifies the Langfuse project.
	PublicKey string
	// SecretKey authenticates writes to the Langfuse project.
	SecretKey string
}

// NewSpanProcessor returns an OpenTelemetry SpanProcessor backed by OTel's
// BatchSpanProcessor. Add it to the application's existing
// sdktrace.TracerProvider. The provider owns ForceFlush and Shutdown.
//
// By default the processor exports explicitly typed Langfuse observations,
// spans carrying gen_ai.* semantic-convention attributes, and spans from known
// LLM instrumentation scopes. Other processors on the same provider continue
// to receive every sampled span.
func NewSpanProcessor(
	ctx context.Context,
	config Config,
	options ...sdktrace.BatchSpanProcessorOption,
) (sdktrace.SpanProcessor, error) {
	if ctx == nil {
		return nil, errors.New("langfuse: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	publicKey := strings.TrimSpace(config.PublicKey)
	if publicKey == "" {
		return nil, errors.New("langfuse: public key is required")
	}
	secretKey := strings.TrimSpace(config.SecretKey)
	if secretKey == "" {
		return nil, errors.New("langfuse: secret key is required")
	}
	endpoint, err := tracesEndpoint(config.BaseURL)
	if err != nil {
		return nil, err
	}

	auth := base64.StdEncoding.EncodeToString([]byte(publicKey + ":" + secretKey))
	exporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithHeaders(map[string]string{
			"Authorization":         "Basic " + auth,
			ingestionVersionKey:     "4",
			"x-langfuse-sdk-name":   "go",
			"x-langfuse-public-key": publicKey,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("langfuse: create OTLP exporter: %w", err)
	}

	return &spanProcessor{
		next:    sdktrace.NewBatchSpanProcessor(exporter, options...),
		claimed: make(map[spanKey]struct{}),
	}, nil
}

func tracesEndpoint(baseURL string) (string, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("langfuse: parse base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("langfuse: base URL must use http or https")
	}
	if parsed.Host == "" {
		return "", errors.New("langfuse: base URL must include a host")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("langfuse: base URL must not include credentials, query, or fragment")
	}

	path := strings.TrimRight(parsed.Path, "/")
	switch path {
	case "":
		parsed.Path = "/api/public/otel/v1/traces"
	case "/api/public/otel":
		parsed.Path = path + "/v1/traces"
	case "/api/public/otel/v1/traces":
		parsed.Path = path
	default:
		return "", errors.New("langfuse: base URL path must be empty, /api/public/otel, or /api/public/otel/v1/traces")
	}
	parsed.RawPath = ""
	return parsed.String(), nil
}

type spanProcessor struct {
	next sdktrace.SpanProcessor

	mu      sync.Mutex
	claimed map[spanKey]struct{}
	stopped bool
}

type spanKey struct {
	traceID oteltrace.TraceID
	spanID  oteltrace.SpanID
}

func (p *spanProcessor) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	spanContext := span.SpanContext()
	expected := spanContext.IsSampled() && shouldExport(span)
	parentClaimed := false
	baggageClaimed := baggage.FromContext(parent).Member(traceIDBaggageKey).Value() == spanContext.TraceID().String()

	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	if parentContext := span.Parent(); parentContext.IsValid() {
		_, parentClaimed = p.claimed[spanKey{
			traceID: parentContext.TraceID(),
			spanID:  parentContext.SpanID(),
		}]
	}
	if expected || parentClaimed || baggageClaimed {
		p.claimed[spanKey{traceID: spanContext.TraceID(), spanID: spanContext.SpanID()}] = struct{}{}
	}
	p.mu.Unlock()

	if expected && !parentClaimed && !baggageClaimed {
		span.SetAttributes(attribute.Bool(appRootKey, true))
	}
	p.next.OnStart(parent, span)
}

func (p *spanProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	spanContext := span.SpanContext()
	p.mu.Lock()
	delete(p.claimed, spanKey{traceID: spanContext.TraceID(), spanID: spanContext.SpanID()})
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	if spanContext.IsSampled() && shouldExport(span) {
		p.next.OnEnd(span)
	}
}

func (p *spanProcessor) ForceFlush(ctx context.Context) error {
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return nil
	}
	return p.next.ForceFlush(ctx)
}

func (p *spanProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	clear(p.claimed)
	p.mu.Unlock()
	return p.next.Shutdown(ctx)
}

var knownLLMScopes = []string{
	"agent_framework",
	"haystack",
	"langsmith",
	"litellm",
	"openinference",
	"opentelemetry.instrumentation.anthropic",
	"opentelemetry.instrumentation.aws_bedrock",
	"opentelemetry.instrumentation.bedrock",
	"opentelemetry.instrumentation.gemini",
	"opentelemetry.instrumentation.google_genai",
	"opentelemetry.instrumentation.google_generativeai",
	"opentelemetry.instrumentation.openai",
	"opentelemetry.instrumentation.openai_v2",
	"opentelemetry.instrumentation.vertex_ai",
	"opentelemetry.instrumentation.vertexai",
	"strands-agents",
	"vllm",
}

func shouldExport(span sdktrace.ReadOnlySpan) bool {
	for _, attr := range span.Attributes() {
		key := string(attr.Key)
		if key == observationTypeKey || strings.HasPrefix(key, "gen_ai.") {
			return true
		}
	}

	scope := span.InstrumentationScope().Name
	if scope == "ai" {
		return true
	}
	for _, prefix := range knownLLMScopes {
		if scope == prefix || strings.HasPrefix(scope, prefix+".") {
			return true
		}
	}
	return false
}
