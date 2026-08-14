// Package processor contains the Langfuse OpenTelemetry span processor.
package processor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	otelattr "go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/diagnostic"
)

// ContextAttributesFunc returns already-normalized attributes belonging to
// this processor's client. The root package owns the context representation so
// multiple isolated clients can safely use the same context.
type ContextAttributesFunc func(context.Context) []otelattr.KeyValue

// TraceClaimFunc reports whether ctx contains this processor's client-scoped
// application-root claim for traceID.
type TraceClaimFunc func(context.Context, oteltrace.TraceID) bool

// AdmitFunc accepts the pending admission token in ctx, confirming to the
// starting SDK observation that this processor observed its start before any
// teardown. expected reports whether the span passed start-time export
// classification. It runs synchronously inside Tracer.Start and must not block.
type AdmitFunc func(context.Context, bool)

// Config configures a Processor.
type Config struct {
	Next sdktrace.SpanProcessor

	PublicKey   string
	Environment string
	Release     string

	ContextAttributes ContextAttributesFunc
	HasTraceClaim     TraceClaimFunc
	Admit             AdmitFunc
	ShouldExportSpan  func(sdktrace.ReadOnlySpan) bool
}

// Processor adds Langfuse propagation and application-root attributes, then
// forwards only LLM-relevant sampled spans to the wrapped processor.
type Processor struct {
	next sdktrace.SpanProcessor

	publicKey   string
	environment string
	release     string

	contextAttributes ContextAttributesFunc
	hasTraceClaim     TraceClaimFunc
	admit             AdmitFunc
	shouldExportSpan  func(sdktrace.ReadOnlySpan) bool

	stopped atomic.Bool

	expectationsMu           sync.Mutex
	expected                 map[spanKey]struct{}
	expectationLimitReported atomic.Bool

	shutdownStarted atomic.Bool
	shutdownDone    chan struct{}
}

// Application-root coordination is best-effort state for active spans, not a
// reason to retain an unbounded number of spans that an instrumentor forgot to
// end. Crossing the cap affects only parent/root classification; export at end
// still uses the span's final attributes.
const maxActiveExpectations = 4096

type spanKey struct {
	traceID oteltrace.TraceID
	spanID  oteltrace.SpanID
}

// New returns a Langfuse processor wrapping config.Next.
func New(config Config) (*Processor, error) {
	if config.Next == nil {
		return nil, errors.New("langfuse processor: next span processor is required")
	}

	shouldExportSpan := config.ShouldExportSpan
	if shouldExportSpan == nil {
		shouldExportSpan = IsDefaultExportSpan
	}

	return &Processor{
		next:              config.Next,
		publicKey:         config.PublicKey,
		environment:       config.Environment,
		release:           config.Release,
		contextAttributes: config.ContextAttributes,
		hasTraceClaim:     config.HasTraceClaim,
		admit:             config.Admit,
		shouldExportSpan:  shouldExportSpan,
		expected:          make(map[spanKey]struct{}),
		shutdownDone:      make(chan struct{}),
	}, nil
}

// OnStart fills missing Langfuse attributes and performs the start-time half
// of application-root detection. It intentionally tracks only direct parents,
// matching the current official Python processor.
func (p *Processor) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	if p.stopped.Load() || !p.acceptsProjectSpan(span) {
		return
	}

	// Context callbacks are deliberately outside lifecycle. A diagnostic hook
	// may itself be instrumented, and no callback should run while holding an
	// SDK lifecycle lock.
	propagated := safeContextAttributes(p.contextAttributes, parent)
	claimed := safeHasTraceClaim(p.hasTraceClaim, parent, span.SpanContext().TraceID())

	if p.stopped.Load() {
		return
	}

	p.fillMissingAttributes(span, propagated)

	spanContext := span.SpanContext()
	expected := spanContext.IsSampled() && safeShouldExportSpan(p.shouldExportSpan, span, false)
	if p.stopped.Load() {
		return
	}

	// Admit only spans from the SDK's own tracer: a foreign span started with
	// a context that happens to carry an admission token must not accept it.
	if span.InstrumentationScope().Name == lfattr.TracerName {
		safeAdmit(p.admit, parent, expected)
	}
	parentExpected := false
	expectationOmitted := false

	p.expectationsMu.Lock()
	if p.stopped.Load() {
		p.expectationsMu.Unlock()
		return
	}
	if parentContext := span.Parent(); parentContext.IsValid() {
		_, parentExpected = p.expected[spanKey{
			traceID: parentContext.TraceID(),
			spanID:  parentContext.SpanID(),
		}]
	}
	if expected {
		if len(p.expected) < maxActiveExpectations {
			p.expected[spanKey{
				traceID: spanContext.TraceID(),
				spanID:  spanContext.SpanID(),
			}] = struct{}{}
		} else {
			expectationOmitted = true
		}
	}
	p.expectationsMu.Unlock()
	if expectationOmitted && p.expectationLimitReported.CompareAndSwap(false, true) {
		diagnostic.Report("active Langfuse span count exceeds the application-root tracking limit; root marking may be conservative")
	}

	if expected &&
		!parentExpected &&
		!claimed &&
		isAppRootEligible(span) {
		span.SetAttributes(otelattr.Bool(lfattr.AppRootKey, true))
	}

	p.next.OnStart(parent, span)
}

// OnEnd removes start-time state and applies the final smart filter. The end
// decision sees attributes added late by streaming/provider instrumentation.
func (p *Processor) OnEnd(span sdktrace.ReadOnlySpan) {
	spanContext := span.SpanContext()
	p.expectationsMu.Lock()
	delete(p.expected, spanKey{
		traceID: spanContext.TraceID(),
		spanID:  spanContext.SpanID(),
	})
	p.expectationsMu.Unlock()

	if p.stopped.Load() ||
		!p.acceptsProjectSpan(span) ||
		!spanContext.IsSampled() ||
		!safeShouldExportSpan(p.shouldExportSpan, span, true) {
		return
	}
	if span.InstrumentationScope().Name == lfattr.TracerName && span.DroppedAttributes() > 0 {
		diagnostic.Report("an SDK observation exceeded the tracer provider's span attribute limits")
	}

	p.next.OnEnd(span)
}

// ForceFlush forwards a flush only while the processor is active.
func (p *Processor) ForceFlush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if p.stopped.Load() {
		return nil
	}

	// Do not hold lifecycle while flushing. Exporters may themselves produce
	// telemetry, and BatchSpanProcessor safely coordinates a concurrent flush
	// and shutdown.
	return p.next.ForceFlush(ctx)
}

// Shutdown stops accepting work, clears application-root state, and shuts
// down the wrapped processor exactly once. Later calls are harmless; a caller
// concurrent with the first shutdown may stop waiting when its own context is
// canceled.
func (p *Processor) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if !p.shutdownStarted.CompareAndSwap(false, true) {
		select {
		case <-p.shutdownDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	p.stopped.Store(true)

	p.expectationsMu.Lock()
	clear(p.expected)
	p.expectationsMu.Unlock()

	defer close(p.shutdownDone)
	return p.next.Shutdown(ctx)
}

func (p *Processor) acceptsProjectSpan(span sdktrace.ReadOnlySpan) bool {
	scope := span.InstrumentationScope()
	if p.publicKey == "" || scope.Name != lfattr.TracerName {
		return true
	}
	value, found := scope.Attributes.Value(otelattr.Key("public_key"))
	return found && value.Type() == otelattr.STRING && value.AsString() == p.publicKey
}

func (p *Processor) fillMissingAttributes(span sdktrace.ReadWriteSpan, propagated []otelattr.KeyValue) {
	missing := make([]otelattr.KeyValue, 0, len(propagated)+2)
	appendMissing := func(item otelattr.KeyValue) {
		if !item.Valid() {
			return
		}
		for _, candidate := range missing {
			if candidate.Key == item.Key {
				return
			}
		}
		missing = append(missing, item)
	}

	// Request-scoped values take precedence over client defaults if a future
	// context helper adds an environment or release override.
	for _, item := range propagated {
		appendMissing(item)
	}
	if p.environment != "" {
		appendMissing(otelattr.String(lfattr.EnvironmentKey, p.environment))
	}
	if p.release != "" {
		appendMissing(otelattr.String(lfattr.ReleaseKey, p.release))
	}
	if len(missing) == 0 {
		return
	}

	// The desired set is capped by the client's propagation budgets. Scan the
	// caller's attributes without materializing an O(n) mirror: borrowed
	// providers may intentionally configure a very high attribute-count limit.
	for _, existing := range span.Attributes() {
		for index := 0; index < len(missing); index++ {
			if missing[index].Key != existing.Key {
				continue
			}
			copy(missing[index:], missing[index+1:])
			missing = missing[:len(missing)-1]
			break
		}
		if len(missing) == 0 {
			return
		}
	}

	span.SetAttributes(missing...)
}

func safeContextAttributes(callback ContextAttributesFunc, ctx context.Context) (result []otelattr.KeyValue) {
	if callback == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			diagnostic.Report("processor context-attribute callback panicked; propagated attributes omitted")
			result = nil
		}
	}()
	return callback(ctx)
}

func safeAdmit(callback AdmitFunc, ctx context.Context, expected bool) {
	if callback == nil {
		return
	}
	defer func() {
		if recover() != nil {
			diagnostic.Report("processor admission callback panicked; start not admitted")
		}
	}()
	callback(ctx, expected)
}

func safeHasTraceClaim(callback TraceClaimFunc, ctx context.Context, traceID oteltrace.TraceID) (claimed bool) {
	if callback == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			diagnostic.Report("processor trace-claim callback panicked; claim ignored")
			claimed = false
		}
	}()
	return callback(ctx, traceID)
}

func safeShouldExportSpan(
	filter func(sdktrace.ReadOnlySpan) bool,
	span sdktrace.ReadOnlySpan,
	final bool,
) (export bool) {
	defer func() {
		if recover() == nil {
			return
		}
		if final {
			diagnostic.Report("span export filter panicked; span dropped")
		} else {
			diagnostic.Report("span export filter panicked during application-root classification; root marker omitted")
		}
		export = false
	}()
	return filter(span)
}

func isAppRootEligible(span sdktrace.ReadOnlySpan) bool {
	if !span.Parent().IsValid() {
		return true
	}

	return span.InstrumentationScope().Name != "litellm" || span.Name() != "raw_gen_ai_request"
}

// IsDefaultExportSpan applies the Langfuse v4 smart default filter.
func IsDefaultExportSpan(span sdktrace.ReadOnlySpan) bool {
	return IsLangfuseSpan(span) || IsGenAISpan(span) || IsKnownLLMInstrumentor(span)
}

// IsLangfuseSpan reports whether span was created by this SDK's tracer.
func IsLangfuseSpan(span sdktrace.ReadOnlySpan) bool {
	return span.InstrumentationScope().Name == lfattr.TracerName
}

// IsGenAISpan reports whether span has an OpenTelemetry GenAI semantic
// convention attribute under gen_ai.*.
func IsGenAISpan(span sdktrace.ReadOnlySpan) bool {
	for _, item := range span.Attributes() {
		if strings.HasPrefix(string(item.Key), "gen_ai.") {
			return true
		}
	}
	return false
}

// IsKnownLLMInstrumentor reports whether span comes from a known LLM
// instrumentation scope.
func IsKnownLLMInstrumentor(span sdktrace.ReadOnlySpan) bool {
	scope := span.InstrumentationScope().Name
	for _, prefix := range knownLLMInstrumentationScopePrefixes {
		// The generic "ai" scope is exact-only, matching the TypeScript SDK;
		// every named instrumentation namespace also admits descendants.
		if scope == prefix || prefix != "ai" && strings.HasPrefix(scope, prefix+".") {
			return true
		}
	}
	return false
}

// This list is the union of the official Langfuse Python SDK at
// 73b5c028d2757d8960b3a468bd80c9ef99b52e74 and TypeScript SDK at
// a4ef9f2b55da5576fe66be4a6c026268ae9656c5, verified 2026-08-14. The Go
// SDK's langfuse-sdk.go scope is handled by IsLangfuseSpan before this list.
// See THIRD_PARTY_NOTICES.md. Prefix matching is namespace-aware.
var knownLLMInstrumentationScopePrefixes = [...]string{
	"langfuse-sdk",
	"agent_framework",
	"autogen-core",
	"ai",
	"haystack",
	"langsmith",
	"litellm",
	"openinference",
	"opentelemetry.instrumentation.agno",
	"opentelemetry.instrumentation.alephalpha",
	"opentelemetry.instrumentation.anthropic",
	"opentelemetry.instrumentation.aws_bedrock",
	"opentelemetry.instrumentation.bedrock",
	"opentelemetry.instrumentation.cohere",
	"opentelemetry.instrumentation.crewai",
	"opentelemetry.instrumentation.gemini",
	"opentelemetry.instrumentation.google_generativeai",
	"opentelemetry.instrumentation.google_genai",
	"opentelemetry.instrumentation.groq",
	"opentelemetry.instrumentation.haystack",
	"opentelemetry.instrumentation.langchain",
	"opentelemetry.instrumentation.llamaindex",
	"opentelemetry.instrumentation.mistralai",
	"opentelemetry.instrumentation.ollama",
	"opentelemetry.instrumentation.openai",
	"opentelemetry.instrumentation.openai_agents",
	"opentelemetry.instrumentation.openai_v2",
	"opentelemetry.instrumentation.replicate",
	"opentelemetry.instrumentation.sagemaker",
	"opentelemetry.instrumentation.together",
	"opentelemetry.instrumentation.transformers",
	"opentelemetry.instrumentation.vertexai",
	"opentelemetry.instrumentation.vertex_ai",
	"opentelemetry.instrumentation.voyageai",
	"opentelemetry.instrumentation.watsonx",
	"opentelemetry.instrumentation.writer",
	"pydantic-ai",
	"strands-agents",
	"vllm",
}
