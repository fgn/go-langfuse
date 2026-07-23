// Package langfuseopenai records Langfuse observations for OpenAI-wire
// API calls (OpenAI, Azure OpenAI, and OpenAI-compatible endpoints)
// made through any HTTP-based client, including the official openai-go
// and sashabaranov/go-openai, without wrapping or modifying those
// clients.
//
// Attach the transport where the application constructs its HTTP
// client:
//
//	cfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
//
// The adapter records one generation or embedding observation per
// recognized HTTP attempt (chat completions, completions, and
// embeddings in v0.1), parented by the observation in the request
// context. SDK-level retries and redirect hops are separate attempts;
// wrap provider calls in a span-typed observation for logical grouping
// and see the README for the retry and metric semantics.
//
// Everything recorded flows through the core client's privacy
// controls: Config.Mask, LANGFUSE_CONTENT_CAPTURE_ENABLED, sampling,
// and payload limits apply unchanged, with one documented exception: a
// strictly validated response model string is promoted to the model
// field, which Langfuse pricing requires. The adapter reads no request
// headers and exports no headers. This module adds no provider SDK to
// the dependency graph: beyond the core module and its OpenTelemetry
// dependencies it uses only the standard library.
package langfuseopenai

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/openai/internal/wiretap"
)

// adapterMarker identifies transports built by this package for the
// double-wrap misuse guard.
const adapterMarker = "langfuseopenai"

// RouteInfo is the sanitized route descriptor passed to naming
// callbacks: never the request, never headers, never body content.
// Model is URL-derived and therefore usually empty on OpenAI routes,
// which carry the model in the body; naming callbacks must tolerate
// that.
type RouteInfo struct {
	Provider   string
	Route      string
	APIVersion string
	Model      string
}

// Option configures NewTransport. Options apply in argument order with
// last-wins precedence; nil Options are ignored.
type Option func(*options)

type options struct {
	name            func(RouteInfo) string
	provider        string
	noBodyInspect   bool
	noContentExport bool
}

// WithObservationName derives observation names from the route
// descriptor. The callback runs synchronously on the request path and
// must be concurrency-safe; a panicking callback is disabled for the
// transport's lifetime with one diagnostic. WithObservationName(nil)
// restores default naming.
func WithObservationName(f func(RouteInfo) string) Option {
	return func(o *options) { o.name = f }
}

var providerShape = regexp.MustCompile(`^[a-z0-9_-]{1,40}$`)

// WithProvider overrides the provider label recorded in metadata, for
// proxied or OpenAI-compatible endpoints the host classifier cannot
// name. Invalid values fall back to the classifier with a diagnostic.
func WithProvider(name string) Option {
	return func(o *options) {
		normalized := strings.ToLower(name)
		if !providerShape.MatchString(normalized) {
			// Last-wins includes invalid values: the override is
			// cleared so classification falls back to the host
			// classifier rather than a stale earlier option.
			o.provider = ""
			otel.Handle(errors.New("langfuse contrib: invalid provider override ignored"))
			return
		}
		o.provider = normalized
	}
}

// WithoutBodyInspection prevents the adapter from reading request or
// response bodies at all: no capture buffers exist and only URL-derived
// route information, HTTP status, and timing are recorded.
func WithoutBodyInspection() Option {
	return func(o *options) { o.noBodyInspect = true }
}

// WithoutContentExport keeps bounded body inspection for the
// allowlisted model and usage fields but never exports Input or
// Output. Inspection still occurs; use WithoutBodyInspection to
// prevent it entirely.
func WithoutContentExport() Option {
	return func(o *options) { o.noContentExport = true }
}

// CallAttributes are caller-supplied fields for attempts started under
// a context, the bridge between wire capture and application knowledge
// such as prompt links. Wire-derived facts always win for model,
// usage, timing, and status; CallAttributes win for name, prompt link,
// and the metadata keys they set.
type CallAttributes struct {
	Name     string
	Prompt   *langfuse.PromptRef
	Metadata map[string]any
}

// ContextWithCall scopes call onto every recognized attempt started
// under ctx, with standard context inheritance: derive a child context
// per operation for per-call scoping. The attributes are copied at
// insertion (PromptRef by value, Metadata shallow-cloned); nested
// metadata values must not be mutated afterwards. A nil ctx returns
// nil with a diagnostic.
func ContextWithCall(ctx context.Context, call CallAttributes) context.Context {
	if ctx == nil {
		otel.Handle(errors.New("langfuse contrib: ContextWithCall called with a nil context"))
		return nil
	}
	return wiretap.ContextWithCall(ctx, wiretap.CallAttributes{
		Name:     call.Name,
		Prompt:   call.Prompt,
		Metadata: call.Metadata,
	})
}

// NewTransport returns an http.RoundTripper that records one Langfuse
// observation per recognized OpenAI-wire HTTP attempt passing through
// it and forwards everything else untouched. A nil base means
// http.DefaultTransport. A nil client records nothing and forwards
// everything. NewTransport never fails: invalid options fall back to
// safe defaults with a payload-free diagnostic.
//
// Wrapping a base that is already a transport from this package
// returns that existing transport unchanged with one diagnostic, so a
// shared client decorated twice cannot double-record. Chaining with
// the langfusegenai adapter is supported; each passes unrecognized
// routes through.
func NewTransport(lf *langfuse.Client, base http.RoundTripper, opts ...Option) http.RoundTripper {
	if base != nil && wiretap.IsOwn(base, adapterMarker) {
		otel.Handle(errors.New("langfuse contrib: transport already instrumented; returning existing layer"))
		return base
	}
	var resolved options
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	cfg := wiretap.Config{
		Provider:        resolved.provider,
		NoBodyInspect:   resolved.noBodyInspect,
		NoContentExport: resolved.noContentExport,
		CaptureCap:      wiretap.DefaultCaptureCap,
	}
	if resolved.name != nil {
		callback := resolved.name
		cfg.Name = func(route wiretap.Route) string {
			return callback(RouteInfo{
				Provider:   route.Provider,
				Route:      route.Name,
				APIVersion: route.APIVersion,
				Model:      route.Model,
			})
		}
	}
	return wiretap.NewRoundTripper(lf, base, protocol{captureCap: cfg.CaptureCap}, cfg, adapterMarker)
}
