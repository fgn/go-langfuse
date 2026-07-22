// Package wiretap is the provider-agnostic transport machinery shared by
// the go-langfuse provider adapters: request-body capture with replay
// coherence, the response-body lifecycle state machine, the incremental
// SSE framer, and the observation mapping.
//
// This package is maintained as a single source of truth and mirrored
// byte-for-byte between the contrib modules; a CI gate asserts the copies
// are identical. Edit the copy under contrib/openai and run the sync task.
package wiretap

import (
	"context"
	"maps"
	"net/http"
	"net/url"

	"github.com/fgn/go-langfuse"
)

// Route describes one recognized provider API route. It is derived from
// the request URL only, before any body is read, and is the basis of the
// sanitized RouteInfo handed to caller naming callbacks.
type Route struct {
	// Provider is the classified provider label, for example "openai",
	// "azure-openai", "google-openai-compat", or "google-genai".
	Provider string
	// Name identifies the route, for example "openai.chat.completions".
	// It is the default observation name.
	Name string
	// APIVersion is the API version derived from the URL, when present.
	APIVersion string
	// Model is the model derived from the URL, when present (Google
	// routes carry it there; OpenAI routes usually do not, so naming
	// callbacks must tolerate an empty model).
	Model string
	// Type is the observation type this route maps to.
	Type langfuse.ObservationType
	// Streaming reports that the URL shape alone promises a streaming
	// response (for example :streamGenerateContent or alt=sse). OpenAI
	// streaming is discovered from the response Content-Type instead.
	Streaming bool
	// Metadata carries route-derived metadata beyond the standard keys,
	// for example an Azure deployment name. Values are URL-derived only.
	Metadata map[string]any
}

// Protocol is implemented once per provider wire format.
type Protocol interface {
	// Recognize inspects a request URL and reports whether the adapter
	// instruments it. It must not read the request body.
	Recognize(u *url.URL) (Route, bool)
	// NewCall returns the parser state for one recognized attempt.
	NewCall(route Route) Call
}

// Call parses one attempt's request and response bytes. Implementations
// are used by a single goroutine at a time (the wrapper serializes
// callbacks) and must be cheap to allocate.
type Call interface {
	// ParseRequest receives the completely transmitted request body.
	// It is invoked at most once, only after the request body was fully
	// written and closed, and never for over-cap or inspection-disabled
	// captures.
	ParseRequest(body []byte)
	// FeedEvent receives one SSE event's data payload. Comment events
	// are filtered by the framer and never reach the parser. The
	// verdict reports a proven hard terminal and whether the event
	// carried output-bearing content; the body wrapper, which owns the
	// clock, stamps CompletionStartTime at the read that completed the
	// first output-bearing event.
	FeedEvent(data []byte) EventVerdict
	// FinishUnary receives the complete unary response body at clean
	// EOF. Malformed bodies must degrade to partial telemetry, never to
	// an error.
	FinishUnary(body []byte, httpStatus int)
	// Result returns everything parsed so far. It is called exactly once
	// when the attempt's telemetry finalizes.
	Result() Result
}

// EventVerdict is a parser's judgment of one SSE event.
type EventVerdict struct {
	Terminal Terminal
	// Output reports a non-role, non-control, output-bearing field:
	// text, refusal, audio, or tool-call argument deltas for OpenAI,
	// and any output Part variant for Gemini.
	Output bool
}

// Terminal is a protocol-level terminal judgment from a parser.
type Terminal int

const (
	// TerminalNone means the stream continues.
	TerminalNone Terminal = iota
	// TerminalSuccess is a hard protocol success terminal, such as the
	// OpenAI "data: [DONE]" sentinel.
	TerminalSuccess
	// TerminalError is a protocol-level error event on an otherwise
	// successful HTTP response.
	TerminalError
)

// Result is the provider-parsed contribution to one observation.
type Result struct {
	Input any
	// Output and Input are exported only through Mask-governed fields.
	Output any
	// Model is the validated RESPONSE model only. It is the sole
	// body-derived value eligible for the unmasked Model field; request
	// models are never promoted (they travel as Mask-governed
	// metadata via RequestModel).
	Model string
	// RequestModel is the model named by the request body or URL,
	// recorded as metadata when it differs from the response model.
	RequestModel    string
	ModelParameters map[string]any
	Usage           *langfuse.Usage
	Metadata        map[string]any
	// ErrorCategory is a fixed adapter-owned status such as
	// "provider error"; empty means no protocol error was proven.
	ErrorCategory string
	// TelemetryPartial reports that parsing degraded and telemetry is
	// incomplete, without implying an application failure.
	TelemetryPartial bool
}

// Config is the resolved option state shared by the adapters. The
// public per-module Option functions mutate it.
type Config struct {
	Name            func(Route) string
	Provider        string
	NoBodyInspect   bool
	NoContentExport bool
	CaptureCap      int
}

// DefaultCaptureCap bounds captured request and response content, per
// direction. Content beyond the cap is dropped entirely (never
// truncated), matching the core omission contract.
const DefaultCaptureCap = 512 << 10

// CallAttributes are caller-supplied per-context fields merged into
// recognized attempts. Wire-derived fields always win for model, usage,
// timing, and status; call attributes win for name, prompt link, and
// the metadata keys they set.
type CallAttributes struct {
	Name     string
	Prompt   *langfuse.PromptRef
	Metadata map[string]any
}

type callAttrKey struct{}

// ContextWithCall stores an insertion-time copy of call. A nil ctx
// returns nil (the public wrappers diagnose it).
func ContextWithCall(ctx context.Context, call CallAttributes) context.Context {
	if ctx == nil {
		return nil
	}
	copied := CallAttributes{Name: call.Name}
	if call.Prompt != nil {
		ref := *call.Prompt
		copied.Prompt = &ref
	}
	if len(call.Metadata) != 0 {
		meta := make(map[string]any, len(call.Metadata))
		maps.Copy(meta, call.Metadata)
		copied.Metadata = meta
	}
	return context.WithValue(ctx, callAttrKey{}, copied)
}

func callFromContext(ctx context.Context) (CallAttributes, bool) {
	call, ok := ctx.Value(callAttrKey{}).(CallAttributes)
	return call, ok
}

// wrapped marks a RoundTripper created by this adapter so that direct
// same-adapter double wrapping can be detected and refused.
type wrapped interface{ wiretapAdapter() string }

// IsOwn reports whether rt was produced by this adapter package (the
// same synced copy, identified by marker).
func IsOwn(rt http.RoundTripper, marker string) bool {
	own, ok := rt.(wrapped)
	return ok && own.wiretapAdapter() == marker
}
