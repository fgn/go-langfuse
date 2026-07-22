package wiretap

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"mime"
	"net/http"
	"regexp"
	"sync/atomic"

	"go.opentelemetry.io/otel"

	"github.com/fgn/go-langfuse"
)

// maxSSEEvent bounds one buffered SSE event; larger events abandon
// content capture while framing resynchronizes. Terminal sentinels are
// tiny and survive discards.
const maxSSEEvent = 256 << 10

// responseModelShape validates a body-derived model string before it is
// promoted to ObservationAttributes.Model, the documented single
// exception to the Mask-governed field boundary. Anything else lands in
// metadata instead.
var responseModelShape = regexp.MustCompile(`^[A-Za-z0-9._:/-]{1,128}$`)

// Transport is the shared RoundTripper implementation behind each
// adapter's NewTransport.
type Transport struct {
	lf     *langfuse.Client
	base   http.RoundTripper
	proto  Protocol
	cfg    Config
	marker string

	nameBroken atomic.Bool
}

// NewRoundTripper builds the shared transport. The public adapter
// constructors are thin wrappers that resolve options and the
// double-wrap guard around this.
func NewRoundTripper(lf *langfuse.Client, base http.RoundTripper, proto Protocol, cfg Config, marker string) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}
	if cfg.CaptureCap <= 0 {
		cfg.CaptureCap = DefaultCaptureCap
	}
	return &Transport{lf: lf, base: base, proto: proto, cfg: cfg, marker: marker}
}

func (t *Transport) wiretapAdapter() string { return t.marker }

// RoundTrip implements the reviewed attempt flow: recognize the URL,
// start the observation, take the no-op fast path when the client
// records nothing, otherwise propagate the observation context and
// capture exactly what the wire carries. Telemetry work never fails,
// mutates, or delays the underlying exchange beyond the documented
// bounded copies.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.lf == nil {
		return t.base.RoundTrip(req)
	}
	route, ok := t.proto.Recognize(req.URL)
	if !ok {
		return t.base.RoundTrip(req)
	}
	if t.cfg.Provider != "" {
		route.Provider = t.cfg.Provider
	}

	ctx := req.Context()
	call, _ := callFromContext(ctx)
	obsCtx, obs := t.lf.StartObservation(ctx, t.observationName(route, call), route.Type,
		langfuse.ObservationAttributes{
			Model:    route.Model,
			Prompt:   call.Prompt,
			Metadata: startMetadata(route, call),
		})

	// No-op fast path: a nil, zero, disabled, or shut-down client
	// returns the no-op observation, identified by its documented
	// empty trace ID. The original request passes through untouched:
	// no clone, no tee, no parsing.
	if obs.TraceID() == "" {
		return t.base.RoundTrip(req)
	}

	// The clone carries the observation context so downstream spans
	// (auth transports, inner otelhttp) parent under the generation,
	// and, on the unsampled path, inherit the dropped trace rather
	// than making a fresh sampling decision.
	clone := req.Clone(obsCtx)

	// Sampled-out attempts skip all capture: the observation is a
	// cheap no-op and content work would be pure waste.
	if !obs.Sampled() {
		resp, err := t.base.RoundTrip(clone)
		obs.End()
		return resp, err
	}

	cancelObserved := &atomic.Bool{}
	stopCancelWatch := context.AfterFunc(obsCtx, func() { cancelObserved.Store(true) })

	parser := t.proto.NewCall(route)
	recorder := &requestRecorder{cap: t.cfg.CaptureCap}
	if !t.cfg.NoBodyInspect {
		recorder.instrument(clone)
	}

	resp, err := t.base.RoundTrip(clone)
	if err != nil {
		t.finalizeTransportError(obs, cancelObserved, stopCancelWatch)
		return resp, err
	}

	finalize := t.newFinalizer(obs, parser, recorder, resp.StatusCode, stopCancelWatch)
	mode := t.selectMode(route, resp)
	resp.Body = newBodyWrapper(obsCtx, resp.Body, parser, mode, t.cfg.CaptureCap,
		resp.StatusCode, maxSSEEvent, cancelObserved, finalize)
	return resp, nil
}

// observationName resolves precedence: per-context call attributes win
// over the naming option, which wins over the route default. A caller
// callback that panics is disabled for the transport's lifetime.
func (t *Transport) observationName(route Route, call CallAttributes) string {
	if call.Name != "" {
		return call.Name
	}
	if t.cfg.Name != nil && !t.nameBroken.Load() {
		if name, ok := t.safeName(route); ok && name != "" {
			return name
		}
	}
	return route.Name
}

func (t *Transport) safeName(route Route) (name string, ok bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if t.nameBroken.CompareAndSwap(false, true) {
				diagnose("observation name callback panicked; using default names")
			}
			name, ok = "", false
		}
	}()
	return t.cfg.Name(route), true
}

// finalizeTransportError ends an attempt whose exchange failed before
// any response arrived. The error text is never exported; the fixed
// category and the causal cancellation rule apply.
func (t *Transport) finalizeTransportError(obs *langfuse.Observation, cancelObserved *atomic.Bool, stop func() bool) {
	defer stop()
	status := "transport error"
	level := langfuse.LevelError
	if cancelObserved.Load() {
		status = "canceled"
		level = langfuse.LevelWarning
	}
	obs.Update(langfuse.ObservationAttributes{Level: level, StatusMessage: status})
	if level == langfuse.LevelError {
		obs.RecordError(errors.New(status))
	}
	obs.End()
}

// newFinalizer builds the exactly-once completion function that maps an
// Outcome plus the parser result onto the observation and ends it.
func (t *Transport) newFinalizer(
	obs *langfuse.Observation,
	parser Call,
	recorder *requestRecorder,
	httpStatus int,
	stopCancelWatch func() bool,
) func(Outcome) {
	return func(outcome Outcome) {
		defer stopCancelWatch()
		defer func() {
			if recovered := recover(); recovered != nil {
				diagnose("attempt finalization panicked; observation ended with partial telemetry")
			}
			obs.EndAt(outcome.End)
		}()

		if body, ok := recorder.snapshot(); ok {
			safeParseRequest(parser, body)
		} else if recorder.overCapped() {
			diagnose("request capture exceeded the size cap; input omitted")
		}

		result := safeResult(parser)
		update := langfuse.ObservationAttributes{
			ModelParameters: result.ModelParameters,
			Usage:           result.Usage,
			Metadata:        result.Metadata,
		}
		if update.Metadata == nil {
			update.Metadata = map[string]any{}
		}
		update.Metadata["http_status"] = httpStatus
		if !t.cfg.NoContentExport && !t.cfg.NoBodyInspect {
			update.Input = result.Input
			update.Output = result.Output
		}
		if result.Model != "" {
			if responseModelShape.MatchString(result.Model) {
				update.Model = result.Model
			} else {
				if update.Metadata == nil {
					update.Metadata = map[string]any{}
				}
				update.Metadata["unvalidated_model"] = result.Model
			}
		}
		if !outcome.CompletionStart.IsZero() {
			update.CompletionStartTime = outcome.CompletionStart
		}

		state := outcome.State
		if state == StateComplete && result.ErrorCategory != "" {
			state = StateFailed
		}
		switch state {
		case StateComplete:
			// The status model marks every non-2xx as a wire-provable
			// failure with its fixed category, including redirect hops.
			if httpStatus >= 300 {
				update.Level = langfuse.LevelError
				update.StatusMessage = httpStatusCategory(httpStatus)
			} else if result.TelemetryPartial {
				update.Level = langfuse.LevelWarning
				update.StatusMessage = "telemetry_partial"
			}
		case StateFailed:
			update.Level = langfuse.LevelError
			update.StatusMessage = failureCategory(httpStatus, result.ErrorCategory)
		case StateIncomplete:
			update.Level = langfuse.LevelWarning
			update.StatusMessage = "incomplete"
		case StateCanceled:
			update.Level = langfuse.LevelWarning
			update.StatusMessage = "canceled"
		case StateClosedEarly:
			update.Level = langfuse.LevelWarning
			update.StatusMessage = "closed_early"
			if outcome.CancelObserved {
				if update.Metadata == nil {
					update.Metadata = map[string]any{}
				}
				update.Metadata["context_canceled_observed"] = true
			}
		}
		obs.Update(update)
		if update.Level == langfuse.LevelError {
			obs.RecordError(errors.New(update.StatusMessage))
		}
	}
}

// selectMode chooses the parse strategy from configuration, the URL
// shape, and the response framing headers (Content-Type and
// Content-Encoding, the only headers the adapter inspects; neither is
// exported). Caller-managed compression skips capture entirely.
func (t *Transport) selectMode(route Route, resp *http.Response) streamMode {
	if t.cfg.NoBodyInspect {
		return modeIgnore
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		diagnose("caller-managed content encoding; capture skipped for this attempt")
		return modeIgnore
	}
	mediaType := ""
	if raw := resp.Header.Get("Content-Type"); raw != "" {
		if parsed, _, err := mime.ParseMediaType(raw); err == nil {
			mediaType = parsed
		}
	}
	switch {
	case mediaType == "text/event-stream":
		return modeSSE
	case route.Streaming:
		return modeSSE
	case mediaType == "application/json":
		return modeUnary
	default:
		return modeUndecided
	}
}

func startMetadata(route Route, call CallAttributes) map[string]any {
	meta := map[string]any{
		"provider": route.Provider,
		"route":    route.Name,
	}
	if route.APIVersion != "" {
		meta["api_version"] = route.APIVersion
	}
	maps.Copy(meta, route.Metadata)
	maps.Copy(meta, call.Metadata)
	return meta
}

func httpStatusCategory(status int) string {
	return fmt.Sprintf("http %d", status)
}

func failureCategory(httpStatus int, parserCategory string) string {
	if httpStatus >= 300 {
		return httpStatusCategory(httpStatus)
	}
	if parserCategory != "" {
		return parserCategory
	}
	return "transport error"
}

// safeParseRequest and safeResult contain parser defects so telemetry
// degrades instead of breaking finalization.
func safeParseRequest(parser Call, body []byte) {
	defer func() { _ = recover() }()
	parser.ParseRequest(body)
}

func safeResult(parser Call) (result Result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Result{TelemetryPartial: true}
		}
	}()
	return parser.Result()
}

// diagnose emits a payload-free diagnostic through the OpenTelemetry
// error handler, matching the core SDK's diagnostic channel.
func diagnose(message string) {
	otel.Handle(errors.New("langfuse contrib: " + message))
}
