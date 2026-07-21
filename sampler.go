package langfuse

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// traceDecision records one sampling decision on a context path. traceID and
// sampled are immutable along the path once the first SDK observation of a
// trace has decided. authoritative and lastSDKSpanID exist only for score
// suppression: authoritative starts true when the SDK originated the trace
// (the ambient parent span context was invalid at the root) and is cleared
// permanently for a path and its descendants once a foreign span interposes,
// because a foreign producer may export part of the trace and path-local
// state cannot prove otherwise.
type traceDecision struct {
	traceID       oteltrace.TraceID
	sampled       bool
	authoritative bool
	lastSDKSpanID oteltrace.SpanID
}

type traceDecisionContextKey struct{ client *Client }

// sampler decides trace sampling for an isolated (SDK-owned) provider. A
// decision already recorded on the context path is inherited by trace ID so
// every SDK observation in one locally coordinated trace agrees even when the
// requested rate changes mid-trace or a foreign span interposes. Otherwise
// the decision is a deterministic threshold on the trace ID, so independent
// deciders using equal fractions always agree. Both branches preserve the
// parent's TraceState, matching AlwaysSample and TraceIDRatioBased.
type sampler struct {
	client          *Client
	defaultFraction float64
}

var _ sdktrace.Sampler = sampler{}

func (s sampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	state := oteltrace.SpanContextFromContext(p.ParentContext).TraceState()
	var sampled bool
	if decision, ok := p.ParentContext.Value(traceDecisionContextKey{client: s.client}).(traceDecision); ok &&
		decision.traceID == p.TraceID {
		sampled = decision.sampled
	} else {
		fraction := s.defaultFraction
		if override, ok := p.ParentContext.Value(sampleRateContextKey{client: s.client}).(float64); ok {
			fraction = override
		}
		sampled = sampledAt(p.TraceID, fraction)
	}
	if sampled {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: state}
}

func (s sampler) Description() string {
	return fmt.Sprintf("LangfuseSampler{%g}", s.defaultFraction)
}

// sampledAt is the deterministic threshold shared by the sampler and
// TraceSampledAt. It matches OpenTelemetry's TraceIDRatioBased bit-for-bit so
// decisions agree with standard OTel ratio sampling and nest across
// fractions. Callers validate fraction; out-of-range values saturate.
func sampledAt(traceID oteltrace.TraceID, fraction float64) bool {
	if fraction >= 1 {
		return true
	}
	if fraction <= 0 {
		return false
	}
	x := binary.BigEndian.Uint64(traceID[8:16]) >> 1
	return x < uint64(fraction*(1<<63))
}

// validSampleFraction reports whether fraction is finite and within [0, 1].
// NaN compares false with everything, so the range checks exclude it and both
// infinities without a separate test.
func validSampleFraction(fraction float64) bool {
	return fraction >= 0 && fraction <= 1 && !math.IsNaN(fraction)
}

// TraceSampledAt reports whether the trace identified by the 32-character
// lowercase hex traceID falls inside fraction under the SDK's deterministic
// threshold scheme — the same scheme the isolated-mode sampler uses, shared
// with OpenTelemetry's TraceIDRatioBased. A trace selected at a smaller
// fraction is always selected at any larger one, so work gated at a small
// fraction (an expensive evaluation on 2% of traces) runs only on traces that
// a larger export fraction also kept. It returns an error for a malformed
// trace ID or a fraction that is not finite or not within [0, 1]; it never
// guesses. It is a pure predicate on the trace ID and does not know what rate
// the trace was actually sampled at: when gating work on an existing
// observation, gate on Observation.Sampled and on the returned decision
// together, after checking the error.
func TraceSampledAt(traceID string, fraction float64) (bool, error) {
	if !validSampleFraction(fraction) {
		return false, errors.New("langfuse: sample fraction must be finite and within [0, 1]")
	}
	id, err := oteltrace.TraceIDFromHex(traceID)
	if err != nil {
		return false, errors.New("langfuse: trace ID must be 32 lowercase hex characters")
	}
	return sampledAt(id, fraction), nil
}
