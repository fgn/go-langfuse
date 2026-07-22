package wiretap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// FinalState classifies how one attempt's response ended. Only
// wire-provable outcomes are asserted; the vocabulary is locked by the
// design review (telemetry_partial is carried separately in Result).
type FinalState int

const (
	// StateComplete is a protocol-proven success terminal or a clean
	// unary EOF.
	StateComplete FinalState = iota
	// StateFailed covers non-2xx status, transport-level terminal read
	// errors, and protocol error events on 2xx streams.
	StateFailed
	// StateIncomplete is a clean transport end before a required
	// protocol terminal, or a truncated final frame.
	StateIncomplete
	// StateCanceled requires causal evidence: cancellation observed
	// before termination and a compatible terminal error.
	StateCanceled
	// StateClosedEarly is a Close before any terminal event. It states
	// the observable fact and never infers intent.
	StateClosedEarly
)

// Outcome is delivered to the transport's finalize function exactly
// once per attempt.
type Outcome struct {
	State FinalState
	// CancelObserved reports that context cancellation was observed at
	// any point before finalization; it accompanies ambiguous races as
	// metadata without asserting cause.
	CancelObserved bool
	// End is the time the terminal event was observed.
	End time.Time
	// CompletionStart is the read-completion time of the first
	// output-bearing event; zero when none was seen.
	CompletionStart time.Time
}

// streamMode selects the parse strategy for a response body.
type streamMode int

const (
	modeUndecided streamMode = iota // sniff on first read
	modeUnary
	modeSSE
	modeIgnore // WithoutBodyInspection: track lifecycle only
)

// bodyWrapper drives one attempt's response lifecycle. It serves reads
// with exact (n, err) passthrough, feeds parsers under a mutex, ends
// telemetry exactly once, and never delays the underlying Close.
type bodyWrapper struct {
	rc   io.ReadCloser
	call Call
	mode streamMode

	mu        sync.Mutex
	finalized bool
	frozen    bool // hard terminal seen; reads keep flowing untouched
	framer    sseFramer
	unary     []byte
	unaryOver bool
	capBytes  int
	status    int

	completionStart time.Time
	cancelObserved  *atomic.Bool
	ctxErr          func() error
	finalize        func(Outcome)
}

// newBodyWrapper receives the transport-owned cancellation observer,
// which was registered via context.AfterFunc before the exchange began,
// strictly ordering the observation against terminal classification.
func newBodyWrapper(
	ctx context.Context,
	rc io.ReadCloser,
	call Call,
	mode streamMode,
	capBytes int,
	status int,
	maxEvent int,
	cancelObserved *atomic.Bool,
	finalize func(Outcome),
) *bodyWrapper {
	w := &bodyWrapper{
		rc:             rc,
		call:           call,
		mode:           mode,
		capBytes:       capBytes,
		status:         status,
		cancelObserved: cancelObserved,
		ctxErr:         func() error { return context.Cause(ctx) },
		finalize:       finalize,
	}
	w.framer.maxEvent = maxEvent
	return w
}

func (w *bodyWrapper) Read(p []byte) (int, error) {
	n, err := w.rc.Read(p)
	if n > 0 {
		w.process(p[:n])
	}
	if err != nil {
		w.terminalRead(err)
	}
	return n, err
}

// Close always delegates to the underlying body first, then finalizes
// telemetry, so connection reuse and stream teardown are never delayed
// by export work.
func (w *bodyWrapper) Close() error {
	err := w.rc.Close()
	w.closeTelemetry()
	return err
}

// process feeds captured bytes to the active parse strategy. Parsing
// runs under recover so a parser defect degrades telemetry, never the
// application's read.
func (w *bodyWrapper) process(p []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized || w.frozen || w.mode == modeIgnore {
		return
	}
	defer w.recoverParse()
	if w.mode == modeUndecided {
		w.mode = sniffMode(p)
	}
	switch w.mode {
	case modeUnary:
		if w.unaryOver {
			return
		}
		if len(w.unary)+len(p) > w.capBytes {
			// Drop, never truncate; scanning cannot continue for a
			// unary body, so telemetry for content degrades.
			w.unary = nil
			w.unaryOver = true
			return
		}
		w.unary = append(w.unary, p...)
	case modeUndecided, modeIgnore:
	case modeSSE:
		readTime := time.Now()
		w.framer.feed(p, func(data []byte) bool {
			verdict := w.call.FeedEvent(data)
			if verdict.Output && w.completionStart.IsZero() {
				w.completionStart = readTime
			}
			switch verdict.Terminal {
			case TerminalSuccess:
				w.finalizeLocked(StateComplete)
				return false
			case TerminalError:
				w.finalizeLocked(StateFailed)
				return false
			case TerminalNone:
			}
			return true
		})
	}
}

// terminalRead classifies a read error. EOF is protocol-sensitive:
// unary and sentinel-free SSE complete, sentinel-requiring SSE that
// never saw its terminal is incomplete.
func (w *bodyWrapper) terminalRead(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return
	}
	if errors.Is(err, io.EOF) {
		defer w.recoverParse()
		if w.mode == modeUnary || w.mode == modeUndecided {
			if !w.unaryOver {
				w.call.FinishUnary(w.unary, w.status)
			}
			w.finalizeLocked(StateComplete)
			return
		}
		if w.mode == modeIgnore {
			w.finalizeLocked(StateComplete)
			return
		}
		// SSE stream ended cleanly. Whether that is success depends on
		// the protocol: the parser reports, via a zero-data probe, if a
		// hard sentinel was required and missing.
		if w.call.FeedEvent(nil).Terminal == TerminalSuccess {
			w.finalizeLocked(StateComplete)
		} else {
			w.finalizeLocked(StateIncomplete)
		}
		return
	}
	if w.causallyCanceled(err) {
		w.finalizeLocked(StateCanceled)
		return
	}
	w.finalizeLocked(StateFailed)
}

// closeTelemetry handles Close before any terminal event: the
// observable fact is closed_early; cancellation is asserted only with
// causal evidence recorded separately as metadata by the transport.
func (w *bodyWrapper) closeTelemetry() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return
	}
	w.finalizeLocked(StateClosedEarly)
}

// causallyCanceled requires both the ordered cancellation observation
// and a compatible error, per the design's conservative rule.
func (w *bodyWrapper) causallyCanceled(err error) bool {
	if !w.cancelObserved.Load() {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	cause := w.ctxErr()
	return cause != nil && errors.Is(err, cause)
}

func (w *bodyWrapper) finalizeLocked(state FinalState) {
	if w.finalized {
		return
	}
	if state == StateComplete || state == StateFailed {
		// Hard terminals freeze telemetry while the body keeps
		// serving reads transparently (official openai-go drains
		// after [DONE]); Close after a terminal is a no-op.
		w.frozen = true
	}
	w.finalized = true
	w.finalize(Outcome{
		State:           state,
		CancelObserved:  w.cancelObserved.Load(),
		End:             time.Now(),
		CompletionStart: w.completionStart,
	})
}

// recoverParse contains parser panics: the attempt still records
// timing, route, and status, and the read path is never failed by
// telemetry (the recovered panic downgrades to partial telemetry
// through the parser-independent finalize path).
func (w *bodyWrapper) recoverParse() {
	if recovered := recover(); recovered != nil {
		w.mode = modeIgnore
	}
}

// sniffMode decides SSE versus unary from leading bytes when neither
// the URL shape nor the Content-Type answered: SSE bodies begin with a
// field name or comment; JSON bodies begin with a JSON value.
func sniffMode(p []byte) streamMode {
	trimmed := bytes.TrimLeft(p, " \t\r\n")
	if len(trimmed) == 0 {
		return modeUndecided
	}
	if trimmed[0] == ':' || bytes.HasPrefix(trimmed, []byte("data")) ||
		bytes.HasPrefix(trimmed, []byte("event")) {
		return modeSSE
	}
	return modeUnary
}
