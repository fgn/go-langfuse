package wiretap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
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
	// CaptureDegraded reports that bounded capture dropped content (an
	// oversized SSE event or unary body); mapped to telemetry_partial.
	CaptureDegraded bool
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

	mu         sync.Mutex
	finalized  bool
	frozen     bool // hard terminal seen; reads keep flowing untouched
	parsePanic bool // a parser defect degraded telemetry
	framer     sseFramer
	sniff      []byte // bounded undecided prefix awaiting SSE/JSON proof
	unary      []byte
	unaryOver  bool
	capBytes   int
	status     int

	completionStart time.Time
	cancelObserved  *atomic.Bool
	ctxDone         func() bool
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
		ctxDone:        func() bool { return ctx.Err() != nil },
		ctxErr:         func() error { return context.Cause(ctx) },
		finalize:       finalize,
	}
	w.framer.maxEvent = maxEvent
	// Some SDK retry loops abandon a failed attempt's response body
	// without reading or closing it (the official openai-go does).
	// Without a safety net that attempt's observation would never end
	// and the failure would silently vanish from telemetry. The
	// finalizer closes the leaked body and finalizes as closed_early;
	// it is cleared on every normal finalization path, so it only ever
	// fires for genuinely abandoned bodies, at GC time.
	runtime.SetFinalizer(w, (*bodyWrapper).abandon)
	return w
}

// abandon is the GC safety net for response bodies dropped without
// Close. It closes the leaked underlying body (releasing the
// connection) and finalizes the attempt with the observable
// closed_early state; the non-2xx precedence rule then reports the
// wire status for abandoned failed attempts.
func (w *bodyWrapper) abandon() {
	w.mu.Lock()
	finalized := w.finalized
	w.mu.Unlock()
	if finalized {
		return
	}
	diagnose("response body abandoned without Close; attempt finalized by safety net")
	_ = w.rc.Close()
	w.closeTelemetry()
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
		// Buffer a bounded prefix until SSE versus JSON is decisive:
		// a first read of "d" or "da" must not misclassify the stream.
		w.sniff = append(w.sniff, p...)
		w.mode = sniffDecide(w.sniff)
		if w.mode == modeUndecided {
			if len(w.sniff) > 16 {
				w.mode = modeUnary
			} else {
				return
			}
		}
		p = w.sniff
		w.sniff = nil
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
				body := w.unary
				if w.mode == modeUndecided {
					body = w.sniff // short body that never left sniffing
				}
				w.call.FinishUnary(body, w.status)
			}
			w.finalizeLocked(StateComplete)
			return
		}
		if w.mode == modeIgnore {
			w.finalizeLocked(StateComplete)
			return
		}
		// SSE stream ended cleanly. Success requires both the
		// protocol's judgment (via a zero-data probe: a sentinel
		// protocol needs its sentinel) and framing completeness (a
		// truncated final frame is incomplete even for sentinel-free
		// protocols).
		if w.call.FeedEvent(nil).Terminal == TerminalSuccess && !w.framer.pending() {
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

// closeTelemetry handles Close before a transport-terminal event.
// Unary JSON consumers routinely decode exactly one value and close
// without ever reading EOF (json.Decoder does precisely this; real
// keep-alive connections then never surface io.EOF, unlike httptest
// servers that deliver it with the final bytes). When the captured
// unary bytes already form a complete JSON document, the protocol
// finished and Close is completion. Anything else remains the
// observable closed_early fact.
func (w *bodyWrapper) closeTelemetry() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return
	}
	if w.mode == modeUnary || w.mode == modeUndecided {
		body := w.unary
		if w.mode == modeUndecided {
			body = w.sniff
		}
		if !w.unaryOver && len(body) > 0 && json.Valid(body) {
			defer w.recoverParse()
			w.call.FinishUnary(body, w.status)
			w.finalizeLocked(StateComplete)
			return
		}
	}
	w.finalizeLocked(StateClosedEarly)
}

// causallyCanceled requires a compatible terminal error together with
// the context actually being done at classification time. Checking the
// context directly is deterministic where the AfterFunc flag, which
// runs on its own goroutine, can lag the error by a few instructions;
// the flag still feeds the CancelObserved metadata.
func (w *bodyWrapper) causallyCanceled(err error) bool {
	if !w.ctxDone() {
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
	runtime.SetFinalizer(w, nil)
	w.finalize(Outcome{
		State:           state,
		CancelObserved:  w.cancelObserved.Load() || w.ctxDone(),
		CaptureDegraded: w.framer.discarded || w.unaryOver || w.parsePanic,
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
		w.parsePanic = true
	}
}

// sniffDecide classifies a buffered prefix: JSON bodies start with a
// JSON value; SSE bodies start with a comment or a field name. A
// prefix that is still a proper prefix of an SSE field name stays
// undecided until more bytes arrive (bounded by the caller).
func sniffDecide(prefix []byte) streamMode {
	trimmed := bytes.TrimLeft(prefix, " \t\r\n")
	if len(trimmed) == 0 {
		return modeUndecided
	}
	if trimmed[0] == ':' {
		return modeSSE
	}
	for _, field := range []string{"data", "event", "id", "retry"} {
		name := []byte(field)
		switch {
		case bytes.HasPrefix(trimmed, name):
			return modeSSE
		case bytes.HasPrefix(name, trimmed):
			return modeUndecided
		}
	}
	return modeUnary
}
