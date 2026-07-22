package wiretap

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedCall is a minimal parser: [DONE]-style sentinel semantics
// with configurable output-bearing events, mirroring the OpenAI
// terminal table.
type scriptedCall struct {
	sentinel     string
	sentinelSeen bool
	events       []string
	unary        []byte
	unaryStatus  int
}

func (c *scriptedCall) ParseRequest([]byte) {}

func (c *scriptedCall) FeedEvent(data []byte) EventVerdict {
	if data == nil {
		if c.sentinel == "" || c.sentinelSeen {
			return EventVerdict{Terminal: TerminalSuccess}
		}
		return EventVerdict{}
	}
	payload := string(data)
	c.events = append(c.events, payload)
	if c.sentinel != "" && payload == c.sentinel {
		c.sentinelSeen = true
		return EventVerdict{Terminal: TerminalSuccess}
	}
	if payload == "ERROR" {
		return EventVerdict{Terminal: TerminalError}
	}
	return EventVerdict{Output: strings.HasPrefix(payload, "delta")}
}

func (c *scriptedCall) FinishUnary(body []byte, status int) {
	c.unary = append([]byte(nil), body...)
	c.unaryStatus = status
}

func (c *scriptedCall) Result() Result { return Result{} }

// errReader yields its payload, then the given terminal error.
type errReader struct {
	data []byte
	err  error
	done bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if !r.done && len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		if len(r.data) == 0 {
			r.done = true
			// n > 0 with the terminal error, the case the review
			// demanded is processed bytes-first.
			return n, r.err
		}
		return n, nil
	}
	return 0, r.err
}

func (r *errReader) Close() error { return nil }

type collected struct {
	outcome Outcome
	count   int
}

func drive(
	t *testing.T,
	ctx context.Context,
	body io.ReadCloser,
	call Call,
	mode streamMode,
	cancelObserved *atomic.Bool,
) (*collected, *bodyWrapper) {
	t.Helper()
	got := &collected{}
	if cancelObserved == nil {
		cancelObserved = &atomic.Bool{}
	}
	wrapper := newBodyWrapper(ctx, body, call, mode, 1<<16, 200, 1<<16, cancelObserved,
		func(outcome Outcome) {
			got.outcome = outcome
			got.count++
		})
	return got, wrapper
}

func TestBodySSESentinelEndsAndFreezes(t *testing.T) {
	stream := "data: delta-1\n\ndata: [DONE]\n\ndata: drained-after\n\n"
	call := &scriptedCall{sentinel: "[DONE]"}
	got, wrapper := drive(t, t.Context(), io.NopCloser(strings.NewReader(stream)), call, modeSSE, nil)

	data, err := io.ReadAll(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != stream {
		t.Fatalf("stream bytes altered: %q", data)
	}
	if got.count != 1 || got.outcome.State != StateComplete {
		t.Fatalf("outcome = %+v x%d, want one StateComplete", got.outcome, got.count)
	}
	// Post-terminal drain must not feed the parser.
	for _, event := range call.events {
		if event == "drained-after" {
			t.Fatal("parser fed after hard terminal")
		}
	}
	// Close after terminal is a no-op.
	if err := wrapper.Close(); err != nil || got.count != 1 {
		t.Fatalf("post-terminal Close changed outcome: %v x%d", err, got.count)
	}
}

func TestBodySSEEOFWithoutSentinelIsIncomplete(t *testing.T) {
	stream := "data: delta-1\n\n"
	call := &scriptedCall{sentinel: "[DONE]"}
	got, wrapper := drive(t, t.Context(), io.NopCloser(strings.NewReader(stream)), call, modeSSE, nil)
	_, _ = io.ReadAll(wrapper)
	if got.count != 1 || got.outcome.State != StateIncomplete {
		t.Fatalf("EOF without sentinel: %+v x%d, want StateIncomplete", got.outcome, got.count)
	}
}

func TestBodySSEEOFWithoutSentinelRequirementCompletes(t *testing.T) {
	stream := "data: delta-1\n\n"
	call := &scriptedCall{} // Gemini-style: clean EOF is success
	got, wrapper := drive(t, t.Context(), io.NopCloser(strings.NewReader(stream)), call, modeSSE, nil)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.State != StateComplete {
		t.Fatalf("sentinel-free EOF: %+v, want StateComplete", got.outcome)
	}
}

func TestBodyUnaryEOFDeliversBody(t *testing.T) {
	call := &scriptedCall{}
	got, wrapper := drive(t, t.Context(), io.NopCloser(strings.NewReader(`{"ok":true}`)), call, modeUnary, nil)
	_, _ = io.ReadAll(wrapper)
	if string(call.unary) != `{"ok":true}` || call.unaryStatus != 200 {
		t.Fatalf("unary body = %q status %d", call.unary, call.unaryStatus)
	}
	if got.outcome.State != StateComplete {
		t.Fatalf("unary outcome %+v", got.outcome)
	}
}

func TestBodyCloseBeforeTerminalIsClosedEarly(t *testing.T) {
	call := &scriptedCall{sentinel: "[DONE]"}
	got, wrapper := drive(t, t.Context(),
		io.NopCloser(strings.NewReader("data: delta-1\n\ndata: more")), call, modeSSE, nil)
	buf := make([]byte, 8)
	_, _ = wrapper.Read(buf)
	if err := wrapper.Close(); err != nil {
		t.Fatal(err)
	}
	if got.count != 1 || got.outcome.State != StateClosedEarly {
		t.Fatalf("early close: %+v x%d, want StateClosedEarly", got.outcome, got.count)
	}
}

// TestBodyCausalCancellation locks the conservative rule: canceled
// requires the ordered observation plus a compatible error.
func TestBodyCausalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancelObserved := &atomic.Bool{}
	stop := context.AfterFunc(ctx, func() { cancelObserved.Store(true) })
	defer stop()

	call := &scriptedCall{sentinel: "[DONE]"}
	reader := &errReader{data: []byte("data: delta-1\n\n"), err: context.Canceled}
	got, wrapper := drive(t, ctx, io.NopCloser(reader), call, modeSSE, cancelObserved)

	cancel()
	// AfterFunc runs asynchronously; wait for the ordered observation.
	deadline := time.Now().Add(2 * time.Second)
	for !cancelObserved.Load() {
		if time.Now().After(deadline) {
			t.Fatal("cancellation observation never fired")
		}
		time.Sleep(time.Millisecond)
	}
	_, _ = io.ReadAll(wrapper)
	if got.outcome.State != StateCanceled {
		t.Fatalf("causal cancel: %+v, want StateCanceled", got.outcome)
	}
}

// TestBodyErrorWithoutCancelEvidenceIsFailed locks that a compatible
// error alone, without the ordered observation, stays a failure.
func TestBodyErrorWithoutCancelEvidenceIsFailed(t *testing.T) {
	call := &scriptedCall{sentinel: "[DONE]"}
	reader := &errReader{data: []byte("data: delta-1\n\n"), err: context.Canceled}
	got, wrapper := drive(t, t.Context(), io.NopCloser(reader), call, modeSSE, nil)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.State != StateFailed {
		t.Fatalf("uncancelled context.Canceled error: %+v, want StateFailed", got.outcome)
	}
}

// TestBodyCancelObservedCloseStaysClosedEarly locks the ambiguous
// race rule: Close without a compatible error never asserts cause,
// even when cancellation was observed.
func TestBodyCancelObservedCloseStaysClosedEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancelObserved := &atomic.Bool{}
	stop := context.AfterFunc(ctx, func() { cancelObserved.Store(true) })
	defer stop()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for !cancelObserved.Load() {
		if time.Now().After(deadline) {
			t.Fatal("cancellation observation never fired")
		}
		time.Sleep(time.Millisecond)
	}

	call := &scriptedCall{sentinel: "[DONE]"}
	got, wrapper := drive(t, ctx, io.NopCloser(strings.NewReader("data: x\n\n")), call, modeSSE, cancelObserved)
	_ = wrapper.Close()
	if got.outcome.State != StateClosedEarly || !got.outcome.CancelObserved {
		t.Fatalf("ambiguous race: %+v, want StateClosedEarly with CancelObserved", got.outcome)
	}
}

func TestBodyReadNWithTerminalErrorProcessesBytesFirst(t *testing.T) {
	call := &scriptedCall{sentinel: "[DONE]"}
	reader := &errReader{data: []byte("data: [DONE]\n\n"), err: errors.New("late network error")}
	got, wrapper := drive(t, t.Context(), io.NopCloser(reader), call, modeSSE, nil)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.State != StateComplete {
		t.Fatalf("n>0-with-error ordering: %+v, want StateComplete from sentinel", got.outcome)
	}
}

func TestBodyCompletionStartOnFirstOutputEvent(t *testing.T) {
	stream := ": ping\n\ndata: control\n\ndata: delta-1\n\ndata: delta-2\n\ndata: [DONE]\n\n"
	call := &scriptedCall{sentinel: "[DONE]"}
	got, wrapper := drive(t, t.Context(), io.NopCloser(strings.NewReader(stream)), call, modeSSE, nil)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.CompletionStart.IsZero() {
		t.Fatal("CompletionStart never stamped")
	}
	if got.outcome.CompletionStart.After(got.outcome.End) {
		t.Fatal("CompletionStart after End")
	}
}

func TestBodySniffSelectsSSE(t *testing.T) {
	call := &scriptedCall{sentinel: "[DONE]"}
	got, wrapper := drive(t, t.Context(),
		io.NopCloser(strings.NewReader("data: [DONE]\n\n")), call, modeUndecided, nil)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.State != StateComplete || len(call.unary) != 0 {
		t.Fatalf("sniffed SSE handled as unary: %+v unary=%q", got.outcome, call.unary)
	}
}

func TestBodyParserPanicDegradesNotBreaks(t *testing.T) {
	got, wrapper := drive(t, t.Context(),
		io.NopCloser(strings.NewReader("data: boom\n\nrest")), panicCall{}, modeSSE, nil)
	data, err := io.ReadAll(wrapper)
	if err != nil {
		t.Fatalf("read failed because of parser panic: %v", err)
	}
	if string(data) != "data: boom\n\nrest" {
		t.Fatalf("bytes altered by panicking parser: %q", data)
	}
	if got.count != 1 {
		t.Fatalf("finalize count %d", got.count)
	}
}

type panicCall struct{}

func (panicCall) ParseRequest([]byte)           {}
func (panicCall) FeedEvent([]byte) EventVerdict { panic("parser defect") }
func (panicCall) FinishUnary([]byte, int)       {}
func (panicCall) Result() Result                { return Result{} }
