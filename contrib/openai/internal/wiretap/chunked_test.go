package wiretap

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// chunkedStub implements ChunkedCall, recording the streamed payload
// and finish calls so tests can compare against eventData's output.
type chunkedStub struct {
	scriptedCall
	streamed        bytes.Buffer
	beganUnary      int
	finishedEvents  int
	finishedUnary   int
	unaryStatus     int
	eventVerdict    EventVerdict
	unaryCompletion bool
}

func (c *chunkedStub) FeedOversized(p []byte) { c.streamed.Write(p) }
func (c *chunkedStub) BeginOversizedUnary()   { c.beganUnary++ }
func (c *chunkedStub) UnaryComplete() bool    { return c.unaryCompletion }
func (c *chunkedStub) FinishOversizedUnary(status int) {
	c.finishedUnary++
	c.unaryStatus = status
}

func (c *chunkedStub) FinishOversizedEvent() EventVerdict {
	c.finishedEvents++
	return c.eventVerdict
}

// driveChunked builds a wrapper with a small cap so oversized paths
// are cheap to construct.
func driveChunked(
	t *testing.T,
	body io.ReadCloser,
	call Call,
	mode streamMode,
	capBytes, maxEvent int,
) (*collected, *bodyWrapper) {
	t.Helper()
	got := &collected{}
	wrapper := newBodyWrapper(t.Context(), body, call, mode, capBytes, 200, maxEvent,
		&atomic.Bool{}, func(outcome Outcome) {
			got.outcome = outcome
			got.count++
		})
	return got, wrapper
}

func TestSSEIncompleteTerminalFreezesAndCloseIsNoOp(t *testing.T) {
	stream := "data: INCOMPLETE\n\ndata: drained-after\n\n"
	call := &incompleteCall{}
	got, wrapper := drive(t, t.Context(), io.NopCloser(strings.NewReader(stream)), call, modeSSE, nil)
	data, err := io.ReadAll(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != stream {
		t.Fatalf("stream bytes altered: %q", data)
	}
	if got.count != 1 || got.outcome.State != StateIncomplete {
		t.Fatalf("outcome %+v x%d, want one StateIncomplete", got.outcome, got.count)
	}
	for _, event := range call.events {
		if event == "drained-after" {
			t.Fatal("parser fed after the hard incomplete terminal")
		}
	}
	if err := wrapper.Close(); err != nil || got.count != 1 {
		t.Fatalf("post-terminal Close changed outcome: %v x%d", err, got.count)
	}
}

// incompleteCall returns the hard incomplete terminal for the
// INCOMPLETE payload.
type incompleteCall struct{ scriptedCall }

func (c *incompleteCall) FeedEvent(data []byte) EventVerdict {
	if data != nil && string(data) == "INCOMPLETE" {
		c.events = append(c.events, string(data))
		return EventVerdict{Terminal: TerminalIncomplete}
	}
	return c.scriptedCall.FeedEvent(data)
}

// oversizedEventStream builds one raw SSE event exceeding the cap with
// every normalization hazard: an event field, a comment, multiple data
// lines, CRLF delimiters, and a leading value space.
func oversizedEventStream(pad int) (raw string, wantPayload string) {
	big := strings.Repeat("x", pad)
	raw = "event: response.chunk\r\n" +
		": keep-alive comment\r\n" +
		"data: {\"part\":1," + big + "}\r\n" +
		"data\r\n" +
		"data: tail\r\n" +
		"\r\n"
	event := []byte(strings.ReplaceAll(raw[:len(raw)-2], "\r\n", "\n"))
	payload, _ := eventData(event)
	return raw, string(payload)
}

func TestOversizedEventStreamsNormalizedPayload(t *testing.T) {
	const maxEvent = 64
	raw, want := oversizedEventStream(3 * maxEvent)
	stream := raw + "data: after\n\n"
	for _, chunk := range []int{1, 3, 7, len(stream)} {
		t.Run(fmt.Sprintf("chunk-%d", chunk), func(t *testing.T) {
			call := &chunkedStub{}
			reader := io.NopCloser(strings.NewReader(stream))
			got, wrapper := driveChunked(t, reader, call, modeSSE, 1<<16, maxEvent)
			buf := make([]byte, chunk)
			for {
				if _, err := wrapper.Read(buf); err != nil {
					break
				}
			}
			if got.count != 1 || got.outcome.State != StateComplete {
				t.Fatalf("outcome %+v x%d", got.outcome, got.count)
			}
			if call.streamed.String() != want {
				t.Fatalf("streamed payload:\n%q\nwant:\n%q", call.streamed.String(), want)
			}
			if call.finishedEvents != 1 {
				t.Fatalf("FinishOversizedEvent calls = %d, want 1", call.finishedEvents)
			}
			if got.outcome.CaptureDegraded {
				t.Fatal("chunk-scanned events must not set the generic degraded bit; the parser owns partial")
			}
			found := false
			for _, event := range call.events {
				if event == "after" {
					found = true
				}
			}
			if !found {
				t.Fatal("the following normal event must still be parsed")
			}
		})
	}
}

func TestOversizedEventOutputVerdictStampsCompletionStart(t *testing.T) {
	const maxEvent = 64
	raw, _ := oversizedEventStream(3 * maxEvent)
	call := &chunkedStub{eventVerdict: EventVerdict{Output: true}}
	got, wrapper := driveChunked(t, io.NopCloser(strings.NewReader(raw)), call, modeSSE, 1<<16, maxEvent)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.CompletionStart.IsZero() {
		t.Fatal("an output-bearing oversized event must stamp CompletionStart")
	}
}

func TestOversizedControlOnlyEventIsIgnored(t *testing.T) {
	const maxEvent = 32
	stream := ": " + strings.Repeat("c", 4*maxEvent) + "\n\ndata: after\n\n"
	call := &chunkedStub{}
	got, wrapper := driveChunked(t, io.NopCloser(strings.NewReader(stream)), call, modeSSE, 1<<16, maxEvent)
	_, _ = io.ReadAll(wrapper)
	if call.finishedEvents != 0 || call.streamed.Len() != 0 {
		t.Fatalf("control-only event reached the sink: %d finishes, %q", call.finishedEvents, call.streamed.String())
	}
	if got.outcome.CaptureDegraded {
		t.Fatal("a control-only oversized event is not degradation")
	}
	if len(call.events) != 1 || call.events[0] != "after" {
		t.Fatalf("following event lost: %v", call.events)
	}
}

func TestOversizedTerminalEventEndsStream(t *testing.T) {
	const maxEvent = 32
	stream := "data: " + strings.Repeat("t", 4*maxEvent) + "\n\ndata: never\n\n"
	call := &chunkedStub{eventVerdict: EventVerdict{Terminal: TerminalError}}
	got, wrapper := driveChunked(t, io.NopCloser(strings.NewReader(stream)), call, modeSSE, 1<<16, maxEvent)
	_, _ = io.ReadAll(wrapper)
	if got.count != 1 || got.outcome.State != StateFailed {
		t.Fatalf("outcome %+v x%d, want StateFailed once", got.outcome, got.count)
	}
	if len(call.events) != 0 {
		t.Fatalf("events after an oversized terminal must be ignored: %v", call.events)
	}
}

func TestOversizedEventEOFMidEventIsIncomplete(t *testing.T) {
	const maxEvent = 32
	stream := "data: " + strings.Repeat("u", 4*maxEvent) // no terminator
	call := &chunkedStub{}
	got, wrapper := driveChunked(t, io.NopCloser(strings.NewReader(stream)), call, modeSSE, 1<<16, maxEvent)
	_, _ = io.ReadAll(wrapper)
	if got.outcome.State != StateIncomplete {
		t.Fatalf("EOF inside a chunked event: %+v, want StateIncomplete", got.outcome)
	}
	if call.finishedEvents != 0 {
		t.Fatal("an unterminated event must not be finished")
	}
}

func TestOversizedUnaryReplaysPrefixAndFinishesOnEOF(t *testing.T) {
	body := `{"status":"incomplete","pad":"` + strings.Repeat("p", 256) + `"}`
	call := &chunkedStub{}
	got, wrapper := driveChunked(t, io.NopCloser(strings.NewReader(body)), call, modeUnary, 64, 1<<10)
	buf := make([]byte, 7)
	for {
		if _, err := wrapper.Read(buf); err != nil {
			break
		}
	}
	if call.beganUnary != 1 {
		t.Fatalf("BeginOversizedUnary calls = %d, want 1", call.beganUnary)
	}
	if call.streamed.String() != body {
		t.Fatalf("scanner must see the full byte history:\n%q\nwant\n%q", call.streamed.String(), body)
	}
	if call.finishedUnary != 1 || call.unaryStatus != 200 {
		t.Fatalf("FinishOversizedUnary = %d (status %d), want once with 200", call.finishedUnary, call.unaryStatus)
	}
	if got.outcome.State != StateComplete {
		t.Fatalf("clean EOF outcome %+v, want StateComplete", got.outcome)
	}
	if !got.outcome.CaptureDegraded {
		t.Fatal("over-cap unary still degrades content capture")
	}
}

func TestOversizedUnaryDecoderClosePattern(t *testing.T) {
	body := `{"status":"completed","pad":"` + strings.Repeat("q", 256) + `"}`
	for _, testCase := range []struct {
		name      string
		complete  bool
		wantState FinalState
		finished  int
	}{
		{"complete-value-close", true, StateComplete, 1},
		{"incomplete-value-close", false, StateClosedEarly, 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			call := &chunkedStub{unaryCompletion: testCase.complete}
			// The reader never returns EOF (keep-alive connection).
			reader := io.NopCloser(&neverEOFReader{data: []byte(body)})
			got, wrapper := driveChunked(t, reader, call, modeUnary, 64, 1<<10)
			buf := make([]byte, len(body))
			total := 0
			for total < len(body) {
				n, err := wrapper.Read(buf)
				total += n
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := wrapper.Close(); err != nil {
				t.Fatal(err)
			}
			if got.outcome.State != testCase.wantState {
				t.Fatalf("outcome %+v, want %v", got.outcome, testCase.wantState)
			}
			if call.finishedUnary != testCase.finished {
				t.Fatalf("FinishOversizedUnary = %d, want %d", call.finishedUnary, testCase.finished)
			}
		})
	}
}

// neverEOFReader serves its data and then blocks the error: reads past
// the end return 0 bytes with no error is illegal, so it returns a
// would-block sentinel by serving one byte at a time and never EOF.
type neverEOFReader struct {
	data []byte
	pos  int
}

func (r *neverEOFReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		// Simulate a keep-alive connection with no further bytes: the
		// test never reads past the payload, so this is unreachable.
		select {}
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestBarelyOverCapCompleteEventsStillSalvage(t *testing.T) {
	// The delimiter slack admits complete events a few bytes over the
	// cap into the buffered path; they must salvage exactly like
	// streamed ones. The sweep derives each fixture's branch from the
	// REAL nextEvent semantics and asserts the intended construction,
	// covering both the complete-event and streamed-lexer paths across
	// both framings and every read split.
	const maxEvent = 64
	for over := 1; over <= delimiterSlack+2; over++ {
		for _, framing := range []string{"\n\n", "\r\n\r\n"} {
			lineEnd := framing[:len(framing)/2]
			payload := strings.Repeat("x", maxEvent+over-len("data: ")-len(lineEnd))
			raw := "data: " + payload + framing
			block := []byte(raw)
			event, _, found := nextEvent(block)
			if !found {
				t.Fatalf("construction: nextEvent found no boundary in %q", raw)
			}
			if len(event) != maxEvent+over {
				t.Fatalf("construction: event block = %d bytes, want %d", len(event), maxEvent+over)
			}
			completeBranch := len(raw) <= maxEvent+delimiterSlack
			_ = completeBranch // both branches must salvage identically
			stream := raw + "data: after" + framing
			for _, chunk := range []int{1, 5, len(stream)} {
				call := &chunkedStub{}
				got, wrapper := driveChunked(t, io.NopCloser(strings.NewReader(stream)), call, modeSSE, 1<<16, maxEvent)
				buf := make([]byte, chunk)
				for {
					if _, err := wrapper.Read(buf); err != nil {
						break
					}
				}
				if call.finishedEvents != 1 {
					t.Fatalf("over=%d framing=%q chunk=%d: finishes = %d, want 1",
						over, framing, chunk, call.finishedEvents)
				}
				if call.streamed.String() != payload {
					t.Fatalf("over=%d framing=%q chunk=%d: payload %q, want %q",
						over, framing, chunk, call.streamed.String(), payload)
				}
				if got.outcome.CaptureDegraded {
					t.Fatalf("over=%d framing=%q chunk=%d: parser owns the degradation bit", over, framing, chunk)
				}
				if len(call.events) != 1 || call.events[0] != "after" {
					t.Fatalf("over=%d framing=%q chunk=%d: following event lost: %v",
						over, framing, chunk, call.events)
				}
			}
		}
	}
}

// panickingChunked panics in its oversized hooks: the wrapper must
// degrade telemetry without retaining the self-referential callback,
// so the GC abandonment safety net still fires.
type panickingChunked struct {
	chunkedStub
	panicOnFeed bool
}

func (c *panickingChunked) FeedOversized(p []byte) {
	if c.panicOnFeed {
		panic("feed")
	}
	c.chunkedStub.FeedOversized(p)
}

func (c *panickingChunked) FinishOversizedEvent() EventVerdict {
	panic("finish")
}

func TestAbandonSafetyNetSurvivesOversizedPanics(t *testing.T) {
	const maxEvent = 32
	for name, call := range map[string]Call{
		"panic-on-feed":   &panickingChunked{panicOnFeed: true},
		"panic-on-finish": &panickingChunked{},
	} {
		t.Run(name, func(t *testing.T) {
			finalized := make(chan Outcome, 1)
			wrapper := newBodyWrapper(t.Context(),
				io.NopCloser(strings.NewReader("data: "+strings.Repeat("p", 4*maxEvent)+"\n\ndata: x\n\n")),
				call, modeSSE, 1<<16, 200, maxEvent, &atomic.Bool{},
				func(outcome Outcome) { finalized <- outcome })
			buf := make([]byte, 7)
			for {
				if _, err := wrapper.Read(buf); err != nil {
					break
				}
			}
			select {
			case outcome := <-finalized:
				// EOF finalized normally despite the panic; the panic
				// downgraded telemetry.
				if !outcome.CaptureDegraded {
					t.Error("a parser panic must degrade capture")
				}
			default:
				// Not finalized by EOF (panic happened first): the
				// abandoned wrapper must still be caught by the GC
				// safety net, which a retained self-referential
				// callback would defeat.
				_ = wrapper
				wrapper = nil
				_ = wrapper
				deadline := time.Now().Add(5 * time.Second)
				for {
					runtime.GC()
					select {
					case <-finalized:
						return
					default:
					}
					if time.Now().After(deadline) {
						t.Fatal("abandoned wrapper never finalized; the oversized callback pinned it")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
}
