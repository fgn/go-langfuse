package wiretap

import (
	"strings"
	"testing"
)

// feedFragmented drives the framer with every fragmentation of the
// stream at the given chunk size, collecting emitted data payloads.
func feedFragmented(t *testing.T, stream string, chunkSize int) []string {
	t.Helper()
	framer := sseFramer{maxEvent: 1 << 16}
	var events []string
	for offset := 0; offset < len(stream); offset += chunkSize {
		end := min(offset+chunkSize, len(stream))
		framer.feed([]byte(stream[offset:end]), func(data []byte) bool {
			events = append(events, string(data))
			return true
		})
	}
	return events
}

// TestSSEFramerFragmentationInvariance is the core framer property:
// event boundaries are independent of read boundaries, from 1-byte
// reads through one jumbo read.
func TestSSEFramerFragmentationInvariance(t *testing.T) {
	stream := "data: {\"a\":1}\n\n" +
		": keep-alive ping\n\n" +
		"event: message\ndata: {\"b\":2}\n\n" +
		"data: line one\ndata: line two\n\n" +
		"data: [DONE]\n\n"
	want := []string{`{"a":1}`, `{"b":2}`, "line one\nline two", "[DONE]"}

	for _, chunkSize := range []int{1, 2, 3, 5, 7, 16, 64, len(stream)} {
		got := feedFragmented(t, stream, chunkSize)
		if len(got) != len(want) {
			t.Fatalf("chunk size %d: got %d events %q, want %d", chunkSize, len(got), got, len(want))
		}
		for index := range want {
			if got[index] != want[index] {
				t.Fatalf("chunk size %d: event %d = %q, want %q", chunkSize, index, got[index], want[index])
			}
		}
	}
}

func TestSSEFramerCRLF(t *testing.T) {
	stream := "data: {\"a\":1}\r\n\r\ndata: [DONE]\r\n\r\n"
	got := feedFragmented(t, stream, 1)
	if len(got) != 2 || got[0] != `{"a":1}` || got[1] != "[DONE]" {
		t.Fatalf("CRLF framing: got %q", got)
	}
}

func TestSSEFramerCommentsAndControlOnly(t *testing.T) {
	stream := ": ping\n\nretry: 100\nid: 7\n\nevent: done\n\ndata: x\n\n"
	got := feedFragmented(t, stream, 4)
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("control events leaked to the parser: %q", got)
	}
}

// TestSSEFramerOversizedEventResync locks the discard behavior: an
// over-cap event abandons content while framing resynchronizes and a
// tiny later sentinel still arrives.
func TestSSEFramerOversizedEventResync(t *testing.T) {
	framer := sseFramer{maxEvent: 64}
	big := "data: " + strings.Repeat("x", 500) + "\n\ndata: [DONE]\n\n"
	var events []string
	for offset := 0; offset < len(big); offset += 9 {
		end := min(offset+9, len(big))
		framer.feed([]byte(big[offset:end]), func(data []byte) bool {
			events = append(events, string(data))
			return true
		})
	}
	if !framer.discarded {
		t.Fatal("oversized event was not flagged as discarded")
	}
	if len(events) != 1 || events[0] != "[DONE]" {
		t.Fatalf("terminal sentinel lost across discard: %q", events)
	}
}

// TestSSEFramerStopsAfterTerminal locks that emit returning false ends
// parsing permanently while later bytes flow elsewhere untouched.
func TestSSEFramerStopsAfterTerminal(t *testing.T) {
	framer := sseFramer{maxEvent: 1 << 16}
	calls := 0
	framer.feed([]byte("data: [DONE]\n\ndata: after\n\n"), func([]byte) bool {
		calls++
		return false
	})
	framer.feed([]byte("data: more\n\n"), func([]byte) bool {
		calls++
		return true
	})
	if calls != 1 {
		t.Fatalf("parser fed %d events after terminal, want 1 total", calls)
	}
}

func TestSSEFieldParsing(t *testing.T) {
	cases := []struct {
		line  string
		value string
		ok    bool
	}{
		{"data: payload", "payload", true},
		{"data:payload", "payload", true},
		{"data:  two spaces", " two spaces", true},
		{"data", "", true},
		{"database: no", "", false},
		{"event: x", "", false},
	}
	for _, tc := range cases {
		value, ok := sseField([]byte(tc.line), "data")
		if ok != tc.ok || string(value) != tc.value {
			t.Fatalf("sseField(%q) = %q, %v; want %q, %v", tc.line, value, ok, tc.value, tc.ok)
		}
	}
}

// TestSSEFramerDiscardSplitDelimiterDoesNotSwallowNext locks review
// round 2 finding 16: when an oversized event's delimiter splits
// across reads, the next valid event must survive. The exact failing
// sequence: the over-cap event ends with "\n", the following read
// starts with the delimiter's second "\n".
func TestSSEFramerDiscardSplitDelimiterDoesNotSwallowNext(t *testing.T) {
	framer := sseFramer{maxEvent: 64}
	var events []string
	emit := func(data []byte) bool {
		events = append(events, string(data))
		return true
	}
	framer.feed([]byte("data: "+strings.Repeat("x", 70)+"\n"), emit)
	framer.feed([]byte("\ndata: [DONE]\n\n"), emit)
	if len(events) != 1 || events[0] != "[DONE]" {
		t.Fatalf("terminal swallowed after split-delimiter discard: %q", events)
	}
	if !framer.discarded {
		t.Fatal("oversized event not flagged")
	}
}

// TestSSEFramerDiscardSplitSweep drives every split position around an
// oversized event's delimiter followed by a small event, under LF and
// CRLF framing.
func TestSSEFramerDiscardSplitSweep(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		stream := "data: " + strings.Repeat("x", 70) + newline + newline +
			"data: [DONE]" + newline + newline
		for split := 1; split < len(stream); split++ {
			framer := sseFramer{maxEvent: 64}
			var events []string
			emit := func(data []byte) bool {
				events = append(events, string(data))
				return true
			}
			framer.feed([]byte(stream[:split]), emit)
			framer.feed([]byte(stream[split:]), emit)
			if len(events) != 1 || events[0] != "[DONE]" {
				t.Fatalf("newline %q split %d: events %q", newline, split, events)
			}
		}
	}
}

// TestSSEFramerBoundedCopyOnJumboRead locks review round 2 finding 17:
// one arbitrarily large Read must never be duplicated into the buffer
// beyond the per-event bound.
func TestSSEFramerBoundedCopyOnJumboRead(t *testing.T) {
	framer := sseFramer{maxEvent: 1 << 10}
	jumbo := []byte("data: " + strings.Repeat("y", 1<<20) + "\n\ndata: [DONE]\n\n")
	var events []string
	framer.feed(jumbo, func(data []byte) bool {
		events = append(events, string(data))
		return true
	})
	if cap(framer.buf) > (1<<10)+delimiterSlack+1024 {
		t.Fatalf("buffer grew to %d for a jumbo read", cap(framer.buf))
	}
	if len(events) != 1 || events[0] != "[DONE]" {
		t.Fatalf("jumbo oversized event handling: %q", events)
	}
	if !framer.discarded {
		t.Fatal("jumbo event not flagged as discarded")
	}
}

// FuzzSSEFramer asserts bounded memory and panic freedom under
// arbitrary bytes and fragmentation.
func FuzzSSEFramer(f *testing.F) {
	f.Add([]byte("data: hello\n\ndata: [DONE]\n\n"), uint8(3))
	f.Add([]byte(": ping\r\n\r\ndata: "+strings.Repeat("z", 300)+"\r\n\r\n"), uint8(1))
	f.Fuzz(func(t *testing.T, stream []byte, chunk uint8) {
		size := int(chunk)%37 + 1
		framer := sseFramer{maxEvent: 128}
		for offset := 0; offset < len(stream); offset += size {
			end := min(offset+size, len(stream))
			framer.feed(stream[offset:end], func([]byte) bool { return true })
			if len(framer.buf) > 128+delimiterSlack {
				t.Fatalf("buffer exceeded bound: %d", len(framer.buf))
			}
		}
	})
}
