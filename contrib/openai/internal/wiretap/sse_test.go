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
