package wiretap

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func newBodyRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://example.test/v1/chat/completions",
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// TestRecorderExactPassthrough locks that the tee forwards every byte
// with exact (n, err) behavior and captures the transmitted body.
func TestRecorderExactPassthrough(t *testing.T) {
	recorder := &requestRecorder{cap: 1 << 16}
	req := newBodyRequest(t, `{"model":"m"}`)
	clone := req.Clone(req.Context())
	recorder.instrument(clone)

	forwarded, err := io.ReadAll(clone.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(forwarded) != `{"model":"m"}` {
		t.Fatalf("forwarded bytes altered: %q", forwarded)
	}
	if err := clone.Body.Close(); err != nil {
		t.Fatal(err)
	}
	captured, ok := recorder.snapshot()
	if !ok || string(captured) != `{"model":"m"}` {
		t.Fatalf("capture = %q, %v", captured, ok)
	}
}

// TestRecorderEOFCompletesWithoutClose locks that a fully read body
// counts as transmitted even when Close never happens.
func TestRecorderEOFCompletesWithoutClose(t *testing.T) {
	recorder := &requestRecorder{cap: 1 << 16}
	req := newBodyRequest(t, "abc")
	clone := req.Clone(req.Context())
	recorder.instrument(clone)
	if _, err := io.ReadAll(clone.Body); err != nil {
		t.Fatal(err)
	}
	if _, ok := recorder.snapshot(); !ok {
		t.Fatal("fully read body was not snapshot-eligible")
	}
}

// TestRecorderPartialTransmissionOmitted locks the full-duplex/early
// response rule: a body that was not completely transmitted must never
// be parsed.
func TestRecorderPartialTransmissionOmitted(t *testing.T) {
	recorder := &requestRecorder{cap: 1 << 16}
	req := newBodyRequest(t, "abcdefgh")
	clone := req.Clone(req.Context())
	recorder.instrument(clone)
	buf := make([]byte, 3)
	if _, err := clone.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	if body, ok := recorder.snapshot(); ok {
		t.Fatalf("partial transmission snapshot succeeded: %q", body)
	}
}

// TestRecorderOverCapDropsNeverTruncates locks drop-not-truncate and
// unchanged forwarding beyond the cap.
func TestRecorderOverCapDropsNeverTruncates(t *testing.T) {
	recorder := &requestRecorder{cap: 8}
	req := newBodyRequest(t, "0123456789ABCDEF")
	clone := req.Clone(req.Context())
	recorder.instrument(clone)
	forwarded, err := io.ReadAll(clone.Body)
	if err != nil || string(forwarded) != "0123456789ABCDEF" {
		t.Fatalf("over-cap forwarding altered: %q, %v", forwarded, err)
	}
	_ = clone.Body.Close()
	if body, ok := recorder.snapshot(); ok {
		t.Fatalf("over-cap capture returned content: %q", body)
	}
	if !recorder.overCapped() {
		t.Fatal("over-cap state not reported")
	}
}

// TestRecorderReplayReplacesCapture locks the finding-19 semantics:
// GetBody replays start a new capture generation that replaces the
// partial previous transmission.
func TestRecorderReplayReplacesCapture(t *testing.T) {
	recorder := &requestRecorder{cap: 1 << 16}
	req := newBodyRequest(t, "replayable-body")
	if req.GetBody == nil {
		t.Fatal("http.NewRequest with strings.Reader must set GetBody")
	}
	clone := req.Clone(req.Context())
	recorder.instrument(clone)

	// First transmission reads only part of the body, then the
	// transport replays via GetBody (connection loss).
	partial := make([]byte, 6)
	if _, err := clone.Body.Read(partial); err != nil {
		t.Fatal(err)
	}
	replay, err := clone.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	full, err := io.ReadAll(replay)
	if err != nil || string(full) != "replayable-body" {
		t.Fatalf("replay body altered: %q, %v", full, err)
	}
	_ = replay.Close()

	captured, ok := recorder.snapshot()
	if !ok || string(captured) != "replayable-body" {
		t.Fatalf("replay capture = %q, %v; want full replayed body", captured, ok)
	}
}

// TestRecorderPreservesNilAndNoBody locks identity for bodyless
// requests: no wrapper, no framing change.
func TestRecorderPreservesNilAndNoBody(t *testing.T) {
	recorder := &requestRecorder{cap: 1 << 16}
	getReq, err := http.NewRequest(http.MethodGet, "https://example.test/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	clone := getReq.Clone(getReq.Context())
	recorder.instrument(clone)
	if clone.Body != nil {
		t.Fatal("nil body was wrapped")
	}

	noBodyReq, err := http.NewRequest(http.MethodPost, "https://example.test/v1/chat/completions", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	clone = noBodyReq.Clone(noBodyReq.Context())
	recorder.instrument(clone)
	if clone.Body != http.NoBody {
		t.Fatal("http.NoBody identity was not preserved")
	}
}

// TestRecorderConcurrentReadClose exercises the documented transport
// behavior of closing request bodies asynchronously; run under -race.
func TestRecorderConcurrentReadClose(t *testing.T) {
	recorder := &requestRecorder{cap: 1 << 16}
	req := newBodyRequest(t, strings.Repeat("payload-", 1024))
	clone := req.Clone(req.Context())
	recorder.instrument(clone)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(io.Discard, clone.Body)
	}()
	go func() {
		defer wg.Done()
		_ = clone.Body.Close()
	}()
	wg.Wait()
	_, _ = recorder.snapshot()
}

// TestRecorderContentLengthUntouched locks that instrumentation does
// not alter declared framing.
func TestRecorderContentLengthUntouched(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 64)
	req, err := http.NewRequest(http.MethodPost, "https://example.test/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	clone := req.Clone(req.Context())
	(&requestRecorder{cap: 8}).instrument(clone)
	if clone.ContentLength != int64(len(body)) {
		t.Fatalf("ContentLength changed to %d", clone.ContentLength)
	}
}
