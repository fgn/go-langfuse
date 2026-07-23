package wiretap

import "bytes"

// sseFramer is a bounded incremental server-sent-events framer. Read
// boundaries are unrelated to event boundaries, so the framer buffers
// bytes until a blank line completes an event, accepting LF and CRLF
// framing, comments, event/id/retry fields, and multi-line data fields.
// An event whose accumulated size exceeds maxEvent is discarded and
// framing resynchronizes at the next blank line, while a discard flag
// records that content capture degraded; small terminal sentinels are
// never over-cap, so terminal detection survives discards.
type sseFramer struct {
	buf       []byte
	maxEvent  int
	discarded bool // at least one event was dropped for size
	dropping  bool // currently skipping an over-cap event
	stopped   bool // terminal reached; the framer is permanently done
}

// feed appends p and emits each completed event's joined data payload.
// emit returning false stops parsing permanently (terminal reached);
// remaining bytes still flow to the application unchanged elsewhere.
// pending reports bytes of an unterminated final frame, including a
// partially discarded one: a stream that ends with pending bytes did
// not close on an event boundary.
func (f *sseFramer) pending() bool {
	return len(f.buf) > 0 || f.dropping
}

// delimiterSlack covers a maximal CRLF event delimiter beyond the
// per-event cap, so an exactly-at-cap event still completes.
const delimiterSlack = 4

// feed consumes p in bounded chunks: the buffer never grows past
// maxEvent plus delimiter slack no matter how large one application
// Read is, so an oversized event costs bounded memory, never a full
// duplicate copy.
func (f *sseFramer) feed(p []byte, emit func(data []byte) bool) {
	if f.stopped {
		return
	}
	for len(p) > 0 {
		room := max(f.maxEvent+delimiterSlack-len(f.buf), 1)
		chunk := p
		if len(chunk) > room {
			chunk = p[:room]
		}
		p = p[len(chunk):]
		f.buf = append(f.buf, chunk...)
		if !f.drain(emit) {
			return
		}
	}
}

// drain emits completed events and enforces the per-event bound. It
// returns false permanently once a terminal was emitted.
func (f *sseFramer) drain(emit func(data []byte) bool) bool {
	for {
		event, rest, ok := nextEvent(f.buf)
		if !ok {
			if len(f.buf) > f.maxEvent {
				f.discardKeepingDelimiterState()
			}
			return true
		}
		f.buf = rest
		if f.dropping {
			// This boundary terminates the partially discarded event,
			// not a fully buffered one.
			f.dropping = false
			continue
		}
		if len(event) > f.maxEvent {
			f.discarded = true
			continue
		}
		data, hasData := eventData(event)
		if !hasData {
			continue // comment-only or control-only event
		}
		if !emit(data) {
			f.stopped = true
			f.buf = nil
			return false
		}
	}
}

// discardKeepingDelimiterState abandons an over-cap partial event
// while preserving the trailing bytes that may already form part of
// the event delimiter. Without this, a delimiter split across reads
// ("...x\n" then "\ndata: [DONE]\n\n") would make the boundary
// invisible and the NEXT valid event would be swallowed as the
// dropped event's terminator.
func (f *sseFramer) discardKeepingDelimiterState() {
	keep := 0
	switch {
	case bytes.HasSuffix(f.buf, []byte("\n\r")):
		keep = 2
	case bytes.HasSuffix(f.buf, []byte("\n")):
		keep = 1
	}
	f.buf = append(f.buf[:0], f.buf[len(f.buf)-keep:]...)
	f.discarded = true
	f.dropping = true
}

// nextEvent splits buf at the first blank line (LF LF, or CRLF CRLF, or
// the mixed forms), returning the raw event block and the remainder.
func nextEvent(buf []byte) (event, rest []byte, ok bool) {
	for i := range buf {
		if buf[i] != '\n' {
			continue
		}
		// A newline ends a line; a following optional \r plus newline
		// makes the next line blank, completing the event.
		j := i + 1
		if j < len(buf) && buf[j] == '\r' {
			j++
		}
		if j < len(buf) && buf[j] == '\n' {
			return buf[:i+1], buf[j+1:], true
		}
	}
	return nil, buf, false
}

// eventData extracts and joins the data field lines of one raw event
// block per the SSE specification: multiple data lines join with a
// single newline; comment lines (leading ':') and non-data fields are
// ignored. hasData is false when the event carries no data field at
// all, which covers keep-alive comments and control-only events.
func eventData(event []byte) (data []byte, hasData bool) {
	var joined []byte
	for len(event) > 0 {
		line := event
		if idx := bytes.IndexByte(event, '\n'); idx >= 0 {
			line = event[:idx]
			event = event[idx+1:]
		} else {
			event = nil
		}
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		value, isData := sseField(line, "data")
		if !isData {
			continue
		}
		if hasData {
			joined = append(joined, '\n')
		}
		joined = append(joined, value...)
		hasData = true
	}
	return joined, hasData
}

// sseField matches "name:" or "name" prefixed lines, stripping one
// optional leading space from the value per the SSE specification.
func sseField(line []byte, name string) (value []byte, ok bool) {
	if !bytes.HasPrefix(line, []byte(name)) {
		return nil, false
	}
	rest := line[len(name):]
	if len(rest) == 0 {
		return nil, true
	}
	if rest[0] != ':' {
		return nil, false
	}
	rest = rest[1:]
	if len(rest) > 0 && rest[0] == ' ' {
		rest = rest[1:]
	}
	return rest, true
}
