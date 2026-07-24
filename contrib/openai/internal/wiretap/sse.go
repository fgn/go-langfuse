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

	// chunked, when non-nil, replaces the discard path for over-cap
	// events: the framer lexes the raw event incrementally and streams
	// the normalized joined data payload to the sink, so control-plane
	// facts survive events that cannot be buffered. The parser owns the
	// degradation bit for these events (every oversized data-bearing
	// event must set TelemetryPartial), so discarded stays false here.
	chunked ChunkedCall
	// finishOversized is invoked when a chunk-lexed over-cap event
	// ends; like emit, returning false stops parsing permanently. Set
	// together with chunked by the body wrapper.
	finishOversized func() bool
	lexer           oversizedLexer
	// chunking marks an over-cap event currently streaming through the
	// lexer; it counts as pending (an EOF mid-event is not an event
	// boundary).
	chunking bool
}

// feed appends p and emits each completed event's joined data payload.
// emit returning false stops parsing permanently (terminal reached);
// remaining bytes still flow to the application unchanged elsewhere.
// pending reports bytes of an unterminated final frame, including a
// partially discarded one: a stream that ends with pending bytes did
// not close on an event boundary.
func (f *sseFramer) pending() bool {
	return len(f.buf) > 0 || f.dropping || f.chunking
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
	for len(p) > 0 || f.chunking {
		if f.chunking {
			consumed, done := f.lexer.feed(p, f.chunked)
			p = p[consumed:]
			if !done {
				return // the event continues in a later read
			}
			f.chunking = false
			// A control-only over-cap event carried no data payload:
			// nothing reached the sink and there is nothing to finish.
			if f.lexer.sawData && !f.finishOversized() {
				f.stopped = true
				f.buf = nil
				return
			}
			f.lexer = oversizedLexer{}
			continue
		}
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
				if f.chunked != nil && !f.dropping {
					// Switch this event to streamed salvage: the buffer
					// holds the raw event from its start, so the lexer
					// begins line-aligned; feed returns with the whole
					// buffer consumed because no event boundary exists
					// in it (nextEvent just failed).
					f.chunking = true
					f.lexer = oversizedLexer{}
					f.lexer.feed(f.buf, f.chunked)
					f.buf = f.buf[:0]
					return true
				}
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
			// The delimiter slack admits complete events a few bytes
			// over the cap; they take the same salvage path as streamed
			// ones so a barely-over-cap terminal is never lost.
			if f.chunked != nil {
				lexer := oversizedLexer{}
				lexer.feed(event, f.chunked)
				lexer.feed([]byte{'\n'}, f.chunked)
				if lexer.sawData && f.finishOversized != nil && !f.finishOversized() {
					f.stopped = true
					f.buf = nil
					return false
				}
				continue
			}
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

// oversizedLexer incrementally lexes ONE raw SSE event that exceeded
// the buffering cap, streaming its normalized joined data payload to a
// ChunkedCall exactly as eventData would have produced it: field names,
// comments, ids, retry fields, and CR/LF delimiters never reach the
// sink, successive data lines are joined with a single newline, and one
// optional leading space per data value is stripped. State is a few
// bytes regardless of event size.
type oversizedLexer struct {
	state   lexState
	field   []byte // bounded partial field-name buffer
	sawData bool   // at least one data field seen in this event
	heldCR  bool   // a \r inside a data value awaiting \n lookahead
}

type lexState int

const (
	lexLineStart lexState = iota
	lexLineStartCR
	lexFieldName
	lexFieldCR
	lexDataValueStart
	lexDataValue
	lexSkipLine
)

// maxFieldName bounds field-name buffering: "data" and every other SSE
// field name fit well below it, and anything longer is not data.
const maxFieldName = 8

// feed consumes bytes of the raw event block and returns how many were
// consumed plus whether the event's terminating blank line was reached.
// Data-value content is forwarded to sink in contiguous runs.
func (l *oversizedLexer) feed(p []byte, sink ChunkedCall) (consumed int, done bool) {
	index := 0
	for index < len(p) {
		b := p[index]
		switch l.state {
		case lexLineStart:
			switch b {
			case '\n':
				return index + 1, true // blank line: event ends
			case '\r':
				l.state = lexLineStartCR
			case ':':
				l.state = lexSkipLine
			default:
				l.field = append(l.field[:0], b)
				l.state = lexFieldName
			}
			index++
		case lexLineStartCR:
			if b == '\n' {
				return index + 1, true // \r\n blank line: event ends
			}
			// A stray \r starts an ordinary (non-data) line.
			l.state = lexSkipLine
		case lexFieldName:
			switch b {
			case ':':
				if string(l.field) == "data" {
					l.state = lexDataValueStart
				} else {
					l.state = lexSkipLine
				}
				index++
			case '\n':
				l.endFieldOnlyLine(sink)
				index++
			case '\r':
				l.state = lexFieldCR
				index++
			default:
				if len(l.field) >= maxFieldName {
					l.state = lexSkipLine
				} else {
					l.field = append(l.field, b)
					index++
				}
			}
		case lexFieldCR:
			if b == '\n' {
				l.endFieldOnlyLine(sink)
				index++
			} else {
				l.state = lexSkipLine
			}
		case lexDataValueStart:
			l.beginDataValue(sink)
			l.state = lexDataValue
			if b == ' ' {
				index++ // one optional leading space per the SSE spec
			}
		case lexDataValue:
			if l.heldCR {
				l.heldCR = false
				if b == '\n' {
					l.state = lexLineStart
					index++
					continue
				}
				sink.FeedOversized([]byte{'\r'})
			}
			run := index
			for run < len(p) && p[run] != '\n' && p[run] != '\r' {
				run++
			}
			if run > index {
				sink.FeedOversized(p[index:run])
				index = run
				continue
			}
			if b == '\r' {
				l.heldCR = true
				index++
				continue
			}
			l.state = lexLineStart // b == '\n'
			index++
		case lexSkipLine:
			if b == '\n' {
				l.state = lexLineStart
			}
			index++
		}
	}
	return len(p), false
}

// endFieldOnlyLine handles a line consisting of a bare field name: a
// bare "data" line contributes an empty data value per the SSE
// specification; every other bare field is ignored.
func (l *oversizedLexer) endFieldOnlyLine(sink ChunkedCall) {
	if string(l.field) == "data" {
		l.beginDataValue(sink)
	}
	l.state = lexLineStart
}

// beginDataValue joins successive data lines with a single newline.
func (l *oversizedLexer) beginDataValue(sink ChunkedCall) {
	if l.sawData {
		sink.FeedOversized([]byte{'\n'})
	}
	l.sawData = true
}
