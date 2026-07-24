package langfuseopenai

import (
	"strconv"
	"unicode/utf8"
)

// controlScanner is a bounded push-mode JSON lexer used as the grammar
// authority for BOTH buffered and over-cap payloads. It proves
// structural validity of the WHOLE document (container-kind stack,
// full number grammar, string escapes, one top-level value plus
// trailing whitespace), retains only a fixed set of small
// control-plane fields as raw JSON (decoded later with encoding/json,
// so escape and Unicode semantics match the ordinary parser exactly),
// detects duplicate selected keys by their DECODED spelling, and
// tracks presence of non-empty output-bearing values at exact
// path-qualified positions without retaining them. Memory is bounded
// by the per-field caps and maxScanDepth regardless of input size.

// maxScanDepth bounds the container-kind stack, far above any pinned
// Responses schema path; over-depth marks the document invalid.
const maxScanDepth = 64

// scanFieldCaps bound each retained field. String fields are capped on
// the DECODED value (checked at read time via decodeScannedString);
// object fields are capped on raw JSON bytes of the value. The raw
// retention buffer allows escape expansion for string fields.
var scanFieldCaps = map[string]int{
	"type":               64,
	"status":             64,
	"model":              256,
	"usage":              4096,
	"error":              4096,
	"incomplete_details": 1024,
}

// scanFieldIsString marks fields whose cap applies to the decoded
// string value rather than raw object bytes.
var scanFieldIsString = map[string]bool{
	"type": true, "status": true, "model": true,
}

// rawCapFor is the raw retention bound: string fields may expand every
// byte to a six-byte escape, plus quotes.
func rawCapFor(name string) int {
	limit := scanFieldCaps[name]
	if scanFieldIsString[name] {
		return limit*6 + 2
	}
	return limit
}

type scanMode int

const (
	// scanSSEEnvelope selects top-level "type" plus the control fields
	// nested one level down inside "response".
	scanSSEEnvelope scanMode = iota
	// scanUnaryRoot selects the control fields at the document root (a
	// unary /responses body IS the Response object).
	scanUnaryRoot
)

type scanState int

const (
	scanValue scanState = iota
	scanString
	scanStringEscape
	scanStringUnicode
	scanNumberSign
	scanNumberZero
	scanNumberInt
	scanNumberFracFirst
	scanNumberFrac
	scanNumberExpSign
	scanNumberExpFirst
	scanNumberExp
	scanLiteral
	scanObjectKeyOrEnd
	scanObjectKey
	scanObjectColon
	scanObjectValueDone
	scanArrayValueDone
	scanValueOrArrayEnd
	scanTrailing
)

type scanFrame struct {
	isObject bool
	// key is the current DECODED member key being parsed inside this
	// object (empty for arrays or keys longer than the bound; long keys
	// are never selected or presence-relevant).
	key string
	// enteredAs is the decoded key under which this container was
	// entered in its parent object ("" for array elements and the
	// root), giving presence tracking its exact path.
	enteredAs string
}

type controlScanner struct {
	mode  scanMode
	state scanState
	stack []scanFrame

	invalid    bool // structural or grammar failure: no hard verdicts
	duplicates bool // duplicate selected key: schema-invalid
	rootObject bool // the top-level value is an object (protocol requirement)
	fieldOver  bool // a selected field breached its cap (dropped whole)
	complete   bool // one full top-level value consumed
	started    bool

	// droppedFields names selected fields whose cap was breached; the
	// buffered parse applies the same whole-field omission.
	droppedFields map[string]bool

	// key assembly (bounded, DECODED); isKey marks the current string
	// as a key.
	isKey      bool
	keyBuf     []byte
	keyOver    bool
	unicodeAcc []byte // pending \uXXXX hex digits for key decoding

	unicodeLeft int
	literal     string
	literalLeft int

	// capture state: while captureName != "", value bytes append to
	// fields[captureName] until captureDepth is restored. Leading
	// colon and whitespace are never charged or retained.
	fields       map[string][]byte
	seen         map[string]bool // decoded selected keys already seen per owner
	captureName  string
	captureDepth int
	captureOver  bool

	// presence: a non-empty string value observed at a recognized
	// path-qualified output-bearing position.
	presentTop    map[string]bool
	presentNested bool
	valueTopKey   string
	valueNested   bool
	valueBytes    int
}

func newControlScanner(mode scanMode) *controlScanner {
	return &controlScanner{
		mode:          mode,
		fields:        make(map[string][]byte),
		seen:          make(map[string]bool),
		presentTop:    make(map[string]bool),
		droppedFields: make(map[string]bool),
	}
}

// selectedKeyName reports whether the key just completed selects a
// retained control field in the current mode.
func (s *controlScanner) selectedKeyName() (string, bool) {
	if len(s.stack) == 0 || !s.stack[len(s.stack)-1].isObject {
		return "", false
	}
	key := s.stack[len(s.stack)-1].key
	if key == "" {
		return "", false
	}
	switch s.mode {
	case scanUnaryRoot:
		if len(s.stack) == 1 && key != "type" && scanFieldCaps[key] > 0 {
			return key, true
		}
	case scanSSEEnvelope:
		if len(s.stack) == 1 && key == "type" {
			return key, true
		}
		if len(s.stack) == 2 && s.stack[0].key == "response" && key != "type" && scanFieldCaps[key] > 0 {
			return key, true
		}
	}
	return "", false
}

// feed consumes one segment. It never returns an error: failures set
// invalid and further input only advances the trailing check.
func (s *controlScanner) feed(p []byte) {
	for index := range p {
		b := p[index]
		if s.captureName != "" && !s.captureOver {
			field := s.fields[s.captureName]
			// Structural colon and whitespace before the value's first
			// byte are neither charged nor retained.
			if len(field) != 0 || (b != ':' && !isJSONSpace(b)) {
				if len(field)+1 > rawCapFor(s.captureName) {
					s.captureOver = true
					s.fieldOver = true
					s.droppedFields[s.captureName] = true
					delete(s.fields, s.captureName)
				} else {
					s.fields[s.captureName] = append(field, b)
				}
			}
		}
		s.step(b)
		if s.invalid && s.state == scanTrailing {
			return // nothing further can change the outcome
		}
	}
}

func (s *controlScanner) fail() {
	s.invalid = true
	s.state = scanTrailing
	s.captureName = ""
}

func (s *controlScanner) step(b byte) {
	switch s.state {
	case scanValue:
		s.stepValue(b)
	case scanString:
		s.stepString(b)
	case scanStringEscape:
		switch b {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			s.state = scanString
			if s.isKey && !s.keyOver {
				s.keyBuf = append(s.keyBuf, decodedEscape(b))
				s.boundKey()
			}
			if !s.isKey {
				s.valueBytes++
			}
		case 'u':
			s.state = scanStringUnicode
			s.unicodeLeft = 4
			s.unicodeAcc = s.unicodeAcc[:0]
			if !s.isKey {
				s.valueBytes++
			}
		default:
			s.fail()
		}
	case scanStringUnicode:
		if !isHexDigit(b) {
			s.fail()
			return
		}
		s.unicodeAcc = append(s.unicodeAcc, b)
		s.unicodeLeft--
		if s.unicodeLeft == 0 {
			s.state = scanString
			if s.isKey && !s.keyOver {
				// Decode the escape so key comparison uses
				// encoding/json's spelling. Surrogate pairs cannot spell
				// any selected ASCII key, so a lone surrogate yields a
				// non-matching rune, which is conservative and correct.
				value, err := strconv.ParseUint(string(s.unicodeAcc), 16, 32)
				if err != nil {
					s.fail()
					return
				}
				s.keyBuf = utf8.AppendRune(s.keyBuf, rune(value))
				s.boundKey()
			}
		}
	case scanNumberSign:
		switch {
		case b == '0':
			s.state = scanNumberZero
		case b >= '1' && b <= '9':
			s.state = scanNumberInt
		default:
			s.fail()
		}
	case scanNumberZero:
		// A leading zero admits no further integer digits.
		if b >= '0' && b <= '9' {
			s.fail()
			return
		}
		s.stepNumber(b, true)
	case scanNumberInt:
		s.stepNumber(b, true)
	case scanNumberFracFirst:
		if b >= '0' && b <= '9' {
			s.state = scanNumberFrac
			return
		}
		s.fail()
	case scanNumberFrac:
		s.stepNumber(b, false)
	case scanNumberExpSign:
		if b == '+' || b == '-' {
			s.state = scanNumberExpFirst
			return
		}
		if b >= '0' && b <= '9' {
			s.state = scanNumberExp
			return
		}
		s.fail()
	case scanNumberExpFirst:
		if b >= '0' && b <= '9' {
			s.state = scanNumberExp
			return
		}
		s.fail()
	case scanNumberExp:
		if b >= '0' && b <= '9' {
			return
		}
		s.endScalar()
		s.step(b)
	case scanLiteral:
		if s.literalLeft == 0 || b != s.literal[len(s.literal)-s.literalLeft] {
			s.fail()
			return
		}
		s.literalLeft--
		if s.literalLeft == 0 {
			s.endScalar()
		}
	case scanObjectKeyOrEnd:
		switch {
		case b == '}':
			s.pop()
		case b == '"':
			s.beginKey()
		case isJSONSpace(b):
		default:
			s.fail()
		}
	case scanObjectColon:
		switch {
		case b == ':':
			s.state = scanValue
		case isJSONSpace(b):
		default:
			s.fail()
		}
	case scanObjectValueDone:
		switch {
		case b == ',':
			s.state = scanObjectKey
		case b == '}':
			s.pop()
		case isJSONSpace(b):
		default:
			s.fail()
		}
	case scanObjectKey:
		switch {
		case b == '"':
			s.beginKey()
		case isJSONSpace(b):
		default:
			s.fail()
		}
	case scanArrayValueDone:
		switch {
		case b == ',':
			s.state = scanValue
		case b == ']':
			s.pop()
		case isJSONSpace(b):
		default:
			s.fail()
		}
	case scanValueOrArrayEnd:
		if b == ']' {
			s.pop()
			return
		}
		s.state = scanValue
		s.step(b)
	case scanTrailing:
		if !isJSONSpace(b) {
			s.invalid = true
			s.complete = false
		}
	}
}

func decodedEscape(b byte) byte {
	switch b {
	case 'b':
		return '\b'
	case 'f':
		return '\f'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	default:
		return b // '"', '\\', '/'
	}
}

func (s *controlScanner) stepValue(b byte) {
	switch {
	case isJSONSpace(b):
	case b == '{':
		if !s.push(true) {
			return
		}
		s.state = scanObjectKeyOrEnd
	case b == '[':
		if !s.push(false) {
			return
		}
		s.state = scanValueOrArrayEnd
	case b == '"':
		s.isKey = false
		s.valueTopKey, s.valueNested = s.currentPresencePosition()
		s.valueBytes = 0
		s.state = scanString
		s.started = true
	case b == '-':
		s.state = scanNumberSign
		s.started = true
	case b == '0':
		s.state = scanNumberZero
		s.started = true
	case b >= '1' && b <= '9':
		s.state = scanNumberInt
		s.started = true
	case b == 't':
		s.literal, s.literalLeft = "true", 3
		s.state = scanLiteral
		s.started = true
	case b == 'f':
		s.literal, s.literalLeft = "false", 4
		s.state = scanLiteral
		s.started = true
	case b == 'n':
		s.literal, s.literalLeft = "null", 3
		s.state = scanLiteral
		s.started = true
	default:
		s.fail()
	}
}

func (s *controlScanner) stepString(b byte) {
	switch b {
	case '"':
		if s.isKey {
			s.endKey()
			return
		}
		if s.valueBytes > 0 {
			s.markPresence()
		}
		s.endScalarAfterString()
	case '\\':
		s.state = scanStringEscape
	default:
		if b < 0x20 {
			s.fail()
			return
		}
		if s.isKey {
			if !s.keyOver {
				s.keyBuf = append(s.keyBuf, b)
				s.boundKey()
			}
		} else {
			s.valueBytes++
		}
	}
}

func (s *controlScanner) stepNumber(b byte, allowFrac bool) {
	switch {
	case b >= '0' && b <= '9':
	case b == '.' && allowFrac:
		s.state = scanNumberFracFirst
	case b == 'e' || b == 'E':
		s.state = scanNumberExpSign
	default:
		s.endScalar()
		s.step(b)
	}
}

const maxScanKey = 64

func (s *controlScanner) boundKey() {
	if len(s.keyBuf) > maxScanKey {
		s.keyBuf = s.keyBuf[:0]
		s.keyOver = true
	}
}

func (s *controlScanner) beginKey() {
	s.isKey = true
	s.keyBuf = s.keyBuf[:0]
	s.keyOver = false
	s.state = scanString
}

func (s *controlScanner) endKey() {
	key := ""
	if !s.keyOver {
		key = string(s.keyBuf)
	}
	if len(s.stack) == 0 {
		s.fail()
		return
	}
	s.stack[len(s.stack)-1].key = key
	s.isKey = false
	s.state = scanObjectColon

	// The root "response" member itself is duplicate-tracked in
	// envelope mode: two terminal response objects are schema-invalid
	// even though the member is not retained as one bounded field.
	if s.mode == scanSSEEnvelope && len(s.stack) == 1 && key == "response" {
		if s.seen["response"] {
			s.duplicates = true
		}
		s.seen["response"] = true
	}

	if name, selected := s.selectedKeyName(); selected {
		if s.seen[s.seenKey(name)] {
			s.duplicates = true
		}
		s.seen[s.seenKey(name)] = true
		s.captureName = name
		s.captureDepth = len(s.stack)
		s.captureOver = false
		delete(s.fields, name)
	}
}

// seenKey namespaces duplicate detection per owning object.
func (s *controlScanner) seenKey(name string) string {
	if s.mode == scanSSEEnvelope && len(s.stack) == 2 {
		return "response." + name
	}
	return name
}

// currentPresencePosition matches the exact path-qualified
// output-bearing positions of the v5 table. Top-level positions are
// reported by key (the event type decides later whether they bear);
// nested positions cover ONLY the closed item/part/output shapes:
//
//	part.text, part.refusal
//	item.arguments, item.result, item.content[].text|refusal
//	response.output[].arguments|result,
//	response.output[].content[].text|refusal
//	and the same output[] shapes at the root in unary mode.
func (s *controlScanner) currentPresencePosition() (string, bool) {
	if len(s.stack) == 0 || !s.stack[len(s.stack)-1].isObject {
		return "", false
	}
	key := s.stack[len(s.stack)-1].key
	if len(s.stack) == 1 && s.mode == scanSSEEnvelope {
		switch key {
		case "delta", "text", "refusal", "arguments", "partial_image_b64":
			return key, true
		}
		return "", false
	}
	frames := s.stack
	entered := func(index int) string { return frames[index].enteredAs }
	object := func(index int) bool { return frames[index].isObject }
	array := func(index int) bool { return !frames[index].isObject }
	textual := key == "text" || key == "refusal"
	direct := key == "arguments" || key == "result"

	switch s.mode {
	case scanSSEEnvelope:
		switch len(frames) {
		case 2:
			if entered(1) == "part" && object(1) && textual {
				return "", true
			}
			if entered(1) == "item" && object(1) && direct {
				return "", true
			}
		case 4:
			if entered(1) == "item" && object(1) && entered(2) == "content" && array(2) && object(3) && textual {
				return "", true
			}
			if entered(1) == "response" && object(1) && entered(2) == "output" && array(2) && object(3) && direct {
				return "", true
			}
		case 6:
			if entered(1) == "response" && object(1) && entered(2) == "output" && array(2) && object(3) &&
				entered(4) == "content" && array(4) && object(5) && textual {
				return "", true
			}
		}
	case scanUnaryRoot:
		switch len(frames) {
		case 3:
			if entered(1) == "output" && array(1) && object(2) && direct {
				return "", true
			}
		case 5:
			if entered(1) == "output" && array(1) && object(2) &&
				entered(3) == "content" && array(3) && object(4) && textual {
				return "", true
			}
		}
	}
	return "", false
}

func (s *controlScanner) markPresence() {
	if s.valueTopKey != "" {
		s.presentTop[s.valueTopKey] = true
		return
	}
	if s.valueNested {
		s.presentNested = true
	}
}

func (s *controlScanner) push(isObject bool) bool {
	if len(s.stack) >= maxScanDepth {
		s.fail()
		return false
	}
	if len(s.stack) == 0 {
		s.rootObject = isObject
	}
	enteredAs := ""
	if len(s.stack) > 0 && s.stack[len(s.stack)-1].isObject {
		enteredAs = s.stack[len(s.stack)-1].key
	}
	s.stack = append(s.stack, scanFrame{isObject: isObject, enteredAs: enteredAs})
	s.started = true
	return true
}

func (s *controlScanner) pop() {
	if len(s.stack) == 0 {
		s.fail()
		return
	}
	s.stack = s.stack[:len(s.stack)-1]
	s.finishCapture()
	s.afterValue()
}

func (s *controlScanner) endScalar() {
	s.finishCapture()
	s.afterValue()
}

func (s *controlScanner) endScalarAfterString() {
	s.finishCapture()
	s.afterValue()
}

func (s *controlScanner) finishCapture() {
	if s.captureName != "" && len(s.stack) == s.captureDepth {
		s.captureName = ""
	}
}

func (s *controlScanner) afterValue() {
	if len(s.stack) == 0 {
		s.complete = true
		s.state = scanTrailing
		return
	}
	if s.stack[len(s.stack)-1].isObject {
		s.state = scanObjectValueDone
	} else {
		s.state = scanArrayValueDone
	}
}

// documentUsable reports whether hard verdicts may be derived: one
// complete top-level value, no structural failure, no duplicate
// selected keys, an object root.
func (s *controlScanner) documentUsable() bool {
	return s.complete && !s.invalid && !s.duplicates && s.rootObject
}

func isJSONSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isHexDigit(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}
