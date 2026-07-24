package langfuseopenai

// controlScanner is a bounded push-mode JSON lexer used for payloads
// that exceed the buffering caps. It proves structural validity of the
// WHOLE document (container-kind stack, full number grammar, string
// escapes, one top-level value plus trailing whitespace), retains only
// a fixed set of small control-plane fields as raw JSON (decoded later
// with encoding/json, so escape and Unicode semantics match the
// ordinary parser exactly), detects duplicate selected keys, and tracks
// presence of non-empty output-bearing values without retaining them.
// Memory is bounded by the per-field caps and maxScanDepth regardless
// of input size.

// maxScanDepth bounds the container-kind stack, far above any pinned
// Responses schema path; over-depth marks the document invalid.
const maxScanDepth = 64

// scanFieldCaps bound each retained raw field (bytes of raw JSON).
var scanFieldCaps = map[string]int{
	"type":               80,
	"status":             80,
	"model":              300,
	"usage":              4096,
	"error":              4096,
	"incomplete_details": 1024,
}

// presenceKeys are the output-bearing field names tracked for TTFT:
// top-level delta/done payload fields plus the nested names inside
// item, part, and response.output subtrees. Values are checked for
// string non-emptiness only; nothing is retained.
var presenceKeys = map[string]bool{
	"delta":             true,
	"text":              true,
	"refusal":           true,
	"arguments":         true,
	"partial_image_b64": true,
	"result":            true,
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
	// key is the bounded current member key (empty for arrays or keys
	// longer than the bound; long keys are never selected or
	// presence-relevant).
	key string
	// presence marks a subtree whose nested output-bearing names count
	// toward presence tracking (item, part, output, response.output).
	presence bool
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

	// key assembly (bounded); isKey marks the current string as a key.
	isKey   bool
	keyBuf  []byte
	keyOver bool

	// unicodeLeft counts remaining \uXXXX hex digits.
	unicodeLeft int
	// literalLeft counts remaining bytes of true/false/null.
	literal     string
	literalLeft int

	// capture state: while captureName != "", consumed bytes append to
	// fields[captureName] until captureDepth is restored.
	fields       map[string][]byte
	seen         map[string]bool // selected keys already seen (per owner)
	captureName  string
	captureDepth int
	captureOver  bool

	// presence: a non-empty string value observed at a recognized
	// (top-level or presence-subtree) output-bearing key. valueBytes
	// counts content bytes of the current string value.
	presentTop    map[string]bool
	presentNested bool
	valueKey      string
	valuePresence bool
	valueBytes    int
}

func newControlScanner(mode scanMode) *controlScanner {
	return &controlScanner{
		mode:       mode,
		fields:     make(map[string][]byte),
		seen:       make(map[string]bool),
		presentTop: make(map[string]bool),
	}
}

// selectedOwner reports whether the CURRENT container is one whose
// member keys are selected control fields: the root object in unary
// mode; the root object (type only) and the root "response" object in
// SSE mode.
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
			// Note: stack[0].key is the key under which the CURRENT
			// container was entered; see push().
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
			// Raw capture appends every byte of the selected value as it
			// is consumed, including this one (append before the state
			// machine so string closers and container ends are kept).
			field := s.fields[s.captureName]
			if len(field)+1 > scanFieldCaps[s.captureName] {
				s.captureOver = true
				s.fieldOver = true
				delete(s.fields, s.captureName)
			} else {
				s.fields[s.captureName] = append(field, b)
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
				// Escaped key bytes keep raw form; decoding happens only
				// for comparison below via json.Unmarshal at finish. For
				// key MATCHING we require the raw spelling; an escaped
				// spelling of a selected key cannot match and therefore
				// selects nothing — conservative, and the whole-document
				// validity rule still holds. Presence keys likewise.
				s.keyBuf = append(s.keyBuf, '\\', b)
				s.boundKey()
			}
			if !s.isKey {
				s.valueBytes++
			}
		case 'u':
			s.state = scanStringUnicode
			s.unicodeLeft = 4
			if s.isKey && !s.keyOver {
				s.keyBuf = append(s.keyBuf, '\\', 'u')
				s.boundKey()
			}
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
		s.unicodeLeft--
		if s.unicodeLeft == 0 {
			s.state = scanString
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
		s.valueKey, s.valuePresence = s.currentPresenceKey()
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
		if s.valuePresence && s.valueBytes > 0 {
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

	if name, selected := s.selectedKeyName(); selected {
		if s.seen[s.seenKey(name)] {
			s.duplicates = true
		}
		s.seen[s.seenKey(name)] = true
		s.captureName = name
		s.captureDepth = len(s.stack)
		s.captureOver = false
		delete(s.fields, name) // capture appends from the value start
	}
}

// seenKey namespaces duplicate detection per owning object so a
// "status" inside response and a "status" inside usage never collide.
func (s *controlScanner) seenKey(name string) string {
	if s.mode == scanSSEEnvelope && len(s.stack) == 2 {
		return "response." + name
	}
	return name
}

// currentPresenceKey reports whether a string value about to be read
// sits at a recognized output-bearing position: a presence key at the
// event's top level, or a presence key anywhere inside a presence
// subtree (item, part, output, response.output).
func (s *controlScanner) currentPresenceKey() (string, bool) {
	if len(s.stack) == 0 || !s.stack[len(s.stack)-1].isObject {
		return "", false
	}
	key := s.stack[len(s.stack)-1].key
	if !presenceKeys[key] {
		return "", false
	}
	if len(s.stack) == 1 && s.mode == scanSSEEnvelope {
		return key, true
	}
	for _, frame := range s.stack {
		if frame.presence {
			return key, true
		}
	}
	return "", false
}

func (s *controlScanner) markPresence() {
	if len(s.stack) == 1 && s.mode == scanSSEEnvelope {
		s.presentTop[s.valueKey] = true
		return
	}
	s.presentNested = true
}

func (s *controlScanner) push(isObject bool) bool {
	if len(s.stack) >= maxScanDepth {
		s.fail()
		return false
	}
	if len(s.stack) == 0 {
		s.rootObject = isObject
	}
	presence := false
	if len(s.stack) > 0 {
		if s.stack[len(s.stack)-1].presence {
			presence = true // arrays and objects both propagate the subtree
		} else if s.stack[len(s.stack)-1].isObject {
			switch s.stack[len(s.stack)-1].key {
			case "item", "part":
				presence = s.mode == scanSSEEnvelope && len(s.stack) == 1
			case "output":
				// response.output in SSE mode; root output in unary mode.
				presence = (s.mode == scanSSEEnvelope && len(s.stack) == 2 && s.stack[0].key == "response") ||
					(s.mode == scanUnaryRoot && len(s.stack) == 1)
			}
		}
	}
	s.stack = append(s.stack, scanFrame{isObject: isObject, presence: presence})
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

// endScalar completes a number or literal value.
func (s *controlScanner) endScalar() {
	s.finishCapture()
	s.afterValue()
}

// endScalarAfterString completes a string VALUE (the closing quote was
// already consumed by capture).
func (s *controlScanner) endScalarAfterString() {
	s.finishCapture()
	s.afterValue()
}

// finishCapture closes raw retention when the captured value's depth
// is restored.
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
// selected keys.
func (s *controlScanner) documentUsable() bool {
	return s.complete && !s.invalid && !s.duplicates && s.rootObject
}

func isJSONSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isHexDigit(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}
