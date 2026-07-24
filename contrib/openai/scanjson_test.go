package langfuseopenai

import (
	"fmt"
	"strings"
	"testing"
)

func scan(mode scanMode, payload string, chunk int) *controlScanner {
	scanner := newControlScanner(mode)
	data := []byte(payload)
	if chunk <= 0 {
		chunk = len(data)
	}
	for start := 0; start < len(data); start += chunk {
		end := min(start+chunk, len(data))
		scanner.feed(data[start:end])
	}
	return scanner
}

func TestScannerGrammarValidity(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		usable  bool
	}{
		{"minimal-object", `{}`, true},
		{"nested-valid", `{"type":"t","response":{"output":[{"content":[{"text":"x"}]}]}}`, true},
		{"trailing-whitespace", `{"a":1}` + " \t\r\n", true},
		{"trailing-garbage", `{"a":1}x`, false},
		{"second-value", `{"a":1}{"b":2}`, false},
		{"array-root", `[{"a":1}]`, false},
		{"string-root", `"hello"`, false},
		{"number-root", `12`, false},
		{"unterminated", `{"a":1`, false},
		{"mismatched-closers", `{"a":[1}}`, false},
		{"trailing-comma-object", `{"a":1,}`, false},
		{"trailing-comma-array", `{"a":[1,]}`, false},
		{"leading-zero", `{"a":01}`, false},
		{"bare-minus", `{"a":-}`, false},
		{"exp-no-digits", `{"a":1e}`, false},
		{"bad-literal", `{"a":tru}`, false},
		{"bad-escape", `{"a":"\x"}`, false},
		{"bad-unicode", `{"a":"\u12g4"}`, false},
		{"control-char-in-string", "{\"a\":\"\x01\"}", false},
		{"valid-escapes", `{"a":"q\"\\\/\b\f\n\r\té"}`, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, chunk := range []int{0, 1, 3} {
				scanner := scan(scanSSEEnvelope, testCase.payload, chunk)
				if got := scanner.documentUsable(); got != testCase.usable {
					t.Fatalf("chunk %d: usable = %v, want %v (invalid=%v complete=%v dup=%v root=%v)",
						chunk, got, testCase.usable, scanner.invalid, scanner.complete,
						scanner.duplicates, scanner.rootObject)
				}
			}
		})
	}
}

func TestScannerValidNumberFormsAreUsable(t *testing.T) {
	scanner := scan(scanSSEEnvelope, `{"a":-0.5e+10,"b":0,"c":1E2}`, 1)
	if !scanner.documentUsable() {
		t.Fatalf("valid number grammar rejected: invalid=%v complete=%v", scanner.invalid, scanner.complete)
	}
}

func TestScannerDuplicateSelectedKeys(t *testing.T) {
	cases := []struct {
		name      string
		mode      scanMode
		payload   string
		duplicate bool
	}{
		{"dup-type", scanSSEEnvelope, `{"type":"a","type":"b"}`, true},
		{"dup-response-status", scanSSEEnvelope, `{"response":{"status":"completed","status":"failed"}}`, true},
		{"dup-unary-status", scanUnaryRoot, `{"status":"completed","status":"failed"}`, true},
		{"dup-unary-usage", scanUnaryRoot, `{"usage":{},"usage":{}}`, true},
		{"same-name-different-owner", scanSSEEnvelope, `{"type":"a","response":{"status":"x","usage":{"status":"y"}}}`, false},
		{"unselected-dups", scanSSEEnvelope, `{"other":1,"other":2}`, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			scanner := scan(testCase.mode, testCase.payload, 1)
			if scanner.duplicates != testCase.duplicate {
				t.Fatalf("duplicates = %v, want %v", scanner.duplicates, testCase.duplicate)
			}
		})
	}
}

func TestScannerFieldRetentionAndDecoding(t *testing.T) {
	payload := `{"type":"response.completed","response":{` +
		`"status":"completed","model":"gpt-5-mini",` +
		`"usage":{"input_tokens":5,"output_tokens":7,"total_tokens":12},` +
		`"error":{"code":"c","message":"m"}}}`
	for _, chunk := range []int{0, 1, 5} {
		scanner := scan(scanSSEEnvelope, payload, chunk)
		if !scanner.documentUsable() {
			t.Fatalf("chunk %d: payload rejected", chunk)
		}
		// Escaped selected values decode with encoding/json semantics.
		eventType, ok := decodeScannedField(scanner, "type")
		if !ok || eventType != "response.completed" {
			t.Fatalf("chunk %d: type = %q (%v)", chunk, eventType, ok)
		}
		status, _ := decodeScannedField(scanner, "status")
		if status != "completed" {
			t.Fatalf("chunk %d: status = %q", chunk, status)
		}
		model, _ := decodeScannedField(scanner, "model")
		if model != "gpt-5-mini" {
			t.Fatalf("chunk %d: model = %q", chunk, model)
		}
		raw, _ := scannedRaw(scanner.fields["usage"])
		if !strings.Contains(string(raw), "input_tokens") {
			t.Fatalf("chunk %d: usage raw = %q", chunk, raw)
		}
	}
}

func TestScannerUnaryModeSelectsRootFields(t *testing.T) {
	payload := `{"status":"incomplete","model":"m","usage":{"input_tokens":1,"output_tokens":2},` +
		`"response":{"status":"IGNORED-NESTED"}}`
	scanner := scan(scanUnaryRoot, payload, 1)
	if !scanner.documentUsable() {
		t.Fatal("payload rejected")
	}
	status, _ := decodeScannedField(scanner, "status")
	if status != "incomplete" {
		t.Fatalf("root status = %q", status)
	}
	// SSE mode over the same bytes must NOT pick up root fields.
	envelope := scan(scanSSEEnvelope, payload, 1)
	if _, found := envelope.fields["model"]; found {
		t.Fatal("envelope mode must not select root model")
	}
}

func TestScannerFieldCapDropsFieldAndSalvagesLater(t *testing.T) {
	huge := strings.Repeat("h", 8192)
	payload := `{"type":"response.completed","response":{"usage":{"pad":"` + huge + `"},` +
		`"status":"completed","model":"m"}}`
	scanner := scan(scanSSEEnvelope, payload, 7)
	if !scanner.documentUsable() {
		t.Fatal("payload rejected")
	}
	if !scanner.fieldOver {
		t.Fatal("over-cap usage must be declared")
	}
	if _, found := scanner.fields["usage"]; found {
		t.Fatal("over-cap field must be dropped whole, never truncated")
	}
	if status, _ := decodeScannedField(scanner, "status"); status != "completed" {
		t.Fatalf("later fields must be salvaged; status = %q", status)
	}
	if model, _ := decodeScannedField(scanner, "model"); model != "m" {
		t.Fatalf("model = %q", model)
	}
}

func TestScannerDepthBound(t *testing.T) {
	atLimit := strings.Repeat(`{"a":`, maxScanDepth-1) + `1` + strings.Repeat(`}`, maxScanDepth-1)
	if scanner := scan(scanUnaryRoot, atLimit, 1); scanner.invalid {
		t.Fatal("nesting at the limit must be valid")
	}
	overLimit := strings.Repeat(`{"a":`, maxScanDepth+1) + `1` + strings.Repeat(`}`, maxScanDepth+1)
	if scanner := scan(scanUnaryRoot, overLimit, 1); !scanner.invalid {
		t.Fatal("nesting over the limit must be invalid")
	}
}

func TestScannerPresenceTracking(t *testing.T) {
	cases := []struct {
		name       string
		payload    string
		wantTop    string
		wantNested bool
	}{
		{"top-delta", `{"type":"t","delta":"x"}`, "delta", false},
		{"empty-delta", `{"type":"t","delta":""}`, "", false},
		{"top-text", `{"type":"t","text":"x"}`, "text", false},
		{"item-nested", `{"type":"t","item":{"content":[{"text":"x"}]}}`, "", true},
		{"part-nested", `{"type":"t","part":{"refusal":"r"}}`, "", true},
		{"terminal-output", `{"type":"t","response":{"output":[{"content":[{"text":"x"}]}]}}`, "", true},
		{"image-result", `{"type":"t","response":{"output":[{"result":"b64"}]}}`, "", true},
		{"unrelated-nested", `{"type":"t","meta":{"text":"not output"}}`, "", false},
		{"usage-not-presence", `{"type":"t","response":{"usage":{"text":"no"}}}`, "", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			scanner := scan(scanSSEEnvelope, testCase.payload, 1)
			if testCase.wantTop != "" && !scanner.presentTop[testCase.wantTop] {
				t.Fatalf("top presence %q missing: %v", testCase.wantTop, scanner.presentTop)
			}
			if testCase.wantTop == "" {
				for key := range scanner.presentTop {
					t.Fatalf("unexpected top presence %q", key)
				}
			}
			if scanner.presentNested != testCase.wantNested {
				t.Fatalf("nested presence = %v, want %v", scanner.presentNested, testCase.wantNested)
			}
		})
	}
}

func TestScannerMemoryStaysBounded(t *testing.T) {
	// A multi-megabyte unretained string must not grow scanner state:
	// the only retained bytes are the bounded fields and the fixed
	// stack. Feed 4 MiB through and inspect the retained sizes.
	scanner := newControlScanner(scanSSEEnvelope)
	scanner.feed([]byte(`{"type":"response.output_text.delta","delta":"`))
	filler := []byte(strings.Repeat("f", 64<<10))
	for range 64 {
		scanner.feed(filler)
	}
	scanner.feed([]byte(`"}`))
	if !scanner.documentUsable() {
		t.Fatal("payload rejected")
	}
	retained := len(scanner.keyBuf) + cap(scanner.stack)*32
	for _, field := range scanner.fields {
		retained += len(field)
	}
	if retained > 16<<10 {
		t.Fatalf("scanner retained %d bytes for a 4 MiB payload", retained)
	}
	if !scanner.presentTop["delta"] {
		t.Fatal("presence must survive without retention")
	}
}

func FuzzControlScanner(f *testing.F) {
	f.Add(`{"type":"response.completed","response":{"status":"completed"}}`, 1)
	f.Add(`{"a":[1,2,{"b":"c"}]}`, 3)
	f.Add(`{"type":"t","delta":"`+strings.Repeat("x", 100)+`"}`, 7)
	f.Fuzz(func(t *testing.T, payload string, chunk int) {
		if chunk <= 0 || chunk > len(payload)+1 {
			chunk = 1
		}
		whole := scan(scanSSEEnvelope, payload, 0)
		split := scan(scanSSEEnvelope, payload, chunk)
		if whole.documentUsable() != split.documentUsable() {
			t.Fatalf("fragmentation changed usability for %q", payload)
		}
		if whole.documentUsable() {
			wholeType, _ := decodeScannedField(whole, "type")
			splitType, _ := decodeScannedField(split, "type")
			if wholeType != splitType {
				t.Fatalf("fragmentation changed type: %q vs %q", wholeType, splitType)
			}
		}
	})
}

func TestScannerChunkIndependence(t *testing.T) {
	payload := `{"type":"response.completed","response":{"status":"completed","model":"` +
		strings.Repeat("m", 200) + `","usage":{"input_tokens":1}}}`
	baseline := scan(scanSSEEnvelope, payload, 0)
	for chunk := 1; chunk <= len(payload); chunk++ {
		scanner := scan(scanSSEEnvelope, payload, chunk)
		if scanner.documentUsable() != baseline.documentUsable() {
			t.Fatalf("chunk %d changed usability", chunk)
		}
		if fmt.Sprintf("%v", scanner.fields) != fmt.Sprintf("%v", baseline.fields) {
			t.Fatalf("chunk %d changed retained fields", chunk)
		}
	}
}

func TestScannerDecodedKeySemantics(t *testing.T) {
	// An escaped spelling of a selected key decodes to the same
	// selection encoding/json would make.
	scanner := scan(scanSSEEnvelope, `{"\u0074ype":"response.completed"}`, 1)
	if !scanner.documentUsable() {
		t.Fatal("payload rejected")
	}
	if eventType, ok := decodeScannedField(scanner, "type"); !ok || eventType != "response.completed" {
		t.Fatalf("escaped key not selected: %q (%v)", eventType, ok)
	}

	// Plain-plus-escaped spellings of one selected key are duplicates.
	scanner = scan(scanSSEEnvelope, `{"type":"a","\u0074ype":"b"}`, 1)
	if !scanner.duplicates {
		t.Fatal("plain/escaped key collision must be a duplicate")
	}

	// Two root response members are schema-invalid even though the
	// member itself is not retained.
	scanner = scan(scanSSEEnvelope, `{"response":{"status":"completed"},"response":{"status":"failed"}}`, 1)
	if !scanner.duplicates {
		t.Fatal("duplicate terminal response object must be rejected")
	}
}

func TestScannerDecodedValueCaps(t *testing.T) {
	atCap := strings.Repeat("s", scanFieldCaps["status"])
	scanner := scan(scanUnaryRoot, `{"status":"`+atCap+`"}`, 1)
	if value, ok := decodeScannedField(scanner, "status"); !ok || value != atCap {
		t.Fatalf("at-cap decoded value rejected: %v", ok)
	}
	overCap := atCap + "x"
	scanner = scan(scanUnaryRoot, `{"status":"`+overCap+`"}`, 1)
	if _, ok := decodeScannedField(scanner, "status"); ok {
		t.Fatal("over-cap decoded value must be dropped")
	}
	if !scanner.droppedFields["status"] {
		t.Fatal("the drop must be projected for the buffered path")
	}

	// An escaped spelling cannot smuggle an over-cap value past the
	// raw bound.
	escaped := strings.Repeat(`\u0073`, scanFieldCaps["status"]+1)
	scanner = scan(scanUnaryRoot, `{"status":"`+escaped+`"}`, 7)
	if _, ok := decodeScannedField(scanner, "status"); ok {
		t.Fatal("escaped over-cap value must be dropped")
	}

	// Colon and whitespace are structural and never charged: a value
	// exactly at cap survives arbitrary surrounding whitespace.
	scanner = scan(scanUnaryRoot, `{"status"  :   "`+atCap+`"}`, 1)
	if value, ok := decodeScannedField(scanner, "status"); !ok || value != atCap {
		t.Fatalf("whitespace was charged against the value cap: %v", ok)
	}
}

func TestScannerDepthBoundExact(t *testing.T) {
	atLimit := strings.Repeat(`{"a":`, maxScanDepth) + `1` + strings.Repeat(`}`, maxScanDepth)
	if scanner := scan(scanUnaryRoot, atLimit, 1); scanner.invalid {
		t.Fatalf("nesting depth %d must be valid", maxScanDepth)
	}
	overLimit := strings.Repeat(`{"a":`, maxScanDepth+1) + `1` + strings.Repeat(`}`, maxScanDepth+1)
	if scanner := scan(scanUnaryRoot, overLimit, 1); !scanner.invalid {
		t.Fatalf("nesting depth %d must be invalid", maxScanDepth+1)
	}
}
