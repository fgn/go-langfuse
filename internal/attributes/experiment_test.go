package attributes_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

func TestEncodeContent(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		value   any
		want    string
		present bool
		err     error
	}{
		"nil":             {value: nil},
		"typed nil":       {value: []string(nil)},
		"empty string":    {value: "", present: true},
		"string":          {value: "Paris", want: "Paris", present: true},
		"JSON string":     {value: `{"a":1}`, want: `{"a":1}`, present: true},
		"map":             {value: map[string]any{"b": 1, "a": "<"}, want: `{"a":"\u003c","b":1}`, present: true},
		"raw string":      {value: json.RawMessage(` "Pa\u0072is" `), want: "Paris", present: true},
		"raw number":      {value: json.RawMessage(`12345678901234567890`), want: "12345678901234567890", present: true},
		"raw object":      {value: json.RawMessage("{ \"a\" : [1, 2] }"), want: `{"a":[1,2]}`, present: true},
		"raw null":        {value: json.RawMessage(`null`)},
		"raw empty":       {value: json.RawMessage(` `)},
		"raw invalid":     {value: json.RawMessage(`{`), err: lfattr.ErrContentInvalid},
		"invalid UTF-8":   {value: "\xff", err: lfattr.ErrContentInvalid},
		"too large":       {value: strings.Repeat("x", 11), err: lfattr.ErrContentTooLarge},
		"raw too large":   {value: json.RawMessage(`"` + strings.Repeat("x", 11) + `"`), err: lfattr.ErrContentTooLarge},
		"unsupported":     {value: func() {}, err: lfattr.ErrContentInvalid},
		"marshaler null":  {value: nullJSON{}},
		"marshaler panic": {value: panicJSON{}, err: lfattr.ErrContentInvalid},
	} {
		limit := 64
		if errors.Is(test.err, lfattr.ErrContentTooLarge) {
			limit = 10
		}
		got, present, err := lfattr.EncodeContent(test.value, limit)
		if !errors.Is(err, test.err) || (err == nil) != (test.err == nil) {
			t.Errorf("%s: error = %v, want %v", name, err, test.err)
			continue
		}
		if got != test.want || present != test.present {
			t.Errorf("%s: EncodeContent() = %q, %t; want %q, %t", name, got, present, test.want, test.present)
		}
	}
}

type nullJSON struct{}

func (nullJSON) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

type panicJSON struct{}

func (panicJSON) MarshalJSON() ([]byte, error) { panic("PANIC-PAYLOAD") }

func TestEncodeMetadataObject(t *testing.T) {
	t.Parallel()
	got, present, err := lfattr.EncodeMetadataObject(map[string]any{
		"s": "a&b", "n": 1.5, "i": json.Number("9007199254740993"), "b": false, "z": nil,
		"list": []any{"x", 2}, "nested": map[string]any{"deep": map[string]any{"k": "v"}, "gone": nil},
		"empty": map[string]any{},
	}, 1<<10)
	want := `{"b":"false","i":"9007199254740993","list":"[\"x\",2]","n":"1.5","nested":{"deep":{"k":"v"}},"s":"a&b"}`
	if err != nil || !present || got != want {
		t.Fatalf("EncodeMetadataObject() = %s, %t, %v\nwant %s", got, present, err, want)
	}
	if _, present, err := lfattr.EncodeMetadataObject(map[string]any{"only": nil}, 1<<10); present || err != nil {
		t.Fatalf("an object without leaves = %t, %v; want absent", present, err)
	}
	for name, metadata := range map[string]map[string]any{
		"collision":     {"a.b": "1", "a": map[string]any{"b": "2"}},
		"proto":         {"__proto__": "x"},
		"empty segment": {"a..b": "x"},
		"empty key":     {"": "x"},
		"long path":     {strings.Repeat("k", 150): map[string]any{strings.Repeat("j", 60): "x"}},
		"too deep":      {"d": json.RawMessage(strings.Repeat(`{"a":`, lfattr.MaxMetadataDepth) + `1` + strings.Repeat(`}`, lfattr.MaxMetadataDepth))},
	} {
		if _, _, err := lfattr.EncodeMetadataObject(metadata, 1<<10); !errors.Is(err, lfattr.ErrMetadataShape) {
			t.Errorf("%s: error = %v, want ErrMetadataShape", name, err)
		}
	}
	if _, _, err := lfattr.EncodeMetadataObject(map[string]any{"x": strings.Repeat("v", 2048)}, 1<<10); !errors.Is(err, lfattr.ErrContentTooLarge) {
		t.Fatalf("oversized metadata error = %v", err)
	}
}
