package attributes_test

import (
	"encoding/json"
	"math"
	"testing"
	"unicode/utf8"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

func FuzzEncode(f *testing.F) {
	f.Add("key", []byte("value"), false, int64(0), 0.25)
	f.Add("__proto__", []byte{0xff, 0x00}, true, int64(-1), -1.5)
	f.Add("nan", []byte("payload"), false, int64(7), math.NaN())
	f.Add("infinity", []byte("payload"), true, int64(7), math.Inf(1))
	f.Fuzz(func(t *testing.T, key string, data []byte, flag bool, number int64, float float64) {
		value := map[string]any{
			key:      string(data),
			"bytes":  data,
			"flag":   flag,
			"number": number,
			"float":  float,
		}
		encoded, ok := lfattr.Encode(value, nil, "fuzz value")
		if !ok {
			return
		}
		if len(encoded) > lfattr.MaxSerializedBytes {
			t.Fatalf("encoded value exceeds internal limit: %d", len(encoded))
		}
		if !json.Valid([]byte(encoded)) {
			t.Fatalf("Encode returned invalid JSON")
		}
	})
}

func FuzzNormalizeUsage(f *testing.F) {
	f.Add(int64(10), int64(5), int64(2), int64(1), int64(1), "input_audio_tokens", int64(1))
	f.Add(int64(-1), int64(0), int64(0), int64(0), int64(0), "total", int64(-1))
	f.Fuzz(func(
		t *testing.T,
		input, output, cacheRead, cacheCreate, reasoning int64,
		key string,
		detail int64,
	) {
		encoded, ok := lfattr.NormalizeUsage(
			input,
			output,
			cacheRead,
			cacheCreate,
			reasoning,
			map[string]int64{key: detail},
		)
		if ok && !json.Valid([]byte(encoded)) {
			t.Fatalf("NormalizeUsage returned invalid JSON")
		}
	})
}

func FuzzEncodeMetadataObject(f *testing.F) {
	f.Add(`{"a":{"b":1},"a.b":2}`)
	f.Add(`{"x":[1,{"y":null}],"z":"s"}`)
	f.Add(`{"n":12345678901234567890123}`)
	f.Fuzz(func(t *testing.T, raw string) {
		var metadata map[string]any
		if json.Unmarshal([]byte(raw), &metadata) != nil {
			return
		}
		encoded, present, err := lfattr.EncodeMetadataObject(metadata, 4<<10)
		if err != nil || !present {
			return
		}
		if len(encoded) > 4<<10 {
			t.Fatalf("encoded metadata exceeds its limit: %d", len(encoded))
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
			t.Fatalf("encoded metadata is not a JSON object: %v", err)
		}
		// Every leaf must be a string, as dotted attributes are.
		var check func(map[string]any)
		check = func(object map[string]any) {
			for _, value := range object {
				switch value := value.(type) {
				case string:
				case map[string]any:
					check(value)
				default:
					t.Fatalf("non-string leaf %T in %s", value, encoded)
				}
			}
		}
		check(decoded)
		again, _, err := lfattr.EncodeMetadataObject(decoded, 4<<10)
		if err != nil || again != encoded {
			t.Fatalf("encoding is not stable: %s then %s (%v)", encoded, again, err)
		}
	})
}

func FuzzEncodeContent(f *testing.F) {
	f.Add([]byte(`"Paris"`))
	f.Add([]byte(`{"a": [1, 2.50]}`))
	f.Add([]byte("\xff"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		encoded, present, err := lfattr.EncodeContent(json.RawMessage(raw), 1<<10)
		if err != nil || !present {
			return
		}
		if len(encoded) > 1<<10 {
			t.Fatalf("encoded content exceeds its limit: %d", len(encoded))
		}
		if !utf8.ValidString(encoded) {
			t.Fatalf("encoded content is not valid UTF-8")
		}
	})
}
