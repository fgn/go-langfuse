package attributes_test

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	lfattr "github.com/fgn/lunte/internal/attributes"
)

type pointerOnlyLargeString string

func (*pointerOnlyLargeString) MarshalJSON() ([]byte, error) { return []byte(`"small"`), nil }

type jsonOnlyMapKey string

func (jsonOnlyMapKey) MarshalJSON() ([]byte, error) { return []byte(`"small"`), nil }

type jsonPreflightProbe struct{ calls *int }

func (p jsonPreflightProbe) MarshalJSON() ([]byte, error) {
	*p.calls++
	return []byte("null"), nil
}

func TestEncodeIsDeterministic(t *testing.T) {
	left := map[string]any{
		"z": map[string]any{"second": 2, "first": 1},
		"a": []any{"text", false, 0},
	}
	right := map[string]any{}
	right["a"] = []any{"text", false, 0}
	right["z"] = map[string]any{"first": 1, "second": 2}

	leftEncoded, leftOK := lfattr.Encode(left, nil, "input")
	rightEncoded, rightOK := lfattr.Encode(right, nil, "input")
	if !leftOK || !rightOK {
		t.Fatalf("Encode() ok = (%v, %v), want both true", leftOK, rightOK)
	}
	if leftEncoded != rightEncoded {
		t.Fatalf("equivalent values encoded differently:\nleft:  %s\nright: %s", leftEncoded, rightEncoded)
	}
	const want = `{"a":["text",false,0],"z":{"first":1,"second":2}}`
	if leftEncoded != want {
		t.Fatalf("Encode() = %s, want %s", leftEncoded, want)
	}
}

func TestEncodePreservesStringAndScalarValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "string stays unquoted", value: "plain text", want: "plain text"},
		{name: "empty string is present", value: "", want: ""},
		{name: "false is present", value: false, want: "false"},
		{name: "zero is present", value: 0, want: "0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := lfattr.Encode(test.value, nil, "value")
			if !ok {
				t.Fatal("Encode() ok = false, want true")
			}
			if got != test.want {
				t.Fatalf("Encode() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEncodeOmitsNilAndTypedNil(t *testing.T) {
	var pointer *struct{}
	var mapping map[string]string
	var slice []string
	var channel chan int
	var function func()

	values := []struct {
		name  string
		value any
	}{
		{name: "nil interface", value: nil},
		{name: "typed nil pointer", value: pointer},
		{name: "typed nil map", value: mapping},
		{name: "typed nil slice", value: slice},
		{name: "typed nil channel", value: channel},
		{name: "typed nil function", value: function},
	}

	for _, value := range values {
		t.Run(value.name, func(t *testing.T) {
			if got, ok := lfattr.Encode(value.value, nil, "value"); ok || got != "" {
				t.Fatalf("Encode() = (%q, %v), want (\"\", false)", got, ok)
			}
		})
	}
}

func TestObservationMetadataPreservesFalseZeroAndEmptyString(t *testing.T) {
	var typedNil *string
	got := keyValuesToStrings(lfattr.ObservationMetadata(map[string]any{
		"false":     false,
		"zero":      0,
		"empty":     "",
		"typed_nil": typedNil,
	}, nil))
	want := map[string]string{
		lfattr.ObservationMetadataKey + ".empty": "",
		lfattr.ObservationMetadataKey + ".false": "false",
		lfattr.ObservationMetadataKey + ".zero":  "0",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ObservationMetadata() = %#v, want %#v", got, want)
	}
}

func TestObservationMetadataOrderIsDeterministic(t *testing.T) {
	got := lfattr.ObservationMetadata(map[string]any{
		"z": 3,
		"a": 1,
		"m": 2,
	}, nil)
	gotKeys := make([]string, 0, len(got))
	for _, keyValue := range got {
		gotKeys = append(gotKeys, string(keyValue.Key))
	}
	wantKeys := []string{
		lfattr.ObservationMetadataKey + ".a",
		lfattr.ObservationMetadataKey + ".m",
		lfattr.ObservationMetadataKey + ".z",
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("metadata attribute order = %v, want %v", gotKeys, wantKeys)
	}
}

func TestMetadataEntryBudgetIsBoundedAndDeterministic(t *testing.T) {
	metadata := make(map[string]any, 100)
	for index := 99; index >= 0; index-- {
		metadata[fmt.Sprintf("key-%03d", index)] = index
	}

	observation := lfattr.ObservationMetadata(metadata, nil)
	if len(observation) != lfattr.MaxMetadataEntries {
		t.Fatalf("ObservationMetadata() entries = %d, want %d", len(observation), lfattr.MaxMetadataEntries)
	}
	for index, item := range observation {
		want := lfattr.ObservationMetadataKey + "." + fmt.Sprintf("key-%03d", index)
		if string(item.Key) != want {
			t.Fatalf("ObservationMetadata()[%d].Key = %q, want %q", index, item.Key, want)
		}
	}

	trace := lfattr.TraceMetadata(metadata, nil)
	if len(trace) != lfattr.MaxMetadataEntries {
		t.Fatalf("TraceMetadata() entries = %d, want %d", len(trace), lfattr.MaxMetadataEntries)
	}
	if _, retained := trace["key-031"]; !retained {
		t.Fatal("TraceMetadata() did not retain lexicographically bounded key key-031")
	}
	if _, retained := trace["key-032"]; retained {
		t.Fatal("TraceMetadata() retained key beyond deterministic budget")
	}
}

func TestEncodeAppliesMaskBeforeSerializationAndSizeCheck(t *testing.T) {
	type privateValue struct {
		Secret string `json:"secret"`
	}
	original := privateValue{Secret: strings.Repeat("s", lfattr.MaxSerializedBytes+1)}
	var received any

	got, ok := lfattr.Encode(original, func(value any) any {
		received = value
		return map[string]any{"redacted": true}
	}, "input")
	if !ok {
		t.Fatal("Encode() ok = false, want true")
	}
	if !reflect.DeepEqual(received, original) {
		t.Fatalf("mask received %#v, want original %#v", received, original)
	}
	if got != `{"redacted":true}` {
		t.Fatalf("Encode() = %q, want redacted JSON", got)
	}
}

func TestEncodeMaskRunsOnceAndMayReturnEmptyScalar(t *testing.T) {
	calls := 0
	got, ok := lfattr.Encode("secret", func(any) any {
		calls++
		return false
	}, "input")
	if !ok || got != "false" {
		t.Fatalf("Encode() = (%q, %v), want (\"false\", true)", got, ok)
	}
	if calls != 1 {
		t.Fatalf("mask calls = %d, want 1", calls)
	}
}

func TestEncodeMaskPanicIsContained(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	got, ok := lfattr.Encode("do not log this payload", func(any) any {
		panic("mask failure containing do not log this payload")
	}, "input")
	if ok || got != "" {
		t.Fatalf("Encode() = (%q, %v), want (\"\", false)", got, ok)
	}
	assertDiagnosticContains(t, diagnostics, "masker panicked; input omitted")
	assertDiagnosticsExclude(t, diagnostics, "do not log this payload")
}

func TestEncodeMaskReturningTypedNilOmitsValue(t *testing.T) {
	var replacement *string
	if got, ok := lfattr.Encode("secret", func(any) any { return replacement }, "input"); ok || got != "" {
		t.Fatalf("Encode() = (%q, %v), want (\"\", false)", got, ok)
	}
}

func TestEncodeSizeBoundary(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	atLimit := strings.Repeat("x", lfattr.MaxSerializedBytes)
	if got, ok := lfattr.Encode(atLimit, nil, "input"); !ok || got != atLimit {
		t.Fatalf("value at limit encoded as (len=%d, %v), want (len=%d, true)", len(got), ok, len(atLimit))
	}

	overLimit := strings.Repeat("sensitive", lfattr.MaxSerializedBytes/len("sensitive")+1)
	if got, ok := lfattr.Encode(overLimit, nil, "input"); ok || got != "" {
		t.Fatalf("oversized value encoded as (len=%d, %v), want (0, false)", len(got), ok)
	}
	assertDiagnosticContains(t, diagnostics, "input exceeds the internal size limit; field omitted")
	assertDiagnosticsExclude(t, diagnostics, overLimit)
}

func TestEncodePreflightRejectsNestedOversizeBeforeMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value func(*int) any
	}{
		{
			name: "non-addressable value with pointer-only marshaler",
			value: func(calls *int) any {
				return struct {
					Large pointerOnlyLargeString `json:"large"`
					Probe jsonPreflightProbe     `json:"probe"`
				}{
					Large: pointerOnlyLargeString(strings.Repeat("x", lfattr.MaxSerializedBytes+1)),
					Probe: jsonPreflightProbe{calls: calls},
				}
			},
		},
		{
			name: "map key JSON marshaler is ignored by encoding json",
			value: func(calls *int) any {
				return struct {
					Large map[jsonOnlyMapKey]string `json:"large"`
					Probe jsonPreflightProbe        `json:"probe"`
				}{
					Large: map[jsonOnlyMapKey]string{
						jsonOnlyMapKey(strings.Repeat("x", lfattr.MaxSerializedBytes+1)): "value",
					},
					Probe: jsonPreflightProbe{calls: calls},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostics := captureDiagnostics(t)
			calls := 0
			if got, ok := lfattr.Encode(test.value(&calls), nil, "input"); ok || got != "" {
				t.Fatalf("Encode() = (len %d, %v), want (0, false)", len(got), ok)
			}
			if calls != 0 {
				t.Fatalf("json.Marshal callback calls = %d, want 0 because preflight rejected first", calls)
			}
			assertDiagnosticContains(t, diagnostics, "input exceeds the internal size limit; field omitted")
		})
	}
}

func TestObservationMetadataMaskRunsOnceOnCompleteMap(t *testing.T) {
	calls := 0
	got := keyValuesToStrings(lfattr.ObservationMetadata(map[string]any{
		"secret": "value",
	}, func(value any) any {
		calls++
		metadata, ok := value.(map[string]any)
		if !ok || metadata["secret"] != "value" {
			t.Fatalf("mask received %#v, want complete metadata map", value)
		}
		return map[string]any{"secret": "[redacted]", "kept": 1}
	}))
	if calls != 1 {
		t.Fatalf("mask calls = %d, want 1", calls)
	}
	want := map[string]string{
		lfattr.ObservationMetadataKey + ".kept":   "1",
		lfattr.ObservationMetadataKey + ".secret": "[redacted]",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ObservationMetadata() = %#v, want %#v", got, want)
	}
}

func TestMetadataReservedPathSegmentsAreRejected(t *testing.T) {
	invalid := []string{
		"",
		".leading",
		"trailing.",
		"two..segments",
		"__proto__",
		"safe.__proto__",
		"constructor",
		"safe.constructor.value",
		"prototype",
		"safe.prototype.value",
	}
	for _, key := range invalid {
		if lfattr.ValidMetadataKey(key) {
			t.Errorf("ValidMetadataKey(%q) = true, want false", key)
		}
	}

	valid := []string{"safe", "safe.nested", "constructor_name", "__proto", "prototype2"}
	for _, key := range valid {
		if !lfattr.ValidMetadataKey(key) {
			t.Errorf("ValidMetadataKey(%q) = false, want true", key)
		}
	}
}

func TestMetadataFunctionsOmitReservedKeys(t *testing.T) {
	metadata := map[string]any{
		"safe":                  "kept",
		"nested.safe":           7,
		"__proto__":             "drop",
		"a.constructor.payload": "drop",
		"prototype":             "drop",
	}

	observation := keyValuesToStrings(lfattr.ObservationMetadata(metadata, nil))
	wantObservation := map[string]string{
		lfattr.ObservationMetadataKey + ".nested.safe": "7",
		lfattr.ObservationMetadataKey + ".safe":        "kept",
	}
	if !reflect.DeepEqual(observation, wantObservation) {
		t.Fatalf("ObservationMetadata() = %#v, want %#v", observation, wantObservation)
	}

	trace := lfattr.TraceMetadata(metadata, nil)
	wantTrace := map[string]string{"nested.safe": "7", "safe": "kept"}
	if !reflect.DeepEqual(trace, wantTrace) {
		t.Fatalf("TraceMetadata() = %#v, want %#v", trace, wantTrace)
	}
}

func TestNormalizeUsageInclusiveCacheReasoningAndAudioGolden(t *testing.T) {
	encoded, ok := lfattr.NormalizeUsage(
		100,
		40,
		20,
		10,
		5,
		map[string]int64{
			"input_audio_tokens":  3,
			"output_audio_tokens": 2,
		},
	)
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	const want = `{"input":67,"input_audio_tokens":3,"input_cache_creation":10,"input_cached_tokens":20,"output":33,"output_audio_tokens":2,"output_reasoning_tokens":5,"total":140}`
	if encoded != want {
		t.Fatalf("NormalizeUsage() = %s, want %s", encoded, want)
	}
}

func TestNormalizeUsageDetailBudgetIsBoundedAndDeterministic(t *testing.T) {
	diagnostics := captureDiagnostics(t)
	details := make(map[string]int64, 100)
	for index := 99; index >= 0; index-- {
		details[fmt.Sprintf("provider_%03d", index)] = int64(index)
	}
	encoded, ok := lfattr.NormalizeUsage(0, 0, 0, 0, 0, details)
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	got := decodeUsage(t, encoded)
	if len(got) != lfattr.MaxUsageDetailEntries+3 {
		t.Fatalf("usage bucket count = %d, want %d details plus input/output/total", len(got), lfattr.MaxUsageDetailEntries)
	}
	if _, retained := got["provider_063"]; !retained {
		t.Fatal("usage details did not retain deterministic boundary key provider_063")
	}
	if _, retained := got["provider_064"]; retained {
		t.Fatal("usage details retained a key beyond the deterministic budget")
	}
	assertDiagnosticContains(t, diagnostics, "usage details exceed the entry limit")
}

func TestNormalizeUsageCanonicalDetailsCannotOverrideTypedBuckets(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	encoded, ok := lfattr.NormalizeUsage(10, 5, 2, 1, 1, map[string]int64{
		"input":                   999,
		"output":                  999,
		"total":                   999,
		"input_cached_tokens":     999,
		"input_cache_creation":    999,
		"output_reasoning_tokens": 999,
	})
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	got := decodeUsage(t, encoded)
	want := map[string]int64{
		"input":                   7,
		"output":                  4,
		"total":                   15,
		"input_cached_tokens":     2,
		"input_cache_creation":    1,
		"output_reasoning_tokens": 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeUsage() = %#v, want %#v", got, want)
	}
	if countDiagnosticsContaining(diagnostics, "collides with a canonical bucket") != 6 {
		t.Fatalf("canonical-collision diagnostics = %v, want one per rejected key", diagnostics.snapshot())
	}
}

func TestNormalizeUsageOmitsNegativeCounts(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	encoded, ok := lfattr.NormalizeUsage(-10, -5, -2, -1, -3, map[string]int64{
		"input_audio_tokens": -4,
		"safe_counter":       -6,
	})
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	got := decodeUsage(t, encoded)
	want := map[string]int64{"input": 0, "output": 0, "total": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeUsage() = %#v, want %#v", got, want)
	}
	if countDiagnosticsContaining(diagnostics, "negative") < 7 {
		t.Fatalf("negative-count diagnostics = %v, want all invalid values diagnosed", diagnostics.snapshot())
	}
}

func TestNormalizeUsageOmitsOverflowedTotal(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	encoded, ok := lfattr.NormalizeUsage(math.MaxInt64, 1, 0, 0, 0, nil)
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	got := decodeUsage(t, encoded)
	if _, exists := got["total"]; exists {
		t.Fatalf("overflowed total unexpectedly emitted: %#v", got)
	}
	if got["input"] != math.MaxInt64 || got["output"] != 1 {
		t.Fatalf("base buckets = %#v, want input MaxInt64 and output 1", got)
	}
	assertDiagnosticContains(t, diagnostics, "usage total overflowed; total bucket omitted")
}

func TestNormalizeUsageClampsSubsetsToZero(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	encoded, ok := lfattr.NormalizeUsage(5, 2, 7, 3, 4, map[string]int64{
		"input_audio_tokens":  2,
		"output_audio_tokens": 3,
	})
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	got := decodeUsage(t, encoded)
	if got["input"] != 0 || got["output"] != 0 {
		t.Fatalf("clamped bases = (input=%d, output=%d), want both zero", got["input"], got["output"])
	}
	if got["total"] != 7 {
		t.Fatalf("total = %d, want inclusive total 7", got["total"])
	}
	assertDiagnosticContains(t, diagnostics, "input usage subsets exceed the inclusive input total")
	assertDiagnosticContains(t, diagnostics, "output usage subsets exceed the inclusive output total")
}

func TestNormalizeUsageSaturatesDetailSubsetAddition(t *testing.T) {
	diagnostics := captureDiagnostics(t)

	encoded, ok := lfattr.NormalizeUsage(math.MaxInt64, 0, 0, 0, 0, map[string]int64{
		"input_a": math.MaxInt64,
		"input_b": 1,
	})
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	got := decodeUsage(t, encoded)
	if got["input"] != 0 {
		t.Fatalf("input = %d, want zero after saturated subset subtraction", got["input"])
	}
	if got["input_a"] != math.MaxInt64 || got["input_b"] != 1 {
		t.Fatalf("detail buckets changed unexpectedly: %#v", got)
	}
	if countDiagnosticsContaining(diagnostics, "subsets exceed") != 0 {
		t.Fatalf("exactly saturated subsets should not exceed total: %v", diagnostics.snapshot())
	}
}

func TestNormalizeUsageNeverEmitsModernSemconvKeys(t *testing.T) {
	encoded, ok := lfattr.NormalizeUsage(12, 4, 2, 0, 1, map[string]int64{
		"input_audio_tokens": 1,
	})
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}
	if strings.Contains(encoded, "gen_ai") {
		t.Fatalf("NormalizeUsage() emitted a gen_ai key: %s", encoded)
	}
	for key := range decodeUsage(t, encoded) {
		if strings.HasPrefix(key, "gen_ai.") {
			t.Fatalf("NormalizeUsage() emitted forbidden key %q", key)
		}
	}
	if strings.HasPrefix(lfattr.ObservationUsageDetailsKey, "gen_ai.") {
		t.Fatalf("usage attribute key = %q, want Langfuse-native key", lfattr.ObservationUsageDetailsKey)
	}
}

func keyValuesToStrings(values []attribute.KeyValue) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		result[string(value.Key)] = value.Value.AsString()
	}
	return result
}

func decodeUsage(t *testing.T, encoded string) map[string]int64 {
	t.Helper()
	var result map[string]int64
	if err := json.Unmarshal([]byte(encoded), &result); err != nil {
		t.Fatalf("decode usage %q: %v", encoded, err)
	}
	return result
}

type diagnosticRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (r *diagnosticRecorder) Handle(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, err.Error())
}

func (r *diagnosticRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.messages...)
}

func captureDiagnostics(t *testing.T) *diagnosticRecorder {
	t.Helper()

	previous := otel.GetErrorHandler()
	recorder := &diagnosticRecorder{}
	otel.SetErrorHandler(recorder)
	t.Cleanup(func() {
		otel.SetErrorHandler(previous)
	})
	return recorder
}

func assertDiagnosticContains(t *testing.T, recorder *diagnosticRecorder, want string) {
	t.Helper()

	for _, message := range recorder.snapshot() {
		if strings.Contains(message, want) {
			return
		}
	}
	t.Fatalf("diagnostics = %v, want message containing %q", recorder.snapshot(), want)
}

func assertDiagnosticsExclude(t *testing.T, recorder *diagnosticRecorder, forbidden string) {
	t.Helper()

	for _, message := range recorder.snapshot() {
		if strings.Contains(message, forbidden) {
			t.Fatalf("diagnostic leaked forbidden text %q: %q", forbidden, message)
		}
	}
}

func countDiagnosticsContaining(recorder *diagnosticRecorder, text string) int {
	count := 0
	for _, message := range recorder.snapshot() {
		if strings.Contains(message, text) {
			count++
		}
	}
	return count
}

func TestUsageJSONKeysRemainSortedForGoldenStability(t *testing.T) {
	encoded, ok := lfattr.NormalizeUsage(20, 10, 2, 1, 3, map[string]int64{
		"z_counter":          1,
		"input_audio_tokens": 4,
		"a_counter":          2,
	})
	if !ok {
		t.Fatal("NormalizeUsage() ok = false, want true")
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		t.Fatalf("decode usage: %v", err)
	}
	keys := make([]string, 0, len(decoded))
	for key := range decoded {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	positions := make([]int, len(keys))
	for i, key := range keys {
		positions[i] = strings.Index(encoded, `"`+key+`"`)
	}
	if !sort.IntsAreSorted(positions) {
		t.Fatalf("usage JSON keys are not deterministic/sorted: %s", encoded)
	}
}

func TestEncodeOmitsNonFiniteFloatsWithDiagnostic(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "NaN in input map", value: map[string]any{"score": math.NaN()}},
		{name: "positive infinity in input map", value: map[string]any{"score": math.Inf(1)}},
		{name: "negative infinity in input map", value: map[string]any{"score": math.Inf(-1)}},
		{name: "float32 NaN in slice", value: []any{float32(math.NaN())}},
		{name: "bare NaN", value: math.NaN()},
		{name: "nested infinity", value: map[string]any{"outer": []any{map[string]any{"inner": math.Inf(1)}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostics := captureDiagnostics(t)
			got, ok := lfattr.Encode(test.value, nil, "observation input")
			if ok || got != "" {
				t.Fatalf("Encode() = (%q, %v), want the non-finite field omitted", got, ok)
			}
			assertDiagnosticContains(t, diagnostics, "observation input could not be serialized; field omitted")
		})
	}
}

func TestJSONMapOmitsNonFiniteModelParametersAndCostDetails(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value any
	}{
		{
			name:  "model parameters with NaN",
			field: "model parameters",
			value: map[string]any{"temperature": math.NaN(), "max_tokens": 64},
		},
		{
			name:  "model parameters with infinity",
			field: "model parameters",
			value: map[string]any{"top_p": math.Inf(1)},
		},
		{
			name:  "cost details with NaN",
			field: "cost details",
			value: map[string]float64{"input": math.NaN()},
		},
		{
			name:  "cost details with negative infinity",
			field: "cost details",
			value: map[string]float64{"input": 0.25, "output": math.Inf(-1)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostics := captureDiagnostics(t)
			value, ok := lfattr.JSONMap(test.value, test.field)
			if ok {
				encoded := value.AsString()
				if !json.Valid([]byte(encoded)) {
					t.Fatalf("JSONMap() emitted invalid JSON: %s", encoded)
				}
				t.Fatalf("JSONMap() = (%q, true), want the non-finite field omitted", encoded)
			}
			assertDiagnosticContains(t, diagnostics, test.field+" could not be serialized; field omitted")
		})
	}
}
