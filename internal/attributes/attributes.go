// Package attributes owns Langfuse's OpenTelemetry attribute contract.
package attributes

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	otelattr "go.opentelemetry.io/otel/attribute"

	"github.com/fgn/go-langfuse/internal/diagnostic"
)

var (
	errJSONSizeLimit    = errors.New("JSON value exceeds the size limit")
	errJSONNestingLimit = errors.New("JSON value exceeds the nesting limit")
	errJSONCycle        = errors.New("JSON value contains a cycle")
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	jsonNumberType      = reflect.TypeFor[json.Number]()
)

const maxJSONNestingDepth = 100

const (
	// TracerName retains Langfuse's server-recognized SDK scope prefix so the
	// ingestion pipeline does not duplicate semantic attributes into generic
	// metadata. The suffix identifies this independent community client.
	TracerName = "langfuse-sdk.go"

	TraceNameKey      = "langfuse.trace.name"
	TraceUserIDKey    = "user.id"
	TraceSessionIDKey = "session.id"
	TraceTagsKey      = "langfuse.trace.tags"
	TraceMetadataKey  = "langfuse.trace.metadata"

	ObservationTypeKey                = "langfuse.observation.type"
	ObservationMetadataKey            = "langfuse.observation.metadata"
	ObservationLevelKey               = "langfuse.observation.level"
	ObservationStatusMessageKey       = "langfuse.observation.status_message"
	ObservationInputKey               = "langfuse.observation.input"
	ObservationOutputKey              = "langfuse.observation.output"
	ObservationCompletionStartTimeKey = "langfuse.observation.completion_start_time"
	ObservationModelKey               = "langfuse.observation.model.name"
	ObservationModelParametersKey     = "langfuse.observation.model.parameters"
	ObservationUsageDetailsKey        = "langfuse.observation.usage_details"
	ObservationCostDetailsKey         = "langfuse.observation.cost_details"
	ObservationPromptNameKey          = "langfuse.observation.prompt.name"
	ObservationPromptVersionKey       = "langfuse.observation.prompt.version"

	EnvironmentKey = "langfuse.environment"
	ReleaseKey     = "langfuse.release"
	VersionKey     = "langfuse.version"
	AppRootKey     = "langfuse.internal.is_app_root"
)

const (
	// MaxSerializedBytes is deliberately internal. A single caller-supplied
	// field cannot make telemetry allocate or enqueue an unbounded attribute.
	MaxSerializedBytes           = 1 << 20
	MaxAttributeKeyBytes         = 200
	MaxDirectStringBytes         = 16 << 10
	MaxErrorMessageBytes         = 64 << 10
	MaxObservationAttributeBytes = 2 << 20
	// Reserve enough of the aggregate attribute budget for RecordError's level
	// and maximum status message even when it is called after content updates.
	MaxObservationDataAttributeBytes = MaxObservationAttributeBytes - MaxErrorMessageBytes - (1 << 10)

	// MaxMetadataEntries reserves ample room under OpenTelemetry's default
	// span attribute limit for required trace, generation, and routing fields.
	MaxMetadataEntries    = 32
	MaxUsageDetailEntries = 64
)

// Encode applies mask and produces the string representation expected by
// Langfuse. Strings stay strings; all other values are deterministic JSON.
func Encode(value any, mask func(any) any, field string) (encoded string, ok bool) {
	if isNil(value) {
		return "", false
	}
	if mask != nil {
		var panicked bool
		func() {
			defer func() {
				if recover() != nil {
					panicked = true
				}
			}()
			value = mask(value)
		}()
		if panicked {
			diagnostic.Report("masker panicked; " + field + " omitted")
			return "", false
		}
	}
	if isNil(value) {
		return "", false
	}
	if value, isString := value.(string); isString {
		if !utf8.ValidString(value) {
			diagnostic.Report(field + " is not valid UTF-8; field omitted")
			return "", false
		}
		if len(value) > MaxSerializedBytes {
			diagnostic.Report(field + " exceeds the internal size limit; field omitted")
			return "", false
		}
		return value, true
	}
	data, err, panicked := safeJSONMarshal(value)
	if panicked {
		diagnostic.Report(field + " serializer panicked; field omitted")
		return "", false
	}
	if err != nil {
		if errors.Is(err, errJSONSizeLimit) {
			diagnostic.Report(field + " exceeds the internal size limit; field omitted")
			return "", false
		}
		diagnostic.Report(field + " could not be serialized; field omitted")
		return "", false
	}
	if len(data) > MaxSerializedBytes {
		diagnostic.Report(field + " exceeds the internal size limit; field omitted")
		return "", false
	}
	return string(data), true
}

// ObservationMetadata applies the masker once to the complete metadata value,
// then emits one deterministic attribute per top-level key.

func ObservationMetadata(metadata map[string]any, mask func(any) any) []otelattr.KeyValue {
	return ObservationMetadataWithExisting(metadata, mask, nil)
}

// ObservationMetadataWithExisting gives retained keys priority over new keys
// so a full lifetime budget never makes a replacement disappear merely
// because newly supplied keys sort earlier.
func ObservationMetadataWithExisting(
	metadata map[string]any,
	mask func(any) any,
	existing map[string]struct{},
) []otelattr.KeyValue {
	if len(metadata) == 0 {
		return nil
	}
	masked, ok := applyMask(metadata, mask, "observation metadata")
	if !ok || isNil(masked) {
		return nil
	}
	values, ok := masked.(map[string]any)
	if !ok {
		diagnostic.Report("masker changed observation metadata to an unsupported type; field omitted")
		return nil
	}

	keys := boundedMetadataKeys(values, "observation", func(key string) bool {
		_, found := existing[key]
		return found
	}, len(existing))
	result := make([]otelattr.KeyValue, 0, len(keys))
	for _, key := range keys {
		value, ok := Encode(values[key], nil, "observation metadata value")
		if ok {
			result = append(result, otelattr.String(ObservationMetadataKey+"."+key, value))
		}
	}
	return result
}

// TraceMetadata normalizes request-scoped metadata to the official propagation
// representation: one string value per top-level key, each at most 200
// characters.
func TraceMetadata(metadata map[string]any, mask func(any) any) map[string]string {
	return TraceMetadataWithExisting(metadata, mask, nil)
}

// TraceMetadataWithExisting gives retained keys priority over new keys; see
// ObservationMetadataWithExisting.
func TraceMetadataWithExisting(
	metadata map[string]any,
	mask func(any) any,
	existing map[string]string,
) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	masked, ok := applyMask(metadata, mask, "trace metadata")
	if !ok || isNil(masked) {
		return nil
	}
	values, ok := masked.(map[string]any)
	if !ok {
		diagnostic.Report("masker changed trace metadata to an unsupported type; field omitted")
		return nil
	}
	keys := boundedMetadataKeys(values, "trace", func(key string) bool {
		_, found := existing[key]
		return found
	}, len(existing))
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		raw := values[key]
		value, ok := Encode(raw, nil, "trace metadata value")
		if !ok {
			continue
		}
		if utf8.RuneCountInString(value) > 200 {
			diagnostic.Report("trace metadata value exceeds 200 characters; entry omitted")
			continue
		}
		result[key] = value
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// boundedMetadataKeys returns the lexicographically smallest valid keys using
// bounded auxiliary memory. This keeps truncation deterministic even for a
// caller-supplied map with an extreme number of entries.
func boundedMetadataKeys[V any](
	values map[string]V,
	field string,
	isExisting func(string) bool,
	existingCount int,
) []string {
	if existingCount < 0 {
		existingCount = 0
	} else if existingCount > MaxMetadataEntries {
		existingCount = MaxMetadataEntries
	}
	replacements := make([]string, 0, min(len(values), existingCount))
	newCapacity := MaxMetadataEntries - existingCount
	newKeys := make([]string, 0, min(len(values), newCapacity))
	invalid := false
	truncated := false
	for key := range values {
		if !ValidMetadataKey(key) {
			invalid = true
			continue
		}
		if isExisting != nil && isExisting(key) {
			index := sort.SearchStrings(replacements, key)
			replacements = append(replacements, "")
			copy(replacements[index+1:], replacements[index:])
			replacements[index] = key
			continue
		}
		index := sort.SearchStrings(newKeys, key)
		if len(newKeys) < newCapacity {
			newKeys = append(newKeys, "")
			copy(newKeys[index+1:], newKeys[index:])
			newKeys[index] = key
			continue
		}
		truncated = true
		if index >= newCapacity || newCapacity == 0 {
			continue
		}
		copy(newKeys[index+1:], newKeys[index:newCapacity-1])
		newKeys[index] = key
	}
	if invalid {
		diagnostic.Report(field + " metadata contains an invalid or reserved key; entry omitted")
	}
	if truncated {
		if existingCount != 0 {
			diagnostic.Report(field + " metadata exceeds the lifetime entry limit; new entries omitted")
		} else {
			diagnostic.Report(field + " metadata exceeds the top-level entry limit; remaining entries omitted")
		}
	}
	return append(replacements, newKeys...)
}

// JSONMap serializes a structured attribute without masking.
func JSONMap(value any, field string) (otelattr.Value, bool) {
	encoded, ok := Encode(value, nil, field)
	if !ok {
		return otelattr.Value{}, false
	}
	return otelattr.StringValue(encoded), true
}

// ValidMetadataKey blocks prototype-pollution path segments used by the
// JavaScript ingestion stack.
func ValidMetadataKey(key string) bool {
	if key == "" || !utf8.ValidString(key) || len(key) > MaxAttributeKeyBytes {
		return false
	}
	for _, segment := range strings.Split(key, ".") {
		switch segment {
		case "", "__proto__", "constructor", "prototype":
			return false
		}
	}
	return true
}

// NormalizeUsage converts inclusive OTel-style totals to Langfuse's exclusive
// buckets. Detail buckets must be disjoint subsets of their base direction.
func NormalizeUsage(
	inputTokens int64,
	outputTokens int64,
	cacheReadInputTokens int64,
	cacheCreationInputTokens int64,
	reasoningOutputTokens int64,
	details map[string]int64,
) (string, bool) {
	canonical := map[string]struct{}{
		"input": {}, "output": {}, "total": {},
		"input_cached_tokens": {}, "input_cache_creation": {},
		"output_reasoning_tokens": {},
	}
	counts := map[string]int64{}
	validBase := func(value int64, name string) int64 {
		if value < 0 {
			diagnostic.Report("usage contains a negative " + name + "; count omitted")
			return 0
		}
		return value
	}
	inputTokens = validBase(inputTokens, "input count")
	outputTokens = validBase(outputTokens, "output count")
	cacheReadInputTokens = validBase(cacheReadInputTokens, "cache-read count")
	cacheCreationInputTokens = validBase(cacheCreationInputTokens, "cache-creation count")
	reasoningOutputTokens = validBase(reasoningOutputTokens, "reasoning count")

	var extraInput, extraOutput int64
	keys := boundedUsageKeys(details)
	for _, key := range keys {
		value := details[key]
		if value < 0 {
			diagnostic.Report("usage detail contains a negative count; entry omitted")
			continue
		}
		if _, reserved := canonical[key]; reserved {
			diagnostic.Report("usage detail collides with a canonical bucket; entry omitted")
			continue
		}
		counts[key] = value
		if strings.HasPrefix(key, "input_") {
			extraInput = saturatingAdd(extraInput, value)
		}
		if strings.HasPrefix(key, "output_") {
			extraOutput = saturatingAdd(extraOutput, value)
		}
	}

	inputSubsets := saturatingAdd(saturatingAdd(cacheReadInputTokens, cacheCreationInputTokens), extraInput)
	outputSubsets := saturatingAdd(reasoningOutputTokens, extraOutput)
	if inputSubsets > inputTokens {
		diagnostic.Report("input usage subsets exceed the inclusive input total; base input clamped to zero")
	}
	if outputSubsets > outputTokens {
		diagnostic.Report("output usage subsets exceed the inclusive output total; base output clamped to zero")
	}
	counts["input"] = max64(saturatingSub(inputTokens, inputSubsets), 0)
	counts["output"] = max64(saturatingSub(outputTokens, outputSubsets), 0)
	if cacheReadInputTokens != 0 {
		counts["input_cached_tokens"] = cacheReadInputTokens
	}
	if cacheCreationInputTokens != 0 {
		counts["input_cache_creation"] = cacheCreationInputTokens
	}
	if reasoningOutputTokens != 0 {
		counts["output_reasoning_tokens"] = reasoningOutputTokens
	}
	if total, overflow := checkedAdd(inputTokens, outputTokens); !overflow {
		counts["total"] = total
	} else {
		diagnostic.Report("usage total overflowed; total bucket omitted")
	}
	encoded, err, panicked := safeJSONMarshal(counts)
	if panicked || err != nil || len(encoded) > MaxSerializedBytes {
		diagnostic.Report("usage could not be serialized; usage omitted")
		return "", false
	}
	return string(encoded), true
}

func boundedUsageKeys(values map[string]int64) []string {
	keys := make([]string, 0, min(len(values), MaxUsageDetailEntries))
	invalid := false
	truncated := false
	for key := range values {
		if key == "" || !utf8.ValidString(key) || len(key) > MaxAttributeKeyBytes {
			invalid = true
			continue
		}
		index := sort.SearchStrings(keys, key)
		if len(keys) < MaxUsageDetailEntries {
			keys = append(keys, "")
			copy(keys[index+1:], keys[index:])
			keys[index] = key
			continue
		}
		truncated = true
		if index >= MaxUsageDetailEntries {
			continue
		}
		copy(keys[index+1:], keys[index:MaxUsageDetailEntries-1])
		keys[index] = key
	}
	if invalid {
		diagnostic.Report("usage detail contains an invalid key; entry omitted")
	}
	if truncated {
		diagnostic.Report("usage details exceed the entry limit; remaining entries omitted")
	}
	return keys
}

func safeJSONMarshal(value any) (data []byte, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			data = nil
			err = nil
			panicked = true
		}
	}()
	if _, err := estimateJSONSize(
		reflect.ValueOf(value), MaxSerializedBytes, 0, make(map[jsonVisit]struct{}),
	); err != nil {
		return nil, err, false
	}
	data, err = json.Marshal(value)
	return data, err, false
}

type jsonVisit struct {
	typeOf  reflect.Type
	pointer uintptr
}

// estimateJSONSize walks ordinary reflection-based JSON values before
// encoding so a large nested string/map/slice is rejected before json.Marshal
// allocates a second copy. User-defined MarshalJSON/MarshalText behavior is a
// documented trusted-callback boundary; its returned bytes are still checked
// after the call, but work performed inside the callback cannot be bounded by
// the SDK.
func estimateJSONSize(
	value reflect.Value,
	limit int,
	depth int,
	active map[jsonVisit]struct{},
) (int, error) {
	if depth > maxJSONNestingDepth {
		return 0, errJSONNestingLimit
	}
	if !value.IsValid() {
		return 4, nil // null
	}
	if hasCustomJSON(value) {
		return 0, nil
	}
	if value.Type() == jsonNumberType {
		return boundedSize(len(value.String()), limit)
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return 4, nil
		}
		return estimateJSONSize(value.Elem(), limit, depth+1, active)
	case reflect.Pointer:
		if value.IsNil() {
			return 4, nil
		}
		return estimateJSONReference(value, limit, depth, active, func() (int, error) {
			return estimateJSONSize(value.Elem(), limit, depth+1, active)
		})
	case reflect.Bool:
		return 5, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return 20, nil
	case reflect.Float32:
		if number := value.Float(); math.IsNaN(number) || math.IsInf(number, 0) {
			return 0, errors.New("unsupported JSON float")
		}
		return 15, nil
	case reflect.Float64:
		if number := value.Float(); math.IsNaN(number) || math.IsInf(number, 0) {
			return 0, errors.New("unsupported JSON float")
		}
		return 24, nil
	case reflect.String:
		if value.Len() > limit {
			return 0, errJSONSizeLimit
		}
		return boundedSize(jsonStringSize(value.String()), limit)
	case reflect.Slice:
		if value.IsNil() {
			return 4, nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			if value.Len() > limit {
				return 0, errJSONSizeLimit
			}
			return boundedSize(2+base64.StdEncoding.EncodedLen(value.Len()), limit)
		}
		return estimateJSONReference(value, limit, depth, active, func() (int, error) {
			return estimateJSONSequence(value, limit, depth, active)
		})
	case reflect.Array:
		return estimateJSONSequence(value, limit, depth, active)
	case reflect.Map:
		if value.IsNil() {
			return 4, nil
		}
		return estimateJSONReference(value, limit, depth, active, func() (int, error) {
			total := 2
			iterator := value.MapRange()
			for index := 0; iterator.Next(); index++ {
				if index != 0 {
					total++
				}
				keySize, err := estimateJSONMapKey(iterator.Key(), limit-total)
				if err != nil {
					return 0, err
				}
				total, err = addJSONSize(total, keySize+1, limit) // colon
				if err != nil {
					return 0, err
				}
				itemSize, err := estimateJSONSize(iterator.Value(), limit-total, depth+1, active)
				if err != nil {
					return 0, err
				}
				total, err = addJSONSize(total, itemSize, limit)
				if err != nil {
					return 0, err
				}
			}
			return total, nil
		})
	case reflect.Struct:
		total := 2
		fieldCount := 0
		typeOf := value.Type()
		for index := 0; index < value.NumField(); index++ {
			field := typeOf.Field(index)
			fieldValue := value.Field(index)
			if field.PkgPath != "" && !(field.Anonymous && indirectKind(field.Type) == reflect.Struct) {
				continue
			}
			tag := field.Tag.Get("json")
			parts := strings.Split(tag, ",")
			if tag == "-" {
				continue
			}
			if fieldCount != 0 {
				total++
			}
			fieldCount++
			nameSize := jsonStringSize(field.Name)
			if len(parts) != 0 && parts[0] != "" {
				nameSize = max(nameSize, jsonStringSize(parts[0]))
			}
			var err error
			total, err = addJSONSize(total, nameSize+1, limit)
			if err != nil {
				return 0, err
			}
			itemSize, err := estimateJSONSize(fieldValue, limit-total, depth+1, active)
			if err != nil {
				return 0, err
			}
			if hasJSONTagOption(parts, "string") {
				itemSize = 2 + 2*itemSize
			}
			total, err = addJSONSize(total, itemSize, limit)
			if err != nil {
				return 0, err
			}
		}
		return total, nil
	default:
		return 0, errors.New("unsupported JSON kind")
	}
}

func estimateJSONReference(
	value reflect.Value,
	limit int,
	depth int,
	active map[jsonVisit]struct{},
	estimate func() (int, error),
) (int, error) {
	if active == nil {
		active = make(map[jsonVisit]struct{})
	}
	visit := jsonVisit{typeOf: value.Type(), pointer: value.Pointer()}
	if _, found := active[visit]; found {
		return 0, errJSONCycle
	}
	active[visit] = struct{}{}
	defer delete(active, visit)
	return estimate()
}

func estimateJSONSequence(
	value reflect.Value,
	limit int,
	depth int,
	active map[jsonVisit]struct{},
) (int, error) {
	total := 2
	for index := 0; index < value.Len(); index++ {
		if index != 0 {
			total++
		}
		itemSize, err := estimateJSONSize(value.Index(index), limit-total, depth+1, active)
		if err != nil {
			return 0, err
		}
		total, err = addJSONSize(total, itemSize, limit)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func estimateJSONMapKey(value reflect.Value, limit int) (int, error) {
	// encoding/json ignores json.Marshaler on map keys. It consults only
	// TextMarshaler, and map keys are not addressable.
	if value.Type().Implements(textMarshalerType) {
		return 0, nil
	}
	switch value.Kind() {
	case reflect.String:
		if value.Len() > limit {
			return 0, errJSONSizeLimit
		}
		return boundedSize(jsonStringSize(value.String()), limit)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return boundedSize(22, limit)
	default:
		return 0, errors.New("unsupported JSON map key")
	}
}

func hasCustomJSON(value reflect.Value) bool {
	typeOf := value.Type()
	if typeOf.Implements(jsonMarshalerType) || typeOf.Implements(textMarshalerType) {
		return true
	}
	return value.CanAddr() && typeOf.Kind() != reflect.Pointer &&
		(reflect.PointerTo(typeOf).Implements(jsonMarshalerType) ||
			reflect.PointerTo(typeOf).Implements(textMarshalerType))
}

func hasJSONTagOption(parts []string, option string) bool {
	for _, item := range parts[1:] {
		if item == option {
			return true
		}
	}
	return false
}

func indirectKind(typeOf reflect.Type) reflect.Kind {
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf.Kind()
}

func boundedSize(size, limit int) (int, error) {
	if size < 0 || size > limit {
		return 0, errJSONSizeLimit
	}
	return size, nil
}

func addJSONSize(total, addition, limit int) (int, error) {
	if addition < 0 || total > limit-addition {
		return 0, errJSONSizeLimit
	}
	return total + addition, nil
}

func jsonStringSize(value string) int {
	size := 2 // quotes
	for index := 0; index < len(value); {
		character := value[index]
		if character < utf8.RuneSelf {
			index++
			switch character {
			case '\\', '"', '\b', '\f', '\n', '\r', '\t':
				size += 2
			case '<', '>', '&':
				size += 6
			default:
				if character < 0x20 {
					size += 6
				} else {
					size++
				}
			}
			continue
		}
		runeValue, width := utf8.DecodeRuneInString(value[index:])
		index += width
		if runeValue == utf8.RuneError && width == 1 || runeValue == '\u2028' || runeValue == '\u2029' {
			size += 6
		} else {
			size += width
		}
	}
	return size
}

func applyMask(value any, mask func(any) any, field string) (result any, ok bool) {
	if mask == nil {
		return value, true
	}
	defer func() {
		if recover() != nil {
			diagnostic.Report("masker panicked; " + field + " omitted")
			result = nil
			ok = false
		}
	}()
	return mask(value), true
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func checkedAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, true
	}
	return a + b, false
}

func saturatingAdd(a, b int64) int64 {
	if sum, overflow := checkedAdd(a, b); !overflow {
		return sum
	}
	return math.MaxInt64
}

func saturatingSub(a, b int64) int64 {
	if b == math.MinInt64 {
		return math.MaxInt64
	}
	if result, overflow := checkedAdd(a, -b); !overflow {
		return result
	}
	return math.MinInt64
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
