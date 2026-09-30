package attributes

import (
	"bytes"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// Langfuse v4 experiment attributes. The server reads each one from every
// span of an experiment item trace; nothing is inferred from a parent.
const (
	// ExperimentNamespace prefixes every experiment attribute.
	ExperimentNamespace = "langfuse.experiment."

	ExperimentIDKey                    = "langfuse.experiment.id"
	ExperimentNameKey                  = "langfuse.experiment.name"
	ExperimentDescriptionKey           = "langfuse.experiment.description"
	ExperimentDatasetIDKey             = "langfuse.experiment.dataset.id"
	ExperimentMetadataKey              = "langfuse.experiment.metadata"
	ExperimentItemIDKey                = "langfuse.experiment.item.id"
	ExperimentItemVersionKey           = "langfuse.experiment.item.version"
	ExperimentItemMetadataKey          = "langfuse.experiment.item.metadata"
	ExperimentItemRootObservationIDKey = "langfuse.experiment.item.root_observation_id"
	ExperimentItemExpectedOutputKey    = "langfuse.experiment.item.expected_output"

	// ExperimentEnvironment is the environment both official SDKs force on
	// every span of an experiment item trace.
	ExperimentEnvironment = "sdk-experiment"
)

var (
	// ErrContentInvalid reports content that cannot be encoded: invalid
	// JSON or UTF-8, a cycle, an unsupported type, or a serializer panic.
	ErrContentInvalid = errors.New("content could not be serialized")
	// ErrContentTooLarge reports content over its limit.
	ErrContentTooLarge = errors.New("content exceeds its size limit")
	// ErrMetadataShape reports metadata that is not a JSON object, has an
	// invalid or reserved key path, or flattens two keys to one path.
	ErrMetadataShape = errors.New("metadata is not a JSON object with unique valid key paths")
)

// EncodeContent produces the string-preserving wire form used for content
// attributes, reporting failures as errors instead of omitting the field.
// A nil value (including a typed nil, an empty json.RawMessage, or JSON
// null) is absent. Strings are sent verbatim. A json.RawMessage holding a
// JSON string is sent as the decoded string, matching the official SDKs,
// which decode stored JSON before serializing it; any other json.RawMessage
// is sent as compact JSON with its exact number tokens. Other values are
// deterministic JSON.
func EncodeContent(value any, limit int) (encoded string, present bool, err error) {
	if isNil(value) {
		return "", false, nil
	}
	switch value := value.(type) {
	case string:
		if !utf8.ValidString(value) {
			return "", false, ErrContentInvalid
		}
		if len(value) > limit {
			return "", false, ErrContentTooLarge
		}
		return value, true, nil
	case json.RawMessage:
		return encodeRawContent(value, limit)
	}
	data, err, panicked := MarshalJSON(value, limit)
	if panicked {
		return "", false, ErrContentInvalid
	}
	if errors.Is(err, errJSONSizeLimit) {
		return "", false, ErrContentTooLarge
	}
	if err != nil {
		return "", false, ErrContentInvalid
	}
	if len(data) > limit {
		return "", false, ErrContentTooLarge
	}
	if string(data) == "null" {
		return "", false, nil
	}
	return string(data), true, nil
}

func encodeRawContent(raw json.RawMessage, limit int) (string, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", false, nil
	}
	if len(trimmed) > 4*limit {
		// Compaction and string decoding never grow a value by more than
		// this, so reject before scanning an absurd input.
		return "", false, ErrContentTooLarge
	}
	if !utf8.Valid(trimmed) || !json.Valid(trimmed) {
		return "", false, ErrContentInvalid
	}
	if trimmed[0] == '"' {
		var decoded string
		if err := json.Unmarshal(trimmed, &decoded); err != nil {
			return "", false, ErrContentInvalid
		}
		if len(decoded) > limit {
			return "", false, ErrContentTooLarge
		}
		return decoded, true, nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, trimmed); err != nil {
		return "", false, ErrContentInvalid
	}
	if compact.String() == "null" {
		return "", false, nil
	}
	if compact.Len() > limit {
		return "", false, ErrContentTooLarge
	}
	return compact.String(), true, nil
}

// MaxMetadataDepth bounds how deeply experiment metadata objects nest.
const MaxMetadataDepth = 32

// EncodeMetadataObject serializes experiment metadata that Mask has already
// processed as one JSON object attribute value. The server parses that
// object and flattens nested objects into dotted paths. To make the stored
// values match dotted attributes from the official Python SDK, every leaf is
// normalized to a string first: strings stay as they are, numbers keep their
// exact JSON digits, booleans become "true" or "false", arrays become compact
// JSON, and null leaves are dropped. Every key path must be valid under
// ValidMetadataKey, objects may nest at most MaxMetadataDepth levels, and no
// two keys may flatten to the same path (for example "a.b" beside
// {"a": {"b": ...}}). Validation runs on the serialized JSON, so custom
// marshalers and json.RawMessage values are covered. Encoding is
// deterministic. An object without leaves is absent.
func EncodeMetadataObject(metadata map[string]any, limit int) (string, bool, error) {
	if len(metadata) == 0 {
		return "", false, nil
	}
	data, err, panicked := MarshalJSON(metadata, limit)
	if panicked {
		return "", false, ErrContentInvalid
	}
	if errors.Is(err, errJSONSizeLimit) || err == nil && len(data) > limit {
		return "", false, ErrContentTooLarge
	}
	if err != nil {
		return "", false, ErrContentInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil || root == nil {
		return "", false, ErrMetadataShape
	}
	normalized, leaves, err := normalizeMetadata(root, "", 1, make(map[string]struct{}))
	if err != nil {
		return "", false, err
	}
	if leaves == 0 {
		return "", false, nil
	}
	encoded, err := marshalCompact(normalized)
	if err != nil {
		return "", false, ErrContentInvalid
	}
	if len(encoded) > limit {
		return "", false, ErrContentTooLarge
	}
	return encoded, true, nil
}

// normalizeMetadata walks a decoded object the way the server flattens it:
// objects recurse, every other value is a leaf.
func normalizeMetadata(
	object map[string]any,
	prefix string,
	depth int,
	seen map[string]struct{},
) (map[string]any, int, error) {
	if depth > MaxMetadataDepth {
		return nil, 0, ErrMetadataShape
	}
	result := make(map[string]any, len(object))
	leaves := 0
	for key, value := range object {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if !ValidMetadataKey(path) {
			return nil, 0, ErrMetadataShape
		}
		var leaf string
		switch value := value.(type) {
		case nil:
			continue
		case map[string]any:
			nested, count, err := normalizeMetadata(value, path, depth+1, seen)
			if err != nil {
				return nil, 0, err
			}
			if count != 0 {
				result[key] = nested
				leaves += count
			}
			continue
		case string:
			leaf = value
		case json.Number:
			leaf = value.String()
		case bool:
			leaf = "false"
			if value {
				leaf = "true"
			}
		default:
			encoded, err := marshalCompact(value)
			if err != nil {
				return nil, 0, ErrContentInvalid
			}
			leaf = encoded
		}
		if _, duplicate := seen[path]; duplicate {
			return nil, 0, ErrMetadataShape
		}
		seen[path] = struct{}{}
		result[key] = leaf
		leaves++
	}
	return result, leaves, nil
}

// marshalCompact encodes decoded JSON values without HTML escaping, as the
// official SDKs do, keeping json.Number digits exact.
func marshalCompact(value any) (string, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return string(bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))), nil
}
