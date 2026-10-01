package attributes

import (
	"bytes"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// Langfuse v4 experiment attributes. The server reads them from every span
// of an item trace.
const (
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

	// ExperimentEnvironment is the environment of every experiment item span.
	ExperimentEnvironment = "sdk-experiment"
)

var (
	// ErrContentInvalid reports content that cannot be encoded.
	ErrContentInvalid = errors.New("content could not be serialized")
	// ErrContentTooLarge reports content over its limit.
	ErrContentTooLarge = errors.New("content exceeds its size limit")
)

// EncodeContent encodes value in the string-preserving content form. Nil
// and JSON null are absent. A json.RawMessage JSON string is sent as the
// decoded string, as the official SDKs decode stored JSON; other raw JSON
// keeps its exact number tokens.
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
