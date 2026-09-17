package api

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Optional is a request field with explicit value/null semantics. A nil pointer
// with json:",omitempty" omits the key; Set includes even false, zero, or an
// empty collection, while Null includes JSON null. Server endpoint semantics
// determine whether null is meaningful; it never means "omit" in this package.
type Optional[T any] struct {
	value T
	null  bool
}

// Set includes a request value, including its zero value.
func Set[T any](value T) *Optional[T] { return &Optional[T]{value: value} }

// Null includes an explicit JSON null in a request.
func Null[T any]() *Optional[T] { return &Optional[T]{null: true} }

// Value returns the value and whether the request field is non-null and set.
func (o *Optional[T]) Value() (T, bool) {
	if o == nil || o.null {
		var zero T
		return zero, false
	}
	return o.value, true
}

// IsNull reports an explicitly requested JSON null.
func (o *Optional[T]) IsNull() bool { return o != nil && o.null }

// MarshalJSON encodes a present value or an explicit null.
func (o Optional[T]) MarshalJSON() ([]byte, error) {
	if o.null {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

// Field preserves absent, null, and present response values. Wire structs use
// Go 1.25's omitzero support so re-encoding a sparse response does not invent
// absent fields. RawMessage fields preserve arbitrary JSON and number precision.
type Field[T any] struct {
	Present bool
	Null    bool
	Value   T
}

// IsZero reports an absent field for encoding/json's omitzero option.
func (f Field[T]) IsZero() bool { return !f.Present }

// UnmarshalJSON records presence and null independently of the value's zero value.
func (f *Field[T]) UnmarshalJSON(data []byte) error {
	*f = Field[T]{Present: true}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		f.Null = true
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(&f.Value)
}

// MarshalJSON encodes null or the present value. Absent fields are omitted by
// their enclosing wire struct's omitzero tag, not by this method alone.
func (f Field[T]) MarshalJSON() ([]byte, error) {
	if !f.Present || f.Null {
		return []byte("null"), nil
	}
	return json.Marshal(f.Value)
}

// ScoreValue is the number, boolean, or string value returned by scores v3.
// Exactly one member is non-nil; JSON numbers are never decoded through float64.
// Boolean read values are not the numeric 0/1 representation used for writes.
type ScoreValue struct {
	Number  *json.Number
	Boolean *bool
	Text    *string
}

// UnmarshalJSON decodes the scalar score union and rejects objects and arrays.
func (v *ScoreValue) UnmarshalJSON(data []byte) error {
	*v = ScoreValue{}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return errors.New("langfuse api: empty score value")
	}
	switch data[0] {
	case '"':
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		v.Text = &value
	case 't', 'f':
		var value bool
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		v.Boolean = &value
	default:
		if data[0] != '-' && (data[0] < '0' || data[0] > '9') {
			return errors.New("langfuse api: invalid score value type")
		}
		var value json.Number
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		v.Number = &value
	}
	return nil
}

// MarshalJSON preserves the scalar representation and rejects invalid unions.
func (v ScoreValue) MarshalJSON() ([]byte, error) {
	count := 0
	if v.Number != nil {
		count++
	}
	if v.Boolean != nil {
		count++
	}
	if v.Text != nil {
		count++
	}
	if count != 1 {
		return nil, errors.New("langfuse api: score value needs exactly one variant")
	}
	if v.Number != nil {
		return json.Marshal(v.Number)
	}
	if v.Boolean != nil {
		return json.Marshal(v.Boolean)
	}
	return json.Marshal(v.Text)
}

func validJSON(value json.RawMessage) bool { return len(value) == 0 || json.Valid(value) }

func validateNumber(value json.Number) bool {
	if value == "" {
		return false
	}
	data, err := json.Marshal(value)
	return err == nil && len(data) > 0 && data[0] != '"'
}
