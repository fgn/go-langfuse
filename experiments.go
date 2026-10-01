package langfuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

// ErrInvalidExperiment reports that [Client.StartExperimentItem] rejected its input.
var ErrInvalidExperiment = errors.New("langfuse: invalid experiment")

// ErrExperimentItemNotExported reports that a borrowed provider's sampler
// dropped an experiment item root.
var ErrExperimentItemNotExported = errors.New("langfuse: experiment item root is not exported")

var errExperimentAfterShutdown = errors.New("langfuse: experiment item started after client shutdown")

const (
	maxExperimentIdentifierBytes = 255
	maxExperimentDescription     = lfattr.MaxDirectStringBytes
	maxExperimentMetadataBytes   = 16 << 10
	maxExperimentExpectedOutput  = 256 << 10
)

// Experiment identifies one experiment run.
type Experiment struct {
	// ID is shared by every item of the run. Required: at most 255 bytes of
	// valid UTF-8 without control characters.
	ID string
	// Name is the run name. Required; same rules as ID.
	Name string
	// Description is optional, at most 16 KiB, and not masked.
	Description string
	// DatasetID links the run to a Langfuse dataset; empty for local data.
	DatasetID string
	// Metadata is masked as MaskExperimentMetadata and exported as a JSON
	// object of at most 16 KiB on every span of the item trace.
	Metadata map[string]any
}

// ExperimentItem identifies one item of an experiment run.
type ExperimentItem struct {
	// ID identifies the item across runs, such as [DatasetItem.ID].
	// Required; same rules as Experiment.ID.
	ID string
	// Version is the DatasetItemQuery.AsOf the item was read at; zero omits it.
	Version time.Time
	// ExpectedOutput is masked as MaskExperimentItemExpectedOutput, at most
	// 256 KiB, and omitted when content capture is off. Strings are sent
	// verbatim, a json.RawMessage JSON string as that string, and other
	// values as JSON.
	ExpectedOutput any
	// Metadata is masked as MaskExperimentItemMetadata, with the rules of
	// Experiment.Metadata.
	Metadata map[string]any
}

type experimentContextKey struct{ client *Client }

type experimentState struct {
	traceID    oteltrace.TraceID
	attributes []attribute.KeyValue
}

// StartExperimentItem starts the root observation of one experiment item in
// a new trace and returns a context that carries the item's identity.
//
// End the observation when the task finishes; Langfuse measures item latency
// on it. Spans started from the returned context in the item trace on this
// client's provider, including evaluators after End, get the item identity
// and langfuse.environment "sdk-experiment" at start. Isolated mode always
// samples the item trace; a borrowed provider's sampler that drops the root
// yields [ErrExperimentItemNotExported]. On error the observation is a no-op
// and the context carries neither the item identity nor an ambient span. A
// nil or disabled client only validates the input.
func (c *Client) StartExperimentItem(
	ctx context.Context,
	experiment Experiment,
	item ExperimentItem,
	name string,
	values ObservationAttributes,
) (context.Context, *Observation, error) {
	if ctx == nil {
		return nil, &Observation{}, errors.New("langfuse: experiment context is nil")
	}
	if err := validateExperiment(experiment, item); err != nil {
		return c.failedExperimentContext(ctx), &Observation{}, err
	}
	if c == nil || c.isDisabled() {
		return ctx, &Observation{}, nil
	}
	failed := c.failedExperimentContext(ctx)
	if c.stopped.Load() {
		return failed, &Observation{}, errExperimentAfterShutdown
	}
	shared, rootOnly, err := c.experimentAttributeSets(ctx, experiment, item)
	if err != nil {
		return failed, &Observation{}, err
	}
	itemCtx, root := c.startObservation(ctx, name, TypeSpan, values, true)
	if root.span == nil {
		return failed, &Observation{}, errExperimentAfterShutdown
	}
	spanContext := root.span.SpanContext()
	if !root.span.IsRecording() || !spanContext.IsSampled() {
		root.span.End()
		return failed, &Observation{}, ErrExperimentItemNotExported
	}
	shared = append(shared, attribute.String(lfattr.ExperimentItemRootObservationIDKey, spanContext.SpanID().String()))
	root.span.SetAttributes(slices.Concat(shared, rootOnly)...)
	state := &experimentState{traceID: spanContext.TraceID(), attributes: shared}
	return context.WithValue(itemCtx, experimentContextKey{client: c}, state), root, nil
}

// Dropping the ambient span too makes a rejected task a new, unlinked trace.
func (c *Client) failedExperimentContext(ctx context.Context) context.Context {
	return oteltrace.ContextWithSpanContext(c.withoutExperiment(ctx), oteltrace.SpanContext{})
}

func (c *Client) withoutExperiment(ctx context.Context) context.Context {
	if c == nil {
		return ctx
	}
	if state, _ := ctx.Value(experimentContextKey{client: c}).(*experimentState); state == nil {
		return ctx
	}
	return context.WithValue(ctx, experimentContextKey{client: c}, (*experimentState)(nil))
}

func (c *Client) experimentAttributes(ctx context.Context, traceID oteltrace.TraceID) []attribute.KeyValue {
	if c == nil || ctx == nil || !traceID.IsValid() {
		return nil
	}
	state, _ := ctx.Value(experimentContextKey{client: c}).(*experimentState)
	if state == nil || state.traceID != traceID {
		return nil
	}
	return state.attributes
}

func (c *Client) experimentAttributeSets(
	ctx context.Context,
	experiment Experiment,
	item ExperimentItem,
) (shared, rootOnly []attribute.KeyValue, err error) {
	shared = []attribute.KeyValue{
		attribute.String(lfattr.EnvironmentKey, lfattr.ExperimentEnvironment),
		attribute.String(lfattr.ExperimentIDKey, experiment.ID),
		attribute.String(lfattr.ExperimentNameKey, experiment.Name),
		attribute.String(lfattr.ExperimentItemIDKey, item.ID),
	}
	if experiment.DatasetID != "" {
		shared = append(shared, attribute.String(lfattr.ExperimentDatasetIDKey, experiment.DatasetID))
	}
	if !item.Version.IsZero() {
		shared = append(shared, attribute.String(lfattr.ExperimentItemVersionKey, formatDatasetInstant(item.Version)))
	}
	for _, metadata := range []struct {
		field MaskField
		key   string
		value map[string]any
	}{
		{MaskExperimentMetadata, lfattr.ExperimentMetadataKey, experiment.Metadata},
		{MaskExperimentItemMetadata, lfattr.ExperimentItemMetadataKey, item.Metadata},
	} {
		encoded, present, err := c.experimentMetadata(metadata.field, metadata.value)
		if err != nil {
			return nil, nil, err
		}
		if present {
			shared = append(shared, attribute.String(metadata.key, encoded))
		}
	}
	if experiment.Description != "" {
		rootOnly = append(rootOnly, attribute.String(lfattr.ExperimentDescriptionKey, experiment.Description))
	}
	if !lfattr.IsNil(item.ExpectedOutput) && c.contentCaptureEnabled(ctx) {
		expected, err := c.maskExperimentValue(MaskExperimentItemExpectedOutput, item.ExpectedOutput)
		if err != nil {
			return nil, nil, err
		}
		encoded, present, err := encodeExperimentValue(MaskExperimentItemExpectedOutput, expected,
			maxExperimentExpectedOutput)
		if err != nil {
			return nil, nil, err
		}
		if present {
			rootOnly = append(rootOnly, attribute.String(lfattr.ExperimentItemExpectedOutputKey, encoded))
		}
	}
	return shared, rootOnly, nil
}

func (c *Client) experimentMetadata(field MaskField, metadata map[string]any) (string, bool, error) {
	if len(metadata) == 0 {
		return "", false, nil
	}
	masked, err := c.maskExperimentValue(field, metadata)
	if err != nil || lfattr.IsNil(masked) {
		return "", false, err
	}
	if _, ok := masked.(map[string]any); !ok {
		return "", false, fmt.Errorf("%w: masker changed %s to an unsupported type", ErrInvalidExperiment, field)
	}
	return encodeExperimentValue(field, masked, maxExperimentMetadataBytes)
}

func (c *Client) maskExperimentValue(field MaskField, value any) (masked any, err error) {
	if c.mask == nil {
		return value, nil
	}
	defer func() {
		if recover() != nil {
			masked = nil
			err = fmt.Errorf("%w: masker panicked on %s", ErrInvalidExperiment, field)
		}
	}()
	return c.mask(string(field), value), nil
}

func encodeExperimentValue(field MaskField, value any, limit int) (string, bool, error) {
	encoded, present, err := lfattr.EncodeContent(value, limit)
	if err != nil {
		return "", false, fmt.Errorf("%w: %s %w", ErrInvalidExperiment, field, err)
	}
	return encoded, present, nil
}

func validateExperiment(experiment Experiment, item ExperimentItem) error {
	for _, field := range []struct {
		name     string
		value    string
		optional bool
	}{
		{"experiment ID", experiment.ID, false},
		{"experiment name", experiment.Name, false},
		{"experiment dataset ID", experiment.DatasetID, true},
		{"experiment item ID", item.ID, false},
	} {
		if field.value == "" && field.optional {
			continue
		}
		if field.value == "" || len(field.value) > maxExperimentIdentifierBytes ||
			!utf8.ValidString(field.value) || containsControl(field.value) {
			return fmt.Errorf("%w: %s must be 1 to 255 bytes of valid UTF-8 without control characters",
				ErrInvalidExperiment, field.name)
		}
	}
	if !utf8.ValidString(experiment.Description) || len(experiment.Description) > maxExperimentDescription {
		return fmt.Errorf("%w: description is invalid UTF-8 or exceeds 16 KiB", ErrInvalidExperiment)
	}
	if !item.Version.IsZero() && !validDatasetInstant(item.Version) {
		return fmt.Errorf("%w: item version is outside the RFC 3339 year range", ErrInvalidExperiment)
	}
	return nil
}

func containsControl(value string) bool {
	return strings.ContainsFunc(value, unicode.IsControl)
}

func validDatasetInstant(t time.Time) bool {
	year := t.UTC().Year()
	return year >= 0 && year <= 9999
}
