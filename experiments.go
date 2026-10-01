package langfuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

// ErrInvalidExperiment reports that [Client.StartExperimentItem] rejected
// its input: an invalid identifier or version, or metadata or expected
// output that could not be masked, encoded, or bounded.
var ErrInvalidExperiment = errors.New("langfuse: invalid experiment")

// ErrExperimentItemNotExported reports that a borrowed provider's sampler
// did not sample an experiment item root, so Langfuse cannot list the item.
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
	// ID groups the items of one run; use the same ID for every item.
	// Required: at most 255 bytes of valid UTF-8 without control characters.
	ID string
	// Name is the run name shown in Langfuse. Required; same rules as ID.
	Name string
	// Description is optional, at most 16 KiB, exported on each item root,
	// and not masked.
	Description string
	// DatasetID links the run to a Langfuse dataset. Leave it empty for
	// local data.
	DatasetID string
	// Metadata is masked as MaskExperimentMetadata once per item start and
	// exported as a JSON object of at most 16 KiB on every span of the item
	// trace. A Mask result of nil omits it.
	Metadata map[string]any
}

// ExperimentItem identifies one item of an experiment run.
type ExperimentItem struct {
	// ID identifies the item across runs, such as [DatasetItem.ID].
	// Required; same rules as Experiment.ID.
	ID string
	// Version is the DatasetItemQuery.AsOf the item was read at. Zero omits
	// it.
	Version time.Time
	// ExpectedOutput is exported on the item root, at most 256 KiB, masked
	// as MaskExperimentItemExpectedOutput, and omitted when content capture
	// is off. Strings are sent verbatim, a json.RawMessage holding a JSON
	// string as that string, and other values as JSON.
	ExpectedOutput any
	// Metadata follows the rules of Experiment.Metadata, masked as
	// MaskExperimentItemMetadata.
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
// Langfuse lists the returned observation as the item and measures item
// latency on it, so end it when the task finishes; evaluators may keep using
// the returned context after End. Spans started from that context in the
// item trace, on the provider running this client's processor, get the item
// identity and langfuse.environment "sdk-experiment" at start. Later changes
// to those attributes are the caller's.
//
// Isolated mode always samples the item trace. With a borrowed provider, a
// root its sampler drops returns [ErrExperimentItemNotExported]. Any error
// returns a no-op observation and a context without the item identity or an
// ambient span. A nil or disabled client only validates the input.
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

// failedExperimentContext drops the ambient span as well as the identity,
// so a rejected task runs as a new, unlinked trace.
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

// experimentAttributes returns the item attributes for a span of traceID
// started from ctx, or nil when ctx carries no item of that trace.
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

// experimentAttributeSets masks and encodes the item once. shared goes on
// every span of the item trace, rootOnly on the item root alone.
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

// experimentMetadata masks and encodes one metadata map. A nil mask result
// omits it; any other non-map result is an error.
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
