package langfuse

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

// ErrInvalidExperiment reports that [Client.StartExperimentItem] rejected
// its experiment or item: an invalid identifier or version, or metadata or
// expected output that could not be masked, encoded, or bounded. Test with
// errors.Is.
var ErrInvalidExperiment = errors.New("langfuse: invalid experiment")

// ErrExperimentItemNotExported reports that an experiment item root would
// not be exported, so Langfuse could never list the item: a borrowed
// provider's sampler did not sample it, or Config.ShouldExportSpan rejected
// it at start. Test with errors.Is.
var ErrExperimentItemNotExported = errors.New("langfuse: experiment item root is not exported")

const (
	maxExperimentIdentifierBytes = 255
	maxExperimentDescription     = lfattr.MaxDirectStringBytes
	maxExperimentMetadataBytes   = 16 << 10
	maxExperimentExpectedOutput  = 256 << 10
)

// Experiment identifies one experiment run: one execution of a task over a
// set of items, compared with other runs in Langfuse.
type Experiment struct {
	// ID groups every item of one run. Required, and it must be the same for
	// every item of the run: create it once, for example as a UUID or as the
	// run name plus a UTC timestamp. At most 255 bytes of valid UTF-8 without
	// control characters.
	ID string
	// Name is the run name shown in Langfuse. Required; same rules as ID.
	Name string
	// Description is optional, is exported on each item root, and is not
	// processed by Config.Mask. At most 16 KiB.
	Description string
	// DatasetID links the run to a Langfuse dataset: use [Dataset.ID], or
	// [DatasetItem.DatasetID]. Leave it empty for local data.
	DatasetID string
	// Metadata is passed to Config.Mask as MaskExperimentMetadata once per
	// item start and exported as one JSON object on every span of the item
	// trace, which Langfuse flattens into dotted keys. The encoded object is
	// limited to 16 KiB, and no two keys may flatten to the same path.
	Metadata map[string]any
}

// ExperimentItem identifies one item of an experiment run.
type ExperimentItem struct {
	// ID identifies the item across runs. Required: use [DatasetItem.ID] for
	// a Langfuse dataset, or a stable caller-chosen ID for local data. Same
	// rules as Experiment.ID.
	ID string
	// Version is the dataset version the item was read at: the
	// DatasetItemQuery.AsOf of the read. Zero omits it.
	Version time.Time
	// ExpectedOutput is exported on the item root. It passes Config.Mask as
	// MaskExperimentItemExpectedOutput, follows the starting context's
	// content-capture setting like observation input and output, and is
	// limited to 256 KiB. Strings are sent verbatim; a json.RawMessage
	// holding a JSON string is sent as the decoded string, other raw JSON as
	// compact JSON, and other values as JSON.
	ExpectedOutput any
	// Metadata follows the rules of Experiment.Metadata, masked as
	// MaskExperimentItemMetadata. Decode a dataset item's metadata with
	// json.Decoder.UseNumber to keep large numbers exact.
	Metadata map[string]any
}

type (
	experimentContextKey     struct{ client *Client }
	experimentRootContextKey struct{ client *Client }
)

// experimentRootToken carries an item root's finalized identity into the
// processor's OnStart for that root alone. It is created per start, and the
// first matching OnStart claims it: that is the root itself, because
// Tracer.Start runs OnStart synchronously before returning, so the root's
// real span ID becomes the canonical pointer before any export
// classification or child start.
type experimentRootToken struct {
	claimed    atomic.Bool
	attributes []attribute.KeyValue
}

// experimentState is the immutable per-item projection stored in the
// context: the item's trace and the already masked and encoded attributes
// every span of that trace carries.
type experimentState struct {
	traceID    oteltrace.TraceID
	attributes []attribute.KeyValue
}

// StartExperimentItem starts the root observation of one experiment item as
// the root of a new trace, and returns a context that carries the item's
// identity to spans started from it.
//
// The returned observation is the item's canonical observation: Langfuse
// lists it as the experiment item and measures item latency on it. Set the
// task input in values.Input, end the observation when the task finishes,
// and attach item scores to it with Score{TraceID: obs.TraceID(),
// ObservationID: obs.ID()}. Evaluators may keep using the returned context
// after End; their spans count toward item cost, not latency.
//
// Every span started from the returned context in the same trace, on the
// provider that runs this client's processor, carries the experiment and
// item identity and langfuse.environment "sdk-experiment". Those values
// replace any the span was started with. Spans on other providers, such as
// instrumentation on the global provider while this client owns an isolated
// provider, are not stamped. A context detached into a new trace carries no
// identity.
//
// In isolated mode the item trace is always sampled, overriding
// Config.SampleRate and Client.WithSampleRate for that trace only. With a
// borrowed provider the application's sampler decides; an item root it does
// not sample, or that Config.ShouldExportSpan rejects at start, is ended and
// reported as [ErrExperimentItemNotExported]. A started root can still be
// lost later by end-time filtering, a full queue, export failure, or ending
// after Shutdown, so end every item before Flush and reconcile the expected
// item count through the Langfuse experiment API when completeness matters.
//
// Invalid input returns an error wrapping [ErrInvalidExperiment] and starts
// nothing. Any error returns a no-op observation and a context with neither
// experiment identity for this client nor an ambient span, so the task can
// still run as a new, unlinked trace without being attributed to an
// enclosing item.
// A nil or disabled client validates the input and then returns ctx, a no-op
// observation, and nil without masking anything. A stopped client returns an
// error. A nil ctx is an error.
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
	cleared := c.withoutExperiment(ctx)
	failed := c.failedExperimentContext(ctx)
	if c.stopped.Load() {
		return failed, &Observation{}, errors.New("langfuse: experiment item started after client shutdown")
	}
	identity, content, rootOnly, err := c.experimentAttributeSets(ctx, experiment, item)
	if err != nil {
		return failed, &Observation{}, err
	}
	// A placeholder reserves the canonical pointer's attribute slot next to
	// the identity; the processor replaces it with the real span ID.
	placeholder := attribute.String(lfattr.ExperimentItemRootObservationIDKey, "")
	token := &experimentRootToken{attributes: slices.Concat(identity, content, rootOnly)}
	start := observationStart{
		root:      true,
		leading:   slices.Concat(identity, []attribute.KeyValue{placeholder}, content, rootOnly),
		rootToken: token,
	}
	if c.owned {
		always := 1.0
		start.sampleRate = &always
	}
	itemCtx, started := c.startObservation(cleared, name, TypeSpan, values, start)
	if started.span == nil {
		// The client stopped during the start.
		return failed, &Observation{}, errors.New("langfuse: experiment item started after client shutdown")
	}
	spanContext := started.span.SpanContext()
	root := attribute.String(lfattr.ExperimentItemRootObservationIDKey, spanContext.SpanID().String())
	required := slices.Concat(identity, []attribute.KeyValue{root},
		[]attribute.KeyValue{attribute.String(lfattr.EnvironmentKey, lfattr.ExperimentEnvironment)})
	if !started.span.IsRecording() || !spanContext.IsSampled() || !started.admitted || !started.expected ||
		!token.claimed.Load() || !spanHasAttributes(started.span, required) {
		// Suppress the root at end too: an export filter that accepts it
		// later must not publish an item the caller was told does not exist.
		if started.span.IsRecording() {
			c.processor.Abort(spanContext)
		}
		started.span.End()
		return failed, &Observation{}, ErrExperimentItemNotExported
	}
	state := &experimentState{
		traceID:    spanContext.TraceID(),
		attributes: slices.Concat(identity, []attribute.KeyValue{root}, content),
	}
	return context.WithValue(itemCtx, experimentContextKey{client: c}, state), started.observation, nil
}

// spanHasAttributes reports whether the started span still carries every
// required attribute value. A borrowed provider's count or value-length
// limits can drop or truncate them, and an item root without its complete
// identity can never be listed.
func spanHasAttributes(span oteltrace.Span, required []attribute.KeyValue) bool {
	readOnly, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		return false
	}
	actual := make(map[attribute.Key]attribute.Value, len(required))
	for _, item := range readOnly.Attributes() {
		actual[item.Key] = item.Value
	}
	for _, item := range required {
		if value, found := actual[item.Key]; !found || value != item.Value {
			return false
		}
	}
	return true
}

// failedExperimentContext is the context returned with a start error. It is
// safe for running the rejected task: it carries no experiment identity for
// this client and no ambient span, so the task's spans form a new, unlinked
// trace instead of joining an enclosing item or request trace, exactly as a
// successful start would not have joined them either.
func (c *Client) failedExperimentContext(ctx context.Context) context.Context {
	return oteltrace.ContextWithSpanContext(c.withoutExperiment(ctx), oteltrace.SpanContext{})
}

// withoutExperiment returns ctx with this client's experiment projection
// cleared, so a failed nested start never leaves the enclosing item's
// identity on work the caller runs next.
func (c *Client) withoutExperiment(ctx context.Context) context.Context {
	if c == nil {
		return ctx
	}
	if state, _ := ctx.Value(experimentContextKey{client: c}).(*experimentState); state == nil {
		return ctx
	}
	return context.WithValue(ctx, experimentContextKey{client: c}, (*experimentState)(nil))
}

// authoritativeAttributes is the processor hook: the finalized identity for
// an item root claiming its token, or the item projection for any span of an
// item trace started from ctx.
func (c *Client) authoritativeAttributes(ctx context.Context, span sdktrace.ReadOnlySpan) []attribute.KeyValue {
	if c == nil || ctx == nil {
		return nil
	}
	spanContext := span.SpanContext()
	if token, _ := ctx.Value(experimentRootContextKey{client: c}).(*experimentRootToken); token != nil &&
		!span.Parent().IsValid() && span.InstrumentationScope().Name == lfattr.TracerName &&
		token.claimed.CompareAndSwap(false, true) {
		return append(slices.Clone(token.attributes),
			attribute.String(lfattr.ExperimentItemRootObservationIDKey, spanContext.SpanID().String()))
	}
	return c.experimentAttributes(ctx, spanContext.TraceID())
}

// experimentAttributes returns the item identity for a span of traceID
// started from ctx, or nil when ctx carries no item of that trace. It also
// seeds SDK observations' creation attributes.
func (c *Client) experimentAttributes(ctx context.Context, traceID oteltrace.TraceID) []attribute.KeyValue {
	if c == nil || ctx == nil || !traceID.IsValid() {
		return nil
	}
	state, _ := ctx.Value(experimentContextKey{client: c}).(*experimentState)
	if state == nil || state.traceID != traceID {
		return nil
	}
	return slices.Clone(state.attributes)
}

// inExperimentTrace reports whether ctx's active span belongs to an
// experiment item trace of this client.
func (c *Client) inExperimentTrace(ctx context.Context) bool {
	return len(c.experimentAttributes(ctx, oteltrace.SpanFromContext(ctx).SpanContext().TraceID())) != 0
}

// experimentAttributeSets masks and encodes the item once. identity and
// content go on every span of the item trace, rootOnly on the item root
// alone. Identity comes first so attribute limits drop content before it.
func (c *Client) experimentAttributeSets(
	ctx context.Context,
	experiment Experiment,
	item ExperimentItem,
) (identity, content, rootOnly []attribute.KeyValue, err error) {
	identity = []attribute.KeyValue{
		attribute.String(lfattr.ExperimentIDKey, experiment.ID),
		attribute.String(lfattr.ExperimentNameKey, experiment.Name),
		attribute.String(lfattr.ExperimentItemIDKey, item.ID),
	}
	if experiment.DatasetID != "" {
		identity = append(identity, attribute.String(lfattr.ExperimentDatasetIDKey, experiment.DatasetID))
	}
	if !item.Version.IsZero() {
		identity = append(identity, attribute.String(lfattr.ExperimentItemVersionKey, formatDatasetInstant(item.Version)))
	}
	content = []attribute.KeyValue{attribute.String(lfattr.EnvironmentKey, lfattr.ExperimentEnvironment)}
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
			return nil, nil, nil, err
		}
		if present {
			content = append(content, attribute.String(metadata.key, encoded))
		}
	}
	if experiment.Description != "" {
		rootOnly = append(rootOnly, attribute.String(lfattr.ExperimentDescriptionKey, experiment.Description))
	}
	if !lfattr.IsNil(item.ExpectedOutput) && c.contentCaptureEnabled(ctx) {
		expected, err := c.maskExperimentValue(MaskExperimentItemExpectedOutput, item.ExpectedOutput)
		if err != nil {
			return nil, nil, nil, err
		}
		encoded, present, err := lfattr.EncodeContent(expected, maxExperimentExpectedOutput)
		if err != nil {
			return nil, nil, nil, experimentContentError(MaskExperimentItemExpectedOutput, err)
		}
		if present {
			rootOnly = append(rootOnly, attribute.String(lfattr.ExperimentItemExpectedOutputKey, encoded))
		}
	}
	return identity, content, rootOnly, nil
}

// experimentMetadata masks one metadata map once and encodes the result as
// a JSON object. A mask result of nil deliberately omits the metadata, as
// for observation metadata; a panic or a result of another type is an error.
func (c *Client) experimentMetadata(field MaskField, metadata map[string]any) (string, bool, error) {
	if len(metadata) == 0 {
		return "", false, nil
	}
	masked, err := c.maskExperimentValue(field, metadata)
	if err != nil {
		return "", false, err
	}
	if lfattr.IsNil(masked) {
		return "", false, nil
	}
	object, ok := masked.(map[string]any)
	if !ok {
		return "", false, &datasetError{
			message: "langfuse: invalid experiment: masker changed " + string(field) + " to an unsupported type",
			wrapped: []error{ErrInvalidExperiment},
		}
	}
	encoded, present, err := lfattr.EncodeMetadataObject(object, maxExperimentMetadataBytes)
	if err != nil {
		return "", false, experimentContentError(field, err)
	}
	return encoded, present, nil
}

func (c *Client) maskExperimentValue(field MaskField, value any) (masked any, err error) {
	if c.mask == nil {
		return value, nil
	}
	defer func() {
		if recover() != nil {
			masked = nil
			err = &datasetError{
				message: "langfuse: invalid experiment: masker panicked on " + string(field),
				wrapped: []error{ErrInvalidExperiment},
			}
		}
	}()
	return c.mask(string(field), value), nil
}

func experimentContentError(field MaskField, cause error) error {
	reason := "could not be serialized"
	switch {
	case errors.Is(cause, lfattr.ErrContentTooLarge):
		reason = "exceeds its size limit"
	case errors.Is(cause, lfattr.ErrMetadataShape):
		reason = "has an invalid, reserved, or colliding key path"
	}
	return &datasetError{
		message: "langfuse: invalid experiment: " + string(field) + " " + reason,
		wrapped: []error{ErrInvalidExperiment},
	}
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
			return &datasetError{
				message: "langfuse: invalid experiment: " + field.name +
					" must be 1 to 255 bytes of valid UTF-8 without control characters",
				wrapped: []error{ErrInvalidExperiment},
			}
		}
	}
	if !utf8.ValidString(experiment.Description) || len(experiment.Description) > maxExperimentDescription {
		return &datasetError{
			message: "langfuse: invalid experiment: description is invalid UTF-8 or exceeds 16 KiB",
			wrapped: []error{ErrInvalidExperiment},
		}
	}
	if !item.Version.IsZero() && !validDatasetInstant(item.Version) {
		return &datasetError{
			message: "langfuse: invalid experiment: item version is outside the RFC 3339 year range",
			wrapped: []error{ErrInvalidExperiment},
		}
	}
	return nil
}
