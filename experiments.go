package langfuse

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
)

// ErrInvalidExperiment reports an experiment, run, or item that
// [Client.StartExperimentItem] or [Client.RunExperiment] rejected.
var ErrInvalidExperiment = errors.New("langfuse: invalid experiment")

// ErrExperimentItemNotExported reports that a borrowed provider's sampler
// dropped an experiment item root.
var ErrExperimentItemNotExported = errors.New("langfuse: experiment item root is not exported")

var (
	errExperimentAfterShutdown = errors.New("langfuse: experiment item started after client shutdown")
	// errExperimentLinkFailed and errExperimentRootRejected are the exported
	// statuses of an item root whose dataset run link failed or that would
	// not be exported with its identity; the returned error carries the cause.
	errExperimentLinkFailed   = errors.New("langfuse: dataset run link failed")
	errExperimentRootRejected = errors.New("langfuse: experiment item root rejected")
)

const (
	maxExperimentIdentifierBytes = 255
	maxExperimentDescription     = lfattr.MaxDirectStringBytes
	maxExperimentMetadataBytes   = 16 << 10
	maxExperimentExpectedOutput  = 256 << 10
)

// Experiment identifies one experiment run.
type Experiment struct {
	// ID identifies the run and is shared by all its items; use a new ID for
	// every run. Required: at most 255 bytes of valid UTF-8 without control
	// characters.
	ID string
	// Name labels the run in Langfuse and may repeat across runs. Required;
	// same rules as ID.
	Name string
	// Description is optional, at most 16 KiB, and not masked.
	Description string
	// Metadata is masked as MaskExperimentMetadata and exported as a JSON
	// object of at most 16 KiB on every span of the item trace.
	Metadata map[string]any
}

// ExperimentItem is one item of an experiment run.
type ExperimentItem struct {
	// ID identifies the item across runs, such as [DatasetItem.ID].
	// Required; same rules as Experiment.ID.
	ID string
	// DatasetID links the item to its Langfuse dataset; empty for local data.
	// Same rules as Experiment.ID.
	DatasetID string
	// Version is the DatasetItemQuery.AsOf the item was read at; zero omits it.
	Version time.Time
	// Input is the task input. It is exported as the root observation's
	// Input, like ObservationAttributes.Input, unless the start values set
	// one.
	Input any
	// ExpectedOutput is masked as MaskExperimentItemExpectedOutput, at most
	// 256 KiB, and omitted when content capture is off. Strings are sent
	// verbatim, a json.RawMessage JSON string as that string, and other
	// values as JSON.
	ExpectedOutput any
	// Metadata is passed to the task and evaluators as supplied. When it is
	// a JSON object, such as a map[string]any or an object json.RawMessage,
	// it is masked as MaskExperimentItemMetadata and exported with the rules
	// of Experiment.Metadata; Langfuse stores no other shape, so other
	// values are not exported.
	Metadata any
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
	ctx, root, _, err := c.startExperimentItem(ctx, experiment, item, name, values, experimentStart{})
	return ctx, root, err
}

// experimentStart holds the runner's additions to an item start.
type experimentStart struct {
	// link, when set, resolves the experiment ID once the root has started,
	// replacing Experiment.ID; the ID it returns is already validated.
	// metadata is the masked experiment metadata JSON, or empty.
	link func(root *Observation, metadata string) (experimentID string, err error)
	// rootMetadata, when set, builds the root's observation metadata from the
	// masked experiment and item metadata.
	rootMetadata func(experimentMetadata, itemMetadata map[string]any) map[string]any
}

func (c *Client) startExperimentItem(
	ctx context.Context,
	experiment Experiment,
	item ExperimentItem,
	name string,
	values ObservationAttributes,
	start experimentStart,
) (context.Context, *Observation, maskedExperimentItem, error) {
	if ctx == nil {
		return nil, &Observation{}, maskedExperimentItem{}, errors.New("langfuse: experiment context is nil")
	}
	err := validateExperiment(experiment, start.link == nil)
	if err == nil {
		err = validateExperimentItem(item)
	}
	if err != nil {
		return c.failedExperimentContext(ctx), &Observation{}, maskedExperimentItem{}, err
	}
	if c == nil || c.isDisabled() {
		return ctx, &Observation{}, maskedExperimentItem{}, nil
	}
	failed := c.failedExperimentContext(ctx)
	if c.stopped.Load() {
		return failed, &Observation{}, maskedExperimentItem{}, errExperimentAfterShutdown
	}
	attributes, err := c.experimentAttributeSets(ctx, experiment, item)
	if err != nil {
		return failed, &Observation{}, maskedExperimentItem{}, err
	}
	if lfattr.IsNil(values.Input) {
		values.Input = item.Input
	}
	if start.rootMetadata != nil {
		values.Metadata = start.rootMetadata(attributes.masked.experimentMetadata, attributes.masked.itemMetadata)
	}
	// The root starts with its identity, except what only exists after the
	// start: a linked experiment ID and the root's own span ID.
	startAttributes := attributes.shared
	if start.link == nil {
		startAttributes = slices.Concat(startAttributes, []attribute.KeyValue{
			attribute.String(lfattr.ExperimentIDKey, experiment.ID),
		})
	}
	itemCtx, root := c.startObservation(ctx, name, TypeSpan, values, &itemRootStart{attributes: startAttributes})
	if root.span == nil {
		return failed, &Observation{}, maskedExperimentItem{}, errExperimentAfterShutdown
	}
	spanContext := root.span.SpanContext()
	if !root.span.IsRecording() || !spanContext.IsSampled() {
		root.span.End()
		return failed, &Observation{}, maskedExperimentItem{}, ErrExperimentItemNotExported
	}
	experimentID := experiment.ID
	if start.link != nil {
		experimentID, err = start.link(root, attributes.metadata)
		if err != nil {
			root.RecordError(errExperimentLinkFailed)
			root.End()
			return failed, &Observation{}, maskedExperimentItem{}, err
		}
	}
	shared := slices.Concat(attributes.shared, []attribute.KeyValue{
		attribute.String(lfattr.ExperimentIDKey, experimentID),
		attribute.String(lfattr.ExperimentItemRootObservationIDKey, spanContext.SpanID().String()),
	})
	root.span.SetAttributes(slices.Concat(shared, attributes.rootOnly)...)
	// Children and scores would reference a root that never arrives, or one
	// with a wrong identity, so the item fails instead.
	if reason := c.unexportableRoot(root.span, shared); reason != "" {
		withoutIdentity(root.span, shared)
		root.RecordError(errExperimentRootRejected)
		root.End()
		return failed, &Observation{}, maskedExperimentItem{}, fmt.Errorf("%w: %s", ErrExperimentItemNotExported, reason)
	}
	itemCtx = c.acceptItemRoot(itemCtx, root)
	state := &experimentState{traceID: spanContext.TraceID(), attributes: shared}
	return context.WithValue(itemCtx, experimentContextKey{client: c}, state), root, attributes.masked, nil
}

// acceptItemRoot completes the bookkeeping for an item root whose export a
// filter may have accepted only once its identity was complete: the root is
// its trace's application root, its children are not, and later spans from
// the returned context, such as evaluators after the root ends, carry the
// trace claim.
func (c *Client) acceptItemRoot(ctx context.Context, root *Observation) context.Context {
	if readable, ok := root.span.(sdktrace.ReadOnlySpan); ok && c.processor != nil {
		c.processor.Expect(readable)
	}
	root.span.SetAttributes(attribute.Bool(lfattr.AppRootKey, true))
	ctx = c.withTraceClaim(ctx, root.span.SpanContext().TraceID())
	return c.syncBaggage(ctx, false, false)
}

// unexportableRoot reports why the Langfuse processor would not export the
// item root with its complete identity, or "". The decision at the root's
// end can still differ for a filter that depends on later attributes.
func (c *Client) unexportableRoot(span oteltrace.Span, identity []attribute.KeyValue) string {
	if !retainsIdentity(span, identity) {
		return "the provider's span limits dropped the item identity"
	}
	if readable, ok := span.(sdktrace.ReadOnlySpan); ok && c.processor != nil && !c.processor.Exports(readable) {
		return "ShouldExportSpan rejected the item root"
	}
	return ""
}

// withoutIdentity blanks the identity of a rejected root so that, if it is
// exported anyway, Langfuse does not take it for an experiment item.
func withoutIdentity(span oteltrace.Span, identity []attribute.KeyValue) {
	blank := make([]attribute.KeyValue, 0, len(identity))
	for _, kv := range identity {
		if identityKeys[kv.Key] && kv.Key != lfattr.EnvironmentKey {
			blank = append(blank, attribute.String(string(kv.Key), ""))
		}
	}
	span.SetAttributes(blank...)
}

// identityKeys are the attributes that link an item; metadata, description,
// and expected output are content, which a borrowed provider's limits may
// truncate like observation input and output.
var identityKeys = map[attribute.Key]bool{
	lfattr.EnvironmentKey:                     true,
	lfattr.ExperimentIDKey:                    true,
	lfattr.ExperimentNameKey:                  true,
	lfattr.ExperimentDatasetIDKey:             true,
	lfattr.ExperimentItemIDKey:                true,
	lfattr.ExperimentItemVersionKey:           true,
	lfattr.ExperimentItemRootObservationIDKey: true,
}

// retainsIdentity reports whether span holds every identity attribute of
// want unchanged; a borrowed provider's span limits can drop or truncate
// them. A span that cannot report its attributes is trusted.
func retainsIdentity(span oteltrace.Span, want []attribute.KeyValue) bool {
	readable, ok := span.(interface{ Attributes() []attribute.KeyValue })
	if !ok {
		return true
	}
	held := make(map[attribute.Key]attribute.Value, len(want))
	for _, kv := range readable.Attributes() {
		held[kv.Key] = kv.Value
	}
	for _, kv := range want {
		if !identityKeys[kv.Key] {
			continue
		}
		if value, ok := held[kv.Key]; !ok || value.Type() != kv.Value.Type() || value.String() != kv.Value.String() {
			return false
		}
	}
	return true
}

// Dropping the ambient span and any WithParent override too makes a
// rejected task a new, unlinked trace.
func (c *Client) failedExperimentContext(ctx context.Context) context.Context {
	ctx = oteltrace.ContextWithSpanContext(c.withoutExperiment(ctx), oteltrace.SpanContext{})
	if c != nil && c.parentOverride(ctx) != nil {
		ctx = context.WithValue(ctx, parentContextKey{client: c}, (*Observation)(nil))
	}
	return ctx
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

// experimentAttributeSet holds an item's attributes before its experiment
// ID and root are known, and the masked values they were encoded from.
type experimentAttributeSet struct {
	shared, rootOnly []attribute.KeyValue
	// metadata is the masked experiment metadata JSON, or empty.
	metadata string
	masked   maskedExperimentItem
}

// maskedExperimentItem holds the masked values of an item that the runner
// exports again, frozen when they were masked: the metadata as fresh maps
// decoded from their encoding, for the root, and as JSON for the evaluator
// observations.
// expectedOutput is nil when content capture is off or it is absent.
type maskedExperimentItem struct {
	experimentMetadata map[string]any
	itemMetadata       map[string]any
	itemMetadataJSON   json.RawMessage
	expectedOutput     json.RawMessage
}

func (c *Client) experimentAttributeSets(
	ctx context.Context,
	experiment Experiment,
	item ExperimentItem,
) (experimentAttributeSet, error) {
	var set experimentAttributeSet
	set.shared = []attribute.KeyValue{
		attribute.String(lfattr.EnvironmentKey, lfattr.ExperimentEnvironment),
		attribute.String(lfattr.ExperimentNameKey, experiment.Name),
		attribute.String(lfattr.ExperimentItemIDKey, item.ID),
	}
	if item.DatasetID != "" {
		set.shared = append(set.shared, attribute.String(lfattr.ExperimentDatasetIDKey, item.DatasetID))
	}
	if !item.Version.IsZero() {
		set.shared = append(set.shared, attribute.String(lfattr.ExperimentItemVersionKey, formatDatasetInstant(item.Version)))
	}
	var err error
	var itemMetadata string
	set.masked.experimentMetadata, set.metadata, err = c.experimentMetadata(MaskExperimentMetadata, experiment.Metadata)
	if err != nil {
		return experimentAttributeSet{}, err
	}
	set.masked.itemMetadata, itemMetadata, err = c.experimentMetadata(MaskExperimentItemMetadata, item.Metadata)
	if err != nil {
		return experimentAttributeSet{}, err
	}
	if set.metadata != "" {
		set.shared = append(set.shared, attribute.String(lfattr.ExperimentMetadataKey, set.metadata))
	}
	if itemMetadata != "" {
		set.shared = append(set.shared, attribute.String(lfattr.ExperimentItemMetadataKey, itemMetadata))
		set.masked.itemMetadataJSON = json.RawMessage(itemMetadata)
	}
	if experiment.Description != "" {
		set.rootOnly = append(set.rootOnly, attribute.String(lfattr.ExperimentDescriptionKey, experiment.Description))
	}
	if !lfattr.IsNil(item.ExpectedOutput) && c.contentCaptureEnabled(ctx) {
		expected, err := c.maskExperimentValue(MaskExperimentItemExpectedOutput, item.ExpectedOutput)
		if err != nil {
			return experimentAttributeSet{}, err
		}
		encoded, present, err := encodeExperimentValue(MaskExperimentItemExpectedOutput, expected,
			maxExperimentExpectedOutput)
		if err != nil {
			return experimentAttributeSet{}, err
		}
		if present {
			set.rootOnly = append(set.rootOnly, attribute.String(lfattr.ExperimentItemExpectedOutputKey, encoded))
			set.masked.expectedOutput = contentJSON(expected, encoded)
		}
	}
	return set, nil
}

// experimentMetadata masks object metadata once and encodes it; masked is
// nil when the metadata is empty, masked away, or not a JSON object.
func (c *Client) experimentMetadata(field MaskField, metadata any) (
	masked map[string]any, encoded string, err error,
) {
	if object, isObject, err := jsonObject(field, metadata); err != nil || !isObject || len(object) == 0 {
		return nil, "", err
	}
	value, err := c.maskExperimentValue(field, metadata)
	if err != nil || lfattr.IsNil(value) {
		return nil, "", err
	}
	masked, isObject, err := jsonObject(field, value)
	if err != nil {
		return nil, "", err
	}
	if !isObject {
		return nil, "", fmt.Errorf("%w: masker changed %s to an unsupported type", ErrInvalidExperiment, field)
	}
	encoded, present, err := encodeExperimentValue(field, masked, maxExperimentMetadataBytes)
	if err != nil || !present {
		return nil, "", err
	}
	// Later copies come from this encoding, made before the next callback
	// could reuse the masker's result, never from that result itself.
	frozen, err := frozenEntries(masked, encoded)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %s %w", ErrInvalidExperiment, field, err)
	}
	return frozen, encoded, nil
}

// frozenEntries rebuilds an object's top-level entries from its one encoding
// so that each exports as the live entry would: a Go string stays a string
// and a nil value stays nil; a JSON string or null token from any other value
// stays that raw token, quoted or null; other values become plain decoded
// values, exact numbers kept, which a metadata masker can still walk.
// live is read only for the entries' types, not serialized again.
func frozenEntries(live map[string]any, encoded string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	var raw map[string]json.RawMessage
	if err := decoder.Decode(&raw); err != nil || raw == nil {
		return nil, lfattr.ErrContentInvalid
	}
	frozen := make(map[string]any, len(raw))
	for key, token := range raw {
		value, exists := live[key]
		switch _, isString := value.(string); {
		case !exists || lfattr.IsNil(value):
			frozen[key] = nil
		case isString:
			var text string
			if err := json.Unmarshal(token, &text); err != nil {
				return nil, lfattr.ErrContentInvalid
			}
			frozen[key] = text
		case len(token) != 0 && (token[0] == '"' || string(token) == "null"):
			frozen[key] = json.RawMessage(bytes.Clone(token))
		default:
			decoded := json.NewDecoder(bytes.NewReader(token))
			decoded.UseNumber()
			var plain any
			if err := decoded.Decode(&plain); err != nil {
				return nil, lfattr.ErrContentInvalid
			}
			frozen[key] = plain
		}
	}
	return frozen, nil
}

// contentJSON is the JSON of content whose string-preserving encoding is
// encoded: the encoding itself, or the JSON string of a string value.
func contentJSON(value any, encoded string) json.RawMessage {
	isString := false
	switch value := value.(type) {
	case string:
		isString = true
	case json.RawMessage:
		trimmed := bytes.TrimSpace(value)
		isString = len(trimmed) != 0 && trimmed[0] == '"'
	}
	if !isString {
		return json.RawMessage(encoded)
	}
	data, err := json.Marshal(encoded)
	if err != nil {
		return nil
	}
	return data
}

// mayEncodeAsObject reports whether encoding/json could encode value as an
// object: a map, a struct, or a type with its own MarshalJSON. Other values
// are never exported as metadata, so they need not be serialized at all.
func mayEncodeAsObject(value any) bool {
	reflected := reflect.ValueOf(value)
	for reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface {
		if reflected.IsNil() {
			return false
		}
		if _, ok := reflected.Interface().(json.Marshaler); ok {
			return true
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() {
		return false
	}
	if _, ok := reflected.Interface().(json.Marshaler); ok {
		return true
	}
	if _, ok := reflected.Interface().(encoding.TextMarshaler); ok {
		return false
	}
	switch reflected.Kind() {
	case reflect.Map, reflect.Struct:
		return true
	default:
		return false
	}
}

// jsonObject returns value as a JSON object; isObject is false for another
// JSON value. Numbers keep their exact tokens.
func jsonObject(field MaskField, value any) (object map[string]any, isObject bool, err error) {
	if object, ok := value.(map[string]any); ok {
		return object, true, nil
	}
	if lfattr.IsNil(value) {
		return nil, false, nil
	}
	data, ok := value.(json.RawMessage)
	if !ok {
		if !mayEncodeAsObject(value) {
			return nil, false, nil
		}
		encoded, err, panicked := lfattr.MarshalJSON(value, maxExperimentMetadataBytes)
		if panicked || err != nil {
			return nil, false, fmt.Errorf("%w: %s could not be serialized or exceeds 16 KiB", ErrInvalidExperiment, field)
		}
		data = encoded
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return nil, false, nil
	}
	if len(data) > 4*maxExperimentMetadataBytes || !utf8.Valid(data) {
		return nil, false, fmt.Errorf("%w: %s is invalid UTF-8 or exceeds 16 KiB", ErrInvalidExperiment, field)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || decoder.More() {
		return nil, false, fmt.Errorf("%w: %s is not valid JSON", ErrInvalidExperiment, field)
	}
	return object, true, nil
}

func (c *Client) maskExperimentValue(field MaskField, value any) (any, error) {
	masked, ok := c.maskStrict(field, value)
	if !ok {
		return nil, fmt.Errorf("%w: masker panicked on %s", ErrInvalidExperiment, field)
	}
	return masked, nil
}

func encodeExperimentValue(field MaskField, value any, limit int) (string, bool, error) {
	encoded, present, err := lfattr.EncodeContent(value, limit)
	if err != nil {
		return "", false, fmt.Errorf("%w: %s %w", ErrInvalidExperiment, field, err)
	}
	return encoded, present, nil
}

// validateExperiment checks the run-level fields; an empty ID passes unless
// requireID is set.
func validateExperiment(experiment Experiment, requireID bool) error {
	if experiment.ID != "" || requireID {
		if err := validateExperimentIdentifier("experiment ID", experiment.ID); err != nil {
			return err
		}
	}
	if err := validateExperimentIdentifier("experiment name", experiment.Name); err != nil {
		return err
	}
	if !utf8.ValidString(experiment.Description) || len(experiment.Description) > maxExperimentDescription {
		return fmt.Errorf("%w: description is invalid UTF-8 or exceeds 16 KiB", ErrInvalidExperiment)
	}
	return nil
}

func validateExperimentItem(item ExperimentItem) error {
	if err := validateExperimentIdentifier("experiment item ID", item.ID); err != nil {
		return err
	}
	if item.DatasetID != "" {
		if err := validateExperimentIdentifier("experiment item dataset ID", item.DatasetID); err != nil {
			return err
		}
	}
	if !item.Version.IsZero() && !validDatasetInstant(item.Version) {
		return fmt.Errorf("%w: item version is outside the RFC 3339 year range", ErrInvalidExperiment)
	}
	return nil
}

func validateExperimentIdentifier(name, value string) error {
	if value == "" || len(value) > maxExperimentIdentifierBytes ||
		!utf8.ValidString(value) || containsControl(value) {
		return fmt.Errorf("%w: %s must be 1 to 255 bytes of valid UTF-8 without control characters",
			ErrInvalidExperiment, name)
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
