package lunte

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/lunte/internal/attributes"
	"github.com/fgn/lunte/internal/diagnostic"
)

// Observation is a concurrency-safe handle around one OpenTelemetry span.
// Its zero value is a safe no-op.
type Observation struct {
	mu                 sync.Mutex
	client             *Client
	span               oteltrace.Span
	typeName           ObservationType
	ended              bool
	explicit           map[string]struct{}
	metadataKeys       map[string]struct{}
	attributeSizes     map[string]int
	attributeBytes     int
	errorEvents        int
	errorLimitReported bool
}

const maxErrorEvents = 8

// StartObservation starts an observation. Child work must use the returned
// context to preserve the parent-child relationship.
func (c *Client) StartObservation(
	ctx context.Context,
	name string,
	observationType ObservationType,
	values ObservationAttributes,
) (context.Context, *Observation) {
	if c == nil || c.isDisabled() || ctx == nil {
		return ctx, &Observation{}
	}
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx, &Observation{}
	}
	if name == "" {
		diagnostic.Report("observation name is empty; using \"observation\"")
		name = "observation"
	} else if normalized, ok := observationString("observation name", name); ok {
		name = normalized
	} else {
		name = "observation"
	}
	typeName := normalizeObservationType(observationType)
	values = normalizeObservationStrings(values)
	spanAttributes, explicit := c.buildObservationAttributes(typeName, values, true, nil)
	spanAttributes, attributeSizes, attributeBytes, attributesOmitted := fitObservationAttributeBudget(
		spanAttributes, nil, 0,
	)
	explicit = acceptedExplicitAttributes(explicit, spanAttributes)
	if attributesOmitted {
		diagnostic.Report("observation attributes exceed the aggregate size limit; remaining fields omitted")
	}
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx, &Observation{}
	}
	options := []oteltrace.SpanStartOption{oteltrace.WithAttributes(spanAttributes...)}
	if !values.StartTime.IsZero() {
		options = append(options, oteltrace.WithTimestamp(values.StartTime))
	}
	spanCtx, span := c.tracer.Start(ctx, name, options...)
	if values.Level == LevelError {
		span.SetStatus(codes.Error, values.StatusMessage)
	}
	observation := &Observation{
		client:         c,
		span:           span,
		typeName:       typeName,
		explicit:       explicit,
		metadataKeys:   observationMetadataKeys(spanAttributes),
		attributeSizes: attributeSizes,
		attributeBytes: attributeBytes,
	}
	spanCtx = context.WithValue(spanCtx, observationContextKey{client: c}, observation)
	if span.IsRecording() && span.SpanContext().IsSampled() {
		spanCtx = c.withTraceClaim(spanCtx, span.SpanContext().TraceID())
	}
	return spanCtx, observation
}

// Observe starts an observation, runs fn with the child context and the
// observation handle, and always ends the observation, including when fn
// panics. A non-nil error returned by fn is recorded through RecordError and
// returned unchanged, so fn does not need to record it itself. When fn panics,
// the observation is marked failed with the payload-free status "panic" — the
// panic value is never captured — and the panic propagates. On a nil,
// disabled, or stopped client fn still runs, receiving ctx unchanged and a
// no-op handle. A nil fn reports a diagnostic and starts no observation.
func (c *Client) Observe(
	ctx context.Context,
	name string,
	observationType ObservationType,
	values ObservationAttributes,
	fn func(ctx context.Context, observation *Observation) error,
) error {
	if fn == nil {
		diagnostic.Report("observe callback is nil; no observation started")
		return nil
	}
	observationCtx, observation := c.StartObservation(ctx, name, observationType, values)
	completed := false
	defer func() {
		if !completed {
			// fn is unwinding from a panic. Record a payload-free failure: the
			// panic value is not explicitly supplied telemetry content.
			observation.Update(ObservationAttributes{Level: LevelError, StatusMessage: "panic"})
		}
		observation.End()
	}()
	err := fn(observationCtx, observation)
	completed = true
	if err != nil {
		observation.RecordError(err)
	}
	return err
}

// Event records an instantaneous event observation.
func (c *Client) Event(ctx context.Context, name string, values ObservationAttributes) {
	if !values.StartTime.IsZero() && c != nil && !c.isDisabled() && ctx != nil && !c.stopped.Load() {
		diagnostic.Report("event start time ignored; events use their recording time")
		values.StartTime = time.Time{}
	}
	_, observation := c.StartObservation(ctx, name, TypeEvent, values)
	observation.End()
}

// Update merges non-zero fields into an active observation. Structured model,
// usage, and cost maps replace their complete serialized attributes when set;
// metadata merges by top-level key. StartTime cannot be changed after start
// and is ignored with a diagnostic.
func (o *Observation) Update(values ObservationAttributes) {
	if o == nil || o.client == nil || o.span == nil {
		return
	}
	if o.client.stopped.Load() {
		o.client.reportStoppedOnce()
		return
	}
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		diagnostic.Report("update ignored after observation end")
		return
	}
	existingMetadata := cloneStringSet(o.metadataKeys)
	o.mu.Unlock()
	values = normalizeObservationStrings(values)
	spanAttributes, explicit := o.client.buildObservationAttributes(o.typeName, values, false, existingMetadata)
	if o.client.stopped.Load() {
		o.client.reportStoppedOnce()
		return
	}
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		diagnostic.Report("update ignored after observation end")
		return
	}
	candidates := spanAttributes[:0]
	metadataOmitted := false
	stagedMetadata := make(map[string]struct{})
	for _, item := range spanAttributes {
		key := string(item.Key)
		if strings.HasPrefix(key, lfattr.ObservationMetadataKey+".") {
			metadataKey := strings.TrimPrefix(key, lfattr.ObservationMetadataKey+".")
			if _, exists := o.metadataKeys[metadataKey]; !exists {
				if _, staged := stagedMetadata[metadataKey]; !staged && len(o.metadataKeys)+len(stagedMetadata) >= lfattr.MaxMetadataEntries {
					metadataOmitted = true
					continue
				}
				stagedMetadata[metadataKey] = struct{}{}
			}
		}
		candidates = append(candidates, item)
	}
	filtered, attributeSizes, attributeBytes, attributesOmitted := fitObservationAttributeBudget(
		candidates, o.attributeSizes, o.attributeBytes,
	)
	o.attributeSizes = attributeSizes
	o.attributeBytes = attributeBytes
	for _, item := range filtered {
		key := string(item.Key)
		metadataKey := strings.TrimPrefix(key, lfattr.ObservationMetadataKey+".")
		if _, staged := stagedMetadata[metadataKey]; !staged {
			continue
		}
		if o.metadataKeys == nil {
			o.metadataKeys = make(map[string]struct{})
		}
		o.metadataKeys[metadataKey] = struct{}{}
	}
	if len(filtered) != 0 {
		o.span.SetAttributes(filtered...)
	}
	if values.Level == LevelError {
		o.span.SetStatus(codes.Error, values.StatusMessage)
	}
	for key := range acceptedExplicitAttributes(explicit, filtered) {
		if o.explicit == nil {
			o.explicit = make(map[string]struct{})
		}
		o.explicit[key] = struct{}{}
	}
	o.mu.Unlock()
	if metadataOmitted {
		diagnostic.Report("observation metadata exceeds the lifetime entry limit; new entries omitted")
	}
	if attributesOmitted {
		diagnostic.Report("observation attributes exceed the aggregate size limit; remaining fields omitted")
	}
}

func observationMetadataKeys(attributes []attribute.KeyValue) map[string]struct{} {
	var result map[string]struct{}
	for _, item := range attributes {
		key := string(item.Key)
		if !strings.HasPrefix(key, lfattr.ObservationMetadataKey+".") {
			continue
		}
		if result == nil {
			result = make(map[string]struct{})
		}
		result[strings.TrimPrefix(key, lfattr.ObservationMetadataKey+".")] = struct{}{}
	}
	return result
}

func cloneStringSet(values map[string]struct{}) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(values))
	for key := range values {
		result[key] = struct{}{}
	}
	return result
}

func fitObservationAttributeBudget(
	attributes []attribute.KeyValue,
	sizes map[string]int,
	used int,
) ([]attribute.KeyValue, map[string]int, int, bool) {
	if sizes == nil {
		sizes = make(map[string]int, len(attributes))
	}
	filtered := attributes[:0]
	omitted := false
	for _, item := range attributes {
		key := string(item.Key)
		size := observationAttributeSize(item)
		previous := sizes[key]
		next := used - previous + size
		reserved := sizes[lfattr.ObservationLevelKey] + sizes[lfattr.ObservationStatusMessageKey]
		if key == lfattr.ObservationLevelKey || key == lfattr.ObservationStatusMessageKey {
			reserved = reserved - previous + size
		}
		if next <= lfattr.MaxObservationAttributeBytes &&
			next-reserved <= lfattr.MaxObservationDataAttributeBytes {
			used = next
			sizes[key] = size
			filtered = append(filtered, item)
		} else {
			omitted = true
		}
	}
	return filtered, sizes, used, omitted
}

func observationAttributeSize(item attribute.KeyValue) int {
	const protobufOverhead = 16
	size := len(item.Key) + protobufOverhead
	value := item.Value
	switch value.Type() {
	case attribute.BOOL:
		return size + 1
	case attribute.INT64, attribute.FLOAT64:
		return size + 8
	case attribute.STRING:
		return size + len(value.AsString())
	case attribute.BOOLSLICE:
		return size + len(value.AsBoolSlice())
	case attribute.INT64SLICE:
		return size + 8*len(value.AsInt64Slice())
	case attribute.FLOAT64SLICE:
		return size + 8*len(value.AsFloat64Slice())
	case attribute.STRINGSLICE:
		for _, item := range value.AsStringSlice() {
			size += protobufOverhead + len(item)
		}
		return size
	case attribute.BYTESLICE:
		return size + len(value.AsByteSlice())
	case attribute.SLICE:
		for _, item := range value.AsSlice() {
			size += protobufOverhead + observationAttributeSize(attribute.KeyValue{Value: item})
		}
		return size
	default:
		return size
	}
}

func acceptedExplicitAttributes(explicit map[string]struct{}, attributes []attribute.KeyValue) map[string]struct{} {
	if len(explicit) == 0 {
		return nil
	}
	accepted := make(map[string]struct{}, len(explicit))
	for _, item := range attributes {
		key := string(item.Key)
		if _, found := explicit[key]; found {
			accepted[key] = struct{}{}
		}
	}
	return accepted
}

// RecordError records an exception and marks the observation as failed. It
// does not end the observation. At most eight exception events are retained;
// later calls are omitted with one diagnostic. The error text is explicitly
// supplied content and is not processed by Config.Mask. Invalid UTF-8 or text
// over 64 KiB is replaced by the payload-free string "error".
func (o *Observation) RecordError(err error) {
	if o == nil || err == nil || o.client == nil || o.span == nil {
		return
	}
	if o.client.stopped.Load() {
		o.client.reportStoppedOnce()
		return
	}
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		diagnostic.Report("error ignored after observation end")
		return
	}
	if o.errorEvents >= maxErrorEvents {
		report := !o.errorLimitReported
		o.errorLimitReported = true
		o.mu.Unlock()
		if report {
			diagnostic.Report("observation error-event limit reached; additional errors omitted")
		}
		return
	}
	// Reserve before invoking Error: concurrent/re-entrant callers cannot all
	// allocate messages and then discover the cap. Error remains outside the
	// lock because it is application code and may call back into this handle.
	o.errorEvents++
	o.mu.Unlock()

	message := safeErrorMessage(err)
	if o.client.stopped.Load() {
		o.client.reportStoppedOnce()
		return
	}
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		diagnostic.Report("error ignored after observation end")
		return
	}
	errorAttributes := []attribute.KeyValue{
		attribute.String(lfattr.ObservationLevelKey, string(LevelError)),
		attribute.String(lfattr.ObservationStatusMessageKey, message),
	}
	filtered, attributeSizes, attributeBytes, attributesOmitted := fitObservationAttributeBudget(
		errorAttributes, o.attributeSizes, o.attributeBytes,
	)
	o.attributeSizes = attributeSizes
	o.attributeBytes = attributeBytes
	o.span.AddEvent(semconv.ExceptionEventName, oteltrace.WithAttributes(
		semconv.ExceptionType(errorType(err)),
		semconv.ExceptionMessage(message),
	))
	o.span.SetStatus(codes.Error, message)
	if len(filtered) != 0 {
		o.span.SetAttributes(filtered...)
	}
	o.mu.Unlock()
	if attributesOmitted {
		diagnostic.Report("observation attributes exceed the aggregate size limit; remaining fields omitted")
	}
}

func errorType(err error) string {
	typeOf := reflect.TypeOf(err)
	if typeOf.PkgPath() == "" && typeOf.Name() == "" {
		return typeOf.String()
	}
	return typeOf.PkgPath() + "." + typeOf.Name()
}

func safeErrorMessage(err error) (message string) {
	defer func() {
		if recover() != nil {
			diagnostic.Report("error string method panicked; generic error recorded")
			message = "error"
		}
	}()
	message = err.Error()
	if !utf8.ValidString(message) || len(message) > lfattr.MaxErrorMessageBytes {
		diagnostic.Report("error string is invalid or exceeds the internal size limit; generic error recorded")
		return "error"
	}
	return message
}

func normalizeObservationStrings(values ObservationAttributes) ObservationAttributes {
	if values.StatusMessage != "" {
		values.StatusMessage, _ = observationString("observation status message", values.StatusMessage)
	}
	if values.Version != "" {
		values.Version, _ = observationString("observation version", values.Version)
	}
	if values.Model != "" {
		values.Model, _ = observationString("observation model", values.Model)
	}
	if values.Prompt != nil {
		prompt := *values.Prompt
		if prompt.Name != "" {
			if normalized, ok := observationString("prompt name", prompt.Name); ok {
				prompt.Name = normalized
			} else {
				values.Prompt = nil
				return values
			}
		}
		values.Prompt = &prompt
	}
	return values
}

func observationString(field, value string) (string, bool) {
	if !utf8.ValidString(value) || len(value) > lfattr.MaxDirectStringBytes {
		diagnostic.Report(field + " is invalid or exceeds the internal size limit; value omitted")
		return "", false
	}
	return value, true
}

// End ends the observation exactly once.
func (o *Observation) End() {
	if o == nil || o.span == nil {
		return
	}
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		return
	}
	o.ended = true
	span := o.span
	o.mu.Unlock()

	// Span.End synchronously invokes application-owned processors. Never hold
	// the observation lock across those callbacks: diagnostics and processors
	// are allowed to re-enter this handle or its Client.
	span.End()
}

// TraceID returns the lowercase 32-character OpenTelemetry trace ID, or an
// empty string for a no-op observation.
func (o *Observation) TraceID() string {
	if o == nil || o.span == nil {
		return ""
	}
	id := o.span.SpanContext().TraceID()
	if !id.IsValid() {
		return ""
	}
	return id.String()
}

// ID returns the lowercase 16-character OpenTelemetry span ID, or an empty
// string for a no-op observation.
func (o *Observation) ID() string {
	if o == nil || o.span == nil {
		return ""
	}
	id := o.span.SpanContext().SpanID()
	if !id.IsValid() {
		return ""
	}
	return id.String()
}

func (o *Observation) applyTraceState(state traceState) {
	if o == nil || o.span == nil {
		return
	}
	spanAttributes := state.attributes()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ended {
		return
	}
	filtered := spanAttributes[:0]
	for _, item := range spanAttributes {
		if _, explicitlySet := o.explicit[string(item.Key)]; !explicitlySet {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) != 0 {
		o.span.SetAttributes(filtered...)
	}
}

func (o *Observation) spanContext() oteltrace.SpanContext {
	if o == nil || o.span == nil {
		return oteltrace.SpanContext{}
	}
	return o.span.SpanContext()
}

func (c *Client) buildObservationAttributes(
	typeName ObservationType,
	values ObservationAttributes,
	starting bool,
	existingMetadata map[string]struct{},
) ([]attribute.KeyValue, map[string]struct{}) {
	result := make([]attribute.KeyValue, 0, 16+min(len(values.Metadata), lfattr.MaxMetadataEntries))
	explicit := make(map[string]struct{})
	if starting {
		// Keep the recognition key first: borrowed providers with low attribute
		// limits must still be able to identify SDK observations.
		result = append(result, attribute.String(lfattr.ObservationTypeKey, string(typeName)))
		explicit[lfattr.ObservationTypeKey] = struct{}{}
	}
	if values.Level != "" {
		if validLevel(values.Level) {
			result = append(result, attribute.String(lfattr.ObservationLevelKey, string(values.Level)))
		} else {
			diagnostic.Report("unsupported observation level omitted")
		}
	}
	if values.StatusMessage != "" {
		result = append(result, attribute.String(lfattr.ObservationStatusMessageKey, values.StatusMessage))
	}
	if values.Version != "" {
		result = append(result, attribute.String(lfattr.VersionKey, values.Version))
		explicit[lfattr.VersionKey] = struct{}{}
	}
	if typeName == TypeGeneration || typeName == TypeEmbedding {
		result = append(result, generationAttributes(typeName, values)...)
	} else if hasGenerationAttributes(values) {
		diagnostic.Report("generation-only attributes omitted from a non-generation observation")
	}
	if !c.disableContentCapture {
		if input, ok := lfattr.Encode(values.Input, c.mask, "observation input"); ok {
			result = append(result, attribute.String(lfattr.ObservationInputKey, input))
		}
		if output, ok := lfattr.Encode(values.Output, c.mask, "observation output"); ok {
			result = append(result, attribute.String(lfattr.ObservationOutputKey, output))
		}
	}
	// Metadata is intentionally last among caller-supplied attributes. The
	// bounded entry count preserves room for generation and processor fields.
	result = append(result, lfattr.ObservationMetadataWithExisting(values.Metadata, c.mask, existingMetadata)...)
	if !starting && !values.StartTime.IsZero() {
		diagnostic.Report("update start time ignored; start time can be set only when the observation starts")
	}
	return result, explicit
}

func generationAttributes(typeName ObservationType, values ObservationAttributes) []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, 8)
	if values.Model != "" {
		result = append(result, attribute.String(lfattr.ObservationModelKey, values.Model))
	}
	if len(values.ModelParameters) != 0 {
		if value, ok := lfattr.JSONMap(values.ModelParameters, "model parameters"); ok {
			result = append(result, attribute.KeyValue{Key: lfattr.ObservationModelParametersKey, Value: value})
		}
	}
	if values.Usage != nil {
		usage := values.Usage
		if encoded, ok := lfattr.NormalizeUsage(
			usage.InputTokens,
			usage.OutputTokens,
			usage.CacheReadInputTokens,
			usage.CacheCreationInputTokens,
			usage.ReasoningOutputTokens,
			usage.Details,
		); ok {
			result = append(result, attribute.String(lfattr.ObservationUsageDetailsKey, encoded))
		}
	}
	if len(values.CostDetails) != 0 {
		if value, ok := lfattr.JSONMap(values.CostDetails, "cost details"); ok {
			result = append(result, attribute.KeyValue{Key: lfattr.ObservationCostDetailsKey, Value: value})
		}
	}
	if !values.CompletionStartTime.IsZero() {
		result = append(result, attribute.String(
			lfattr.ObservationCompletionStartTimeKey,
			values.CompletionStartTime.UTC().Format(time.RFC3339Nano),
		))
	}
	if values.Prompt != nil {
		if typeName != TypeGeneration {
			diagnostic.Report("prompt reference omitted from a non-generation observation")
		} else if values.Prompt.Name == "" || values.Prompt.Version < 1 {
			diagnostic.Report("invalid prompt reference omitted")
		} else {
			result = append(result,
				attribute.String(lfattr.ObservationPromptNameKey, values.Prompt.Name),
				attribute.Int(lfattr.ObservationPromptVersionKey, values.Prompt.Version),
			)
		}
	}
	return result
}

func hasGenerationAttributes(values ObservationAttributes) bool {
	return values.Model != "" || len(values.ModelParameters) != 0 || values.Usage != nil ||
		len(values.CostDetails) != 0 || values.Prompt != nil || !values.CompletionStartTime.IsZero()
}

func normalizeObservationType(value ObservationType) ObservationType {
	if value == "" {
		return TypeSpan
	}
	switch value {
	case TypeSpan, TypeGeneration, TypeEvent, TypeEmbedding, TypeAgent, TypeTool,
		TypeChain, TypeRetriever, TypeEvaluator, TypeGuardrail:
		return value
	default:
		diagnostic.Report("unsupported observation type; using span")
		return TypeSpan
	}
}

func validLevel(value Level) bool {
	switch value {
	case LevelDefault, LevelDebug, LevelWarning, LevelError:
		return true
	default:
		return false
	}
}
