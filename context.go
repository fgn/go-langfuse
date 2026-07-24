package langfuse

import (
	"context"
	"maps"
	"sort"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/diagnostic"
)

const (
	maxTraceTags     = 64
	maxTraceTagBytes = 16 << 10
)

// A detached context (the ambient span context cleared with the standard
// OpenTelemetry helper so background work starts a new trace) needs no
// cleanup of these keys: a fresh random trace ID cannot match a stale
// application-root claim, and the active-observation pointer is only
// consulted while the ambient span context still matches that observation.
type (
	traceStateContextKey     struct{ client *Client }
	observationContextKey    struct{ client *Client }
	traceClaimContextKey     struct{ client *Client }
	sampleRateContextKey     struct{ client *Client }
	admissionTokenContextKey struct{ client *Client }
)

type traceState struct {
	name        string
	userID      string
	sessionID   string
	tags        []string
	tagBytes    int
	metadata    map[string]string
	version     string
	environment string
	// wireMetadata marks metadata keys whose value was a string AFTER
	// masking; only those are eligible for baggage export, because
	// JSON-encoded non-string values have no cross-SDK wire contract.
	wireMetadata map[string]struct{}
	// accepted marks fields and metadata keys whose CURRENT value came
	// from imported baggage rather than a local call. A later import may
	// replace an accepted value with the current namespace's value, but
	// never a local one: local > accepted baggage, per field, across
	// repeated imports.
	accepted map[string]struct{}
}

// Origin keys for traceState.accepted.
const (
	acceptedName        = "name"
	acceptedUserID      = "user"
	acceptedSessionID   = "session"
	acceptedVersion     = "version"
	acceptedEnvironment = "environment"
	acceptedMetadata    = "metadata:"
)

// traceClaim is the client-scoped application-root claim state. The
// imported origin bit lets a later import replace or clear authority
// that arrived in older baggage without ever touching a claim
// installed by a locally started root.
type traceClaim struct {
	id       oteltrace.TraceID
	imported bool
}

// WithTraceAttributes returns a context that propagates request-scoped trace
// fields to observations subsequently started from it. If an SDK observation
// belonging to this Client is active, it is updated immediately as well.
func (c *Client) WithTraceAttributes(ctx context.Context, values TraceAttributes) context.Context {
	if c == nil || c.isDisabled() || ctx == nil {
		return ctx
	}
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx
	}
	previous, _ := ctx.Value(traceStateContextKey{client: c}).(traceState)
	next := previous.clone()
	next.merge(values, c.mask)
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx
	}

	result := context.WithValue(ctx, traceStateContextKey{client: c}, next)
	if observation, _ := ctx.Value(observationContextKey{client: c}).(*Observation); observation != nil {
		active := oteltrace.SpanFromContext(ctx).SpanContext()
		owned := observation.spanContext()
		if active.IsValid() && active.TraceID() == owned.TraceID() && active.SpanID() == owned.SpanID() {
			observation.applyTraceState(next)
		}
	}
	if values.Environment != "" && next.environment == values.Environment {
		// The request-scoped environment also updates the current recording
		// span — an already-started borrowed server span included — matching
		// the official SDKs' propagate-attributes semantics.
		stampEnvironmentAttribute(ctx, next.environment)
	}
	return c.syncBaggage(result, false, false)
}

// WithSampleRate returns a context that overrides the configured sample rate
// for the sampling decision made by the first SDK observation subsequently
// started on this context path. The fraction must be finite and within
// [0, 1]; 0 exports nothing, 1 exports everything. The decision is inherited
// by every SDK observation started from that observation's returned context
// (and inside its [Client.Observe] callback), so a rate set later on that
// path has no effect on the already-decided trace. Sibling observations
// started independently from a context that carries no decision yet
// re-decide deterministically from the trace ID and their own effective
// rate: with equal rates they always agree; setting different rates inside
// one trace makes trace membership subtree-scoped. Set the rate once per
// request, before the first observation, unless subtree-scoped rates are
// intended. An invalid fraction is ignored with a diagnostic, as is any call
// on a borrowed-provider client, where the application's sampler remains
// authoritative.
func (c *Client) WithSampleRate(ctx context.Context, fraction float64) context.Context {
	if c == nil || c.isDisabled() || ctx == nil {
		return ctx
	}
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx
	}
	if !c.owned {
		if c.borrowedRateWarning.CompareAndSwap(false, true) {
			diagnostic.Report("sample rate is ignored on a borrowed tracer provider; the application's sampler remains authoritative")
		}
		return ctx
	}
	if !validSampleFraction(fraction) {
		diagnostic.Report("sample rate fraction is not finite or is outside [0, 1]; value ignored")
		return ctx
	}
	return context.WithValue(ctx, sampleRateContextKey{client: c}, fraction)
}

func (s traceState) clone() traceState {
	result := s
	result.tags = append([]string(nil), s.tags...)
	if s.metadata != nil {
		result.metadata = make(map[string]string, len(s.metadata))
		maps.Copy(result.metadata, s.metadata)
	}
	if s.wireMetadata != nil {
		result.wireMetadata = make(map[string]struct{}, len(s.wireMetadata))
		maps.Copy(result.wireMetadata, s.wireMetadata)
	}
	if s.accepted != nil {
		result.accepted = make(map[string]struct{}, len(s.accepted))
		maps.Copy(result.accepted, s.accepted)
	}
	return result
}

// markAccepted records baggage origin for a field; clearAccepted
// restores local precedence when a local call sets it.
func (s *traceState) markAccepted(key string) {
	if s.accepted == nil {
		s.accepted = make(map[string]struct{})
	}
	s.accepted[key] = struct{}{}
}

func (s *traceState) isAccepted(key string) bool {
	_, found := s.accepted[key]
	return found
}

func (s *traceState) merge(values TraceAttributes, mask func(any) any) {
	if value := propagatedString("trace name", values.Name); value != "" {
		s.name = value
		delete(s.accepted, acceptedName)
	}
	if value := propagatedString("user ID", values.UserID); value != "" {
		s.userID = value
		delete(s.accepted, acceptedUserID)
	}
	if value := propagatedString("session ID", values.SessionID); value != "" {
		s.sessionID = value
		delete(s.accepted, acceptedSessionID)
	}
	if value := propagatedString("version", values.Version); value != "" {
		s.version = value
		delete(s.accepted, acceptedVersion)
	}
	if values.Environment != "" {
		if err := validateEnvironment(values.Environment); err != nil {
			diagnostic.Report("propagated environment is invalid; value ignored")
		} else {
			s.environment = values.Environment
			delete(s.accepted, acceptedEnvironment)
		}
	}
	if len(values.Tags) != 0 {
		seenCapacity := len(s.tags) + min(len(values.Tags), maxTraceTags-len(s.tags))
		seen := make(map[string]struct{}, seenCapacity)
		for _, tag := range s.tags {
			seen[tag] = struct{}{}
		}
		truncated := false
		for _, tag := range values.Tags {
			tag = propagatedString("tag", tag)
			if tag == "" {
				continue
			}
			if _, exists := seen[tag]; exists {
				continue
			}
			if len(s.tags) >= maxTraceTags || s.tagBytes+len(tag) > maxTraceTagBytes {
				truncated = true
				continue
			}
			s.tags = append(s.tags, tag)
			s.tagBytes += len(tag)
			seen[tag] = struct{}{}
		}
		if truncated {
			diagnostic.Report("trace tags exceed the lifetime count or byte limit; remaining tags omitted")
		}
	}
	metadata, stringKeys := lfattr.TraceMetadataWithExisting(values.Metadata, mask, s.metadata)
	if len(metadata) != 0 {
		if s.metadata == nil {
			s.metadata = make(map[string]string, len(metadata))
		}
		keys := make([]string, 0, len(metadata))
		for key := range metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		truncated := false
		for _, key := range keys {
			if _, exists := s.metadata[key]; !exists && len(s.metadata) >= lfattr.MaxMetadataEntries {
				truncated = true
				continue
			}
			s.metadata[key] = metadata[key]
			delete(s.accepted, acceptedMetadata+key)
			// Wire eligibility follows the POST-mask value type: a mask
			// that turns a string into a structured value removes the
			// cross-SDK wire representation.
			if stringKeys[key] {
				if s.wireMetadata == nil {
					s.wireMetadata = make(map[string]struct{})
				}
				s.wireMetadata[key] = struct{}{}
			} else {
				delete(s.wireMetadata, key)
			}
		}
		if truncated {
			diagnostic.Report("trace metadata exceeds the lifetime entry limit; new entries omitted")
		}
	}
}

// mergeImportedMetadata applies baggage-accepted metadata values, which
// are strings by wire construction, through the same normalization and
// mask path as local metadata, exactly once. The caller passes only
// keys that are absent locally or whose current value is itself
// baggage-accepted; those are replaced from the current namespace and
// re-marked, so a later import updates earlier imports while local
// values keep winning.
func (s *traceState) mergeImportedMetadata(imported map[string]any, mask func(any) any, hasNamespace bool) {
	normalized, stringKeys := lfattr.TraceMetadataWithExisting(imported, mask, s.metadata)
	if hasNamespace {
		// Retire accepted metadata keys the current namespace does not
		// re-confirm (post-mask identity): the accepted layer is a
		// projection of this namespace, never an accumulation.
		for originKey := range s.accepted {
			suffix, isMetadata := strings.CutPrefix(originKey, acceptedMetadata)
			if !isMetadata {
				continue
			}
			if _, kept := normalized[suffix]; kept {
				continue
			}
			delete(s.metadata, suffix)
			delete(s.wireMetadata, suffix)
			delete(s.accepted, originKey)
		}
	}
	if len(normalized) == 0 {
		return
	}
	if s.metadata == nil {
		s.metadata = make(map[string]string, len(normalized))
	}
	keys := make([]string, 0, len(normalized))
	for key := range normalized {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	truncated := false
	for _, key := range keys {
		// The mask may remap keys: a post-mask key landing on a locally
		// originated entry must not clobber it (local wins per key).
		if _, exists := s.metadata[key]; exists && !s.isAccepted(acceptedMetadata+key) {
			continue
		}
		if _, exists := s.metadata[key]; !exists && len(s.metadata) >= lfattr.MaxMetadataEntries {
			truncated = true
			continue
		}
		s.metadata[key] = normalized[key]
		s.markAccepted(acceptedMetadata + key)
		if stringKeys[key] {
			if s.wireMetadata == nil {
				s.wireMetadata = make(map[string]struct{})
			}
			s.wireMetadata[key] = struct{}{}
		} else {
			delete(s.wireMetadata, key)
		}
	}
	if truncated {
		diagnostic.Report("trace metadata exceeds the lifetime entry limit; imported entries omitted")
	}
}

func propagatedString(field, value string) string {
	if value == "" {
		return ""
	}
	if !utf8.ValidString(value) {
		diagnostic.Report("propagated " + field + " is not valid UTF-8; value omitted")
		return ""
	}
	if utf8.RuneCountInString(value) > 200 {
		diagnostic.Report("propagated " + field + " exceeds 200 characters; value omitted")
		return ""
	}
	return value
}

func (c *Client) propagatedAttributes(ctx context.Context) []attribute.KeyValue {
	if c == nil || ctx == nil {
		return nil
	}
	state, _ := ctx.Value(traceStateContextKey{client: c}).(traceState)
	return state.attributes()
}

func (s traceState) attributes() []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, 5+len(s.metadata))
	if s.name != "" {
		result = append(result, attribute.String(lfattr.TraceNameKey, s.name))
	}
	if s.userID != "" {
		result = append(result, attribute.String(lfattr.TraceUserIDKey, s.userID))
	}
	if s.sessionID != "" {
		result = append(result, attribute.String(lfattr.TraceSessionIDKey, s.sessionID))
	}
	if len(s.tags) != 0 {
		result = append(result, attribute.StringSlice(lfattr.TraceTagsKey, append([]string(nil), s.tags...)))
	}
	keys := make([]string, 0, len(s.metadata))
	for key := range s.metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result = append(result, attribute.String(lfattr.TraceMetadataKey+"."+key, s.metadata[key]))
	}
	if s.version != "" {
		result = append(result, attribute.String(lfattr.VersionKey, s.version))
	}
	if s.environment != "" {
		result = append(result, attribute.String(lfattr.EnvironmentKey, s.environment))
	}
	return result
}

func (c *Client) hasTraceClaim(ctx context.Context, traceID oteltrace.TraceID) bool {
	if c == nil || ctx == nil || !traceID.IsValid() {
		return false
	}
	claim, _ := ctx.Value(traceClaimContextKey{client: c}).(traceClaim)
	return claim.id == traceID
}

func (c *Client) withTraceClaim(ctx context.Context, traceID oteltrace.TraceID) context.Context {
	if ctx == nil || !traceID.IsValid() {
		return ctx
	}
	return context.WithValue(ctx, traceClaimContextKey{client: c}, traceClaim{id: traceID})
}

func (c *Client) traceClaimState(ctx context.Context) traceClaim {
	claim, _ := ctx.Value(traceClaimContextKey{client: c}).(traceClaim)
	return claim
}

func (c *Client) withImportedTraceClaim(ctx context.Context, traceID oteltrace.TraceID) context.Context {
	return context.WithValue(ctx, traceClaimContextKey{client: c},
		traceClaim{id: traceID, imported: true})
}

// withClearedTraceClaim removes previously imported claim authority; a
// claim installed by a locally started root is never cleared this way.
func (c *Client) withClearedTraceClaim(ctx context.Context) context.Context {
	return context.WithValue(ctx, traceClaimContextKey{client: c}, traceClaim{})
}
