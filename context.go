package lunte

import (
	"context"
	"sort"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/lunte/internal/attributes"
	"github.com/fgn/lunte/internal/diagnostic"
)

const (
	maxTraceTags     = 64
	maxTraceTagBytes = 16 << 10
)

type traceStateContextKey struct{ client *Client }
type observationContextKey struct{ client *Client }
type traceClaimContextKey struct{ client *Client }

type traceState struct {
	name      string
	userID    string
	sessionID string
	tags      []string
	tagBytes  int
	metadata  map[string]string
	version   string
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
	return result
}

func (s traceState) clone() traceState {
	result := s
	result.tags = append([]string(nil), s.tags...)
	if s.metadata != nil {
		result.metadata = make(map[string]string, len(s.metadata))
		for key, value := range s.metadata {
			result.metadata[key] = value
		}
	}
	return result
}

func (s *traceState) merge(values TraceAttributes, mask func(any) any) {
	if value := propagatedString("trace name", values.Name); value != "" {
		s.name = value
	}
	if value := propagatedString("user ID", values.UserID); value != "" {
		s.userID = value
	}
	if value := propagatedString("session ID", values.SessionID); value != "" {
		s.sessionID = value
	}
	if value := propagatedString("version", values.Version); value != "" {
		s.version = value
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
	metadata := lfattr.TraceMetadataWithExisting(values.Metadata, mask, s.metadata)
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
		}
		if truncated {
			diagnostic.Report("trace metadata exceeds the lifetime entry limit; new entries omitted")
		}
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
	return result
}

func (c *Client) hasTraceClaim(ctx context.Context, traceID oteltrace.TraceID) bool {
	if c == nil || ctx == nil || !traceID.IsValid() {
		return false
	}
	claim, _ := ctx.Value(traceClaimContextKey{client: c}).(oteltrace.TraceID)
	return claim == traceID
}

func (c *Client) withTraceClaim(ctx context.Context, traceID oteltrace.TraceID) context.Context {
	if ctx == nil || !traceID.IsValid() {
		return ctx
	}
	return context.WithValue(ctx, traceClaimContextKey{client: c}, traceID)
}
