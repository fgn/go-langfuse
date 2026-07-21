package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fgn/go-langfuse/internal/transport"
)

// PromptType discriminates text and chat prompts.
type PromptType string

const (
	PromptTypeText PromptType = "text"
	PromptTypeChat PromptType = "chat"
)

// ErrPromptNotFound reports that the requested prompt name, version, or
// label does not exist in the Langfuse project. Test with errors.Is.
var ErrPromptNotFound = errors.New("langfuse: prompt not found")

const (
	defaultPromptCacheTTL = time.Minute
	// promptFetchBudget bounds one fetch end to end: every attempt, backoff,
	// and Retry-After wait. The synchronous path is additionally bounded by
	// the caller's context.
	promptFetchBudget = 10 * time.Second
	// maxPromptNameBytes and maxPromptLabelChars are local wire-safety
	// bounds, not Langfuse validation (the server is stricter for labels).
	// Prompt names may contain '/' folder separators.
	maxPromptNameBytes  = 500
	maxPromptLabelChars = 200
	maxPromptBodyBytes  = 1 << 20
	defaultPromptLabel  = "production"
)

// PromptQuery selects a prompt version and controls caching. The zero value
// requests the "production"-labeled version with the default 60-second TTL.
type PromptQuery struct {
	// Version selects an exact prompt version. Zero means "use Label";
	// negative values are rejected.
	Version int
	// Label selects the deployment label; empty means "production".
	// Version and Label are mutually exclusive.
	Label string
	// CacheTTL overrides the 60-second default freshness window for this
	// call. Zero means the default; negative values are rejected. Freshness
	// is judged against the age of the cached entry at call time, so callers
	// with different TTLs share one entry (a deliberate divergence from the
	// official SDKs, which stamp an expiry when the entry is inserted).
	CacheTTL time.Duration
	// DisableCache bypasses the cache entirely for this call (no read, no
	// write, no shared in-flight fetch), forcing a fresh fetch.
	DisableCache bool
	// Fallback is returned when the fetch fails and no cached value exists,
	// so a hardcoded prompt can guarantee availability. A fallback result is
	// never cached and Ref on it returns nil, so it is not linked to
	// observations. Fallback never overrides the caller's own context
	// cancellation.
	Fallback *PromptFallback
}

// PromptFallback is a locally supplied prompt body used when Langfuse is
// unreachable and nothing is cached.
type PromptFallback struct {
	// Type declares the prompt shape. Empty infers chat when Messages is
	// non-nil (an empty non-nil slice is a deliberate empty chat prompt) and
	// text otherwise. Explicit text requires Messages == nil; explicit chat
	// requires Text == "".
	Type PromptType
	// Text is the text prompt body.
	Text string
	// Messages is the chat prompt body.
	Messages []PromptMessage
	// Config is surfaced through Prompt.Config; when non-empty it must be
	// valid JSON.
	Config json.RawMessage
}

// PromptMessage is one chat prompt message. A regular message sets Role
// (required) and Content (optional — tool-call messages may have empty
// content); a placeholder message sets only PlaceholderName, a named slot
// filled at Compile time.
type PromptMessage struct {
	Role    string
	Content string
	// PlaceholderName marks this message as a named placeholder slot; when
	// set, Role, Content, and Extra must be empty.
	PlaceholderName string
	// Extra preserves additional message fields beyond role and content (for
	// example tool_calls) verbatim, as one JSON object. Compile never
	// substitutes inside Extra.
	Extra json.RawMessage
}

// Prompt is one fetched prompt version. GetPrompt returns an independent
// deep copy — the SDK's cached master is never exposed — so callers may
// retain or modify the value freely.
type Prompt struct {
	Name          string
	Version       int
	Type          PromptType
	Text          string
	Messages      []PromptMessage
	Config        json.RawMessage
	Labels        []string
	Tags          []string
	CommitMessage string
	// Fallback reports that this value came from PromptQuery.Fallback rather
	// than Langfuse.
	Fallback bool
}

// Ref returns the reference that links observations to this prompt version,
// for ObservationAttributes.Prompt. It returns nil — safely skipping
// linking — for a fallback prompt and for any value that cannot form a valid
// reference (empty Name or Version <= 0, such as a zero Prompt).
func (p Prompt) Ref() *PromptRef {
	if p.Fallback || p.Name == "" || p.Version <= 0 {
		return nil
	}
	return &PromptRef{Name: p.Name, Version: p.Version}
}

// GetPrompt fetches one prompt version from the Langfuse prompts API,
// serving it from the client-side cache when fresh. An expired entry is
// returned immediately while one background refresh per prompt runs; a cache
// miss fetches synchronously, bounded by ctx and a 10-second fetch budget.
// If the fetch fails for any reason other than ctx ending, query.Fallback is
// returned when set; otherwise the error carries at most the prompt name and
// HTTP status, and wraps ErrPromptNotFound for 404. A nil, disabled, or
// shut-down client returns the fallback when set and an error otherwise; a
// nil ctx or invalid query is always an error.
func (c *Client) GetPrompt(ctx context.Context, name string, query PromptQuery) (Prompt, error) {
	// Validation runs before the disabled-client short circuit so a broken
	// query fails deterministically in every environment.
	if err := validatePromptQuery(name, query); err != nil {
		return Prompt{}, err
	}
	var fallback *promptFallbackValue
	if query.Fallback != nil {
		normalized, err := normalizePromptFallback(*query.Fallback)
		if err != nil {
			return Prompt{}, err
		}
		fallback = &normalized
	}
	if c == nil || c.isDisabled() || c.prompts == nil {
		return promptFromFallback(name, query.Version, fallback,
			errors.New("langfuse: prompt requested on a disabled client"))
	}
	if ctx == nil {
		return Prompt{}, errors.New("langfuse: prompt context is nil")
	}
	if c.stopped.Load() {
		return promptFromFallback(name, query.Version, fallback,
			errors.New("langfuse: prompt requested after client shutdown"))
	}
	return c.prompts.get(ctx, name, query, fallback)
}

// promptFallbackValue is a validated, deeply copied PromptFallback with its
// effective type resolved.
type promptFallbackValue struct {
	promptType PromptType
	text       string
	messages   []PromptMessage
	config     json.RawMessage
}

// promptFromFallback resolves a failed or unavailable fetch: the validated
// fallback when one was supplied, the cause otherwise. The projection fixes
// every Prompt field: name and exact version from the request (zero for a
// label query), body and config from the fallback, server-owned metadata
// empty, and Fallback true.
func promptFromFallback(name string, version int, fallback *promptFallbackValue, cause error) (Prompt, error) {
	if fallback == nil {
		return Prompt{}, cause
	}
	return Prompt{
		Name:     name,
		Version:  version,
		Type:     fallback.promptType,
		Text:     fallback.text,
		Messages: fallback.messages,
		Config:   fallback.config,
		Fallback: true,
	}, nil
}

func validatePromptQuery(name string, query PromptQuery) error {
	if name == "" || !utf8.ValidString(name) || len(name) > maxPromptNameBytes {
		return errors.New("langfuse: prompt name must be non-empty valid UTF-8 of at most 500 bytes")
	}
	if query.Version < 0 {
		return errors.New("langfuse: prompt version must not be negative")
	}
	if query.Version > 0 && query.Label != "" {
		return errors.New("langfuse: prompt version and label are mutually exclusive")
	}
	if !utf8.ValidString(query.Label) || utf8.RuneCountInString(query.Label) > maxPromptLabelChars {
		return errors.New("langfuse: prompt label must be valid UTF-8 of at most 200 characters")
	}
	if query.CacheTTL < 0 {
		return errors.New("langfuse: prompt cache TTL must not be negative")
	}
	return nil
}

// normalizePromptFallback validates the fallback against the frozen message
// and shape invariants and deep-copies it, so later caller mutation cannot
// race a slow fetch.
func normalizePromptFallback(fallback PromptFallback) (promptFallbackValue, error) {
	promptType := fallback.Type
	switch promptType {
	case "":
		if fallback.Messages != nil {
			promptType = PromptTypeChat
		} else {
			promptType = PromptTypeText
		}
	case PromptTypeText, PromptTypeChat:
	default:
		return promptFallbackValue{}, errors.New("langfuse: unsupported prompt fallback type")
	}
	if promptType == PromptTypeText && fallback.Messages != nil {
		return promptFallbackValue{}, errors.New("langfuse: a text prompt fallback must not carry messages")
	}
	if promptType == PromptTypeChat && fallback.Text != "" {
		return promptFallbackValue{}, errors.New("langfuse: a chat prompt fallback must not carry text")
	}
	size := len(fallback.Text) + len(fallback.Config)
	if !utf8.ValidString(fallback.Text) {
		return promptFallbackValue{}, errors.New("langfuse: prompt fallback text is not valid UTF-8")
	}
	for _, message := range fallback.Messages {
		if err := validPromptMessage(message); err != nil {
			return promptFallbackValue{}, err
		}
		size += len(message.Role) + len(message.Content) + len(message.PlaceholderName) + len(message.Extra)
	}
	if len(fallback.Config) > 0 && !json.Valid(fallback.Config) {
		return promptFallbackValue{}, errors.New("langfuse: prompt fallback config is not valid JSON")
	}
	if size > maxPromptBodyBytes {
		return promptFallbackValue{}, errors.New("langfuse: prompt fallback exceeds the 1 MiB limit")
	}
	return promptFallbackValue{
		promptType: promptType,
		text:       fallback.Text,
		messages:   deepCopyPromptMessages(fallback.Messages),
		config:     clonePromptRaw(fallback.Config),
	}, nil
}

// validPromptMessage enforces the frozen PromptMessage contract: a
// placeholder sets only PlaceholderName; a regular message requires Role,
// allows empty Content, and restricts a non-empty Extra to one JSON object.
func validPromptMessage(message PromptMessage) error {
	if message.PlaceholderName != "" {
		if message.Role != "" || message.Content != "" || len(message.Extra) != 0 {
			return errors.New("langfuse: a placeholder prompt message must set only PlaceholderName")
		}
		if !utf8.ValidString(message.PlaceholderName) {
			return errors.New("langfuse: prompt message placeholder name is not valid UTF-8")
		}
		return nil
	}
	if message.Role == "" {
		return errors.New("langfuse: prompt message role is required")
	}
	if !utf8.ValidString(message.Role) || !utf8.ValidString(message.Content) {
		return errors.New("langfuse: prompt message strings must be valid UTF-8")
	}
	if len(message.Extra) > 0 {
		trimmed := strings.TrimSpace(string(message.Extra))
		if !strings.HasPrefix(trimmed, "{") || !json.Valid(message.Extra) {
			return errors.New("langfuse: prompt message extra must be one JSON object")
		}
	}
	return nil
}

// Compile substitutes {{variable}} occurrences in the prompt content and
// returns the compiled copy; the receiver is unchanged. String values
// substitute verbatim; any other value is JSON-encoded, and a conversion
// that fails or panics leaves that occurrence unresolved — Compile never
// fails and never panics. For chat prompts, a variable whose value is
// []PromptMessage fills the placeholder of that name: an empty slice removes
// the placeholder, and a slice containing any invalid message leaves the
// placeholder unchanged (never a partial splice). Unresolved variables and
// unfilled placeholders stay verbatim, matching the Python SDK; non-string
// values use JSON rather than Python's str().
func (p Prompt) Compile(vars map[string]any) Prompt {
	result := deepCopyPrompt(p)
	if len(vars) == 0 {
		return result
	}
	result.Text = substitutePromptVariables(result.Text, vars)
	if result.Messages == nil {
		return result
	}
	compiled := make([]PromptMessage, 0, len(result.Messages))
	for _, message := range result.Messages {
		if message.PlaceholderName != "" {
			if fill, ok := promptPlaceholderFill(vars[message.PlaceholderName]); ok {
				compiled = append(compiled, deepCopyPromptMessages(fill)...)
			} else {
				compiled = append(compiled, message)
			}
			continue
		}
		message.Content = substitutePromptVariables(message.Content, vars)
		compiled = append(compiled, message)
	}
	result.Messages = compiled
	return result
}

// promptPlaceholderFill accepts a []PromptMessage variable whose every
// message is valid; anything else leaves the placeholder unchanged, so a
// splice is atomic.
func promptPlaceholderFill(value any) ([]PromptMessage, bool) {
	messages, ok := value.([]PromptMessage)
	if !ok {
		return nil, false
	}
	for _, message := range messages {
		if validPromptMessage(message) != nil {
			return nil, false
		}
	}
	return messages, true
}

// substitutePromptVariables replaces {{name}} (inner whitespace allowed) in
// a single pass. Unresolved or empty names, unterminated braces, and values
// that cannot be stringified are left verbatim.
func substitutePromptVariables(text string, vars map[string]any) string {
	open := strings.Index(text, "{{")
	if open < 0 {
		return text
	}
	var builder strings.Builder
	builder.Grow(len(text))
	rest := text
	for {
		open = strings.Index(rest, "{{")
		if open < 0 {
			builder.WriteString(rest)
			return builder.String()
		}
		builder.WriteString(rest[:open])
		closing := strings.Index(rest[open+2:], "}}")
		if closing < 0 {
			builder.WriteString(rest[open:])
			return builder.String()
		}
		token := rest[open : open+2+closing+2]
		name := strings.TrimSpace(rest[open+2 : open+2+closing])
		rest = rest[open+2+closing+2:]
		value, exists := vars[name]
		if name == "" || !exists {
			builder.WriteString(token)
			continue
		}
		if replacement, ok := promptVariableString(value); ok {
			builder.WriteString(replacement)
		} else {
			builder.WriteString(token)
		}
	}
}

// promptVariableString stringifies one variable: strings verbatim, message
// slices never (they are placeholder fills), everything else JSON. The
// recover isolates a panicking caller-supplied Marshaler so Compile keeps
// its never-fails contract.
func promptVariableString(value any) (result string, ok bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []PromptMessage:
		return "", false
	}
	defer func() {
		if recover() != nil {
			result, ok = "", false
		}
	}()
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func deepCopyPrompt(p Prompt) Prompt {
	result := p
	result.Messages = deepCopyPromptMessages(p.Messages)
	result.Labels = clonePromptStrings(p.Labels)
	result.Tags = clonePromptStrings(p.Tags)
	result.Config = clonePromptRaw(p.Config)
	return result
}

func deepCopyPromptMessages(messages []PromptMessage) []PromptMessage {
	if messages == nil {
		return nil
	}
	result := make([]PromptMessage, len(messages))
	for i, message := range messages {
		message.Extra = clonePromptRaw(message.Extra)
		result[i] = message
	}
	return result
}

func clonePromptStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string(nil), values...)
}

func clonePromptRaw(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

// promptFromWire converts a validated transport prompt into the public shape
// used as the immutable cache master.
func promptFromWire(wire transport.Prompt) Prompt {
	prompt := Prompt{
		Name:          wire.Name,
		Version:       wire.Version,
		Type:          PromptType(wire.Type),
		Text:          wire.Text,
		Config:        wire.Config,
		Labels:        wire.Labels,
		Tags:          wire.Tags,
		CommitMessage: wire.CommitMessage,
	}
	if wire.Type == string(PromptTypeChat) {
		messages := make([]PromptMessage, len(wire.Messages))
		for i, message := range wire.Messages {
			messages[i] = PromptMessage{
				Role:            message.Role,
				Content:         message.Content,
				PlaceholderName: message.PlaceholderName,
				Extra:           message.Extra,
			}
		}
		prompt.Messages = messages
	}
	return prompt
}

// wrapPromptError attaches the caller-owned prompt name and maps the
// transport 404 sentinel onto the exported one. Transport error text is
// static words plus numeric statuses, so the result stays payload-free.
func wrapPromptError(name string, err error) error {
	if errors.Is(err, transport.ErrPromptNotFound) {
		return fmt.Errorf("langfuse: prompt %q: %w", name, ErrPromptNotFound)
	}
	return fmt.Errorf("langfuse: prompt %q: %w", name, err)
}
