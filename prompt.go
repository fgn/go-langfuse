package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

// PromptSource reports how GetPrompt resolved a prompt.
type PromptSource string

const (
	PromptSourceServer   PromptSource = "server"
	PromptSourceCache    PromptSource = "cache"
	PromptSourceStale    PromptSource = "stale"
	PromptSourceFallback PromptSource = "fallback"
)

// ErrPromptNotFound reports that the requested prompt name, version, or
// label does not exist in the Langfuse project. Test with errors.Is.
var ErrPromptNotFound = errors.New("langfuse: prompt not found")

// ErrPromptTypeMismatch reports that a resolved prompt did not have the type
// requested by PromptQuery.Type. Test with errors.Is.
var ErrPromptTypeMismatch = errors.New("langfuse: prompt type mismatch")

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
	// maxPromptFallbackMessages caps a chat fallback's message count so a
	// pathological slice of tiny messages cannot pass the byte budget and
	// then force a large deep copy. A real chat prompt has far fewer.
	maxPromptFallbackMessages = 4096
	// promptMessageOverhead approximates one chat message's JSON structural
	// cost (braces, quoted keys, separators) so the byte budget reflects the
	// encoded size rather than only field payloads.
	promptMessageOverhead = 64
	defaultPromptLabel    = "production"
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
	// Type requires the resolved prompt to have this shape. Empty accepts
	// either text or chat. A mismatch is resolved through Fallback when set
	// and otherwise returns ErrPromptTypeMismatch.
	Type PromptType
	// CacheTTL overrides the 60-second default freshness window for this
	// call. Zero means the default; negative values are rejected. Freshness
	// is judged against the age of the cached entry at call time, so callers
	// with different TTLs share one entry (a deliberate divergence from the
	// official SDKs, which stamp an expiry when the entry is inserted).
	CacheTTL time.Duration
	// DisableCache bypasses the cache entirely for this call (no read, no
	// write, no shared in-flight fetch), forcing a fresh fetch.
	DisableCache bool
	// Fallback is returned when a fetch fails without a cached value or the
	// resolved prompt does not match Type, so a local prompt can guarantee
	// availability. A fallback result is never cached and Ref on it returns
	// nil, so it is not linked to observations. Fallback never overrides the
	// caller's own context cancellation on a blocking fetch path.
	Fallback *PromptFallback
}

// PromptFallback is a locally supplied prompt body used when a fetch cannot
// resolve a compatible prompt.
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
	// Source reports whether this value was fetched, served from fresh or
	// stale cache, or produced from PromptQuery.Fallback.
	Source PromptSource
}

// Ref returns the reference that links observations to this prompt version,
// for ObservationAttributes.Prompt. It returns nil — safely skipping
// linking — for a fallback prompt and for any value that cannot form a valid
// reference (empty Name or Version <= 0, such as a zero Prompt).
func (p Prompt) Ref() *PromptRef {
	if p.Source == PromptSourceFallback || p.Name == "" || p.Version <= 0 {
		return nil
	}
	return &PromptRef{Name: p.Name, Version: p.Version}
}

// GetPrompt is safe to call on a nil, disabled, or shut-down client; with a
// Fallback set it returns that fallback, so callers need no nil guard.
//
// GetPrompt fetches one prompt version from the Langfuse prompts API,
// serving it from the client-side cache when fresh. An expired entry is
// returned immediately while one background refresh per prompt runs; a cache
// miss fetches synchronously, bounded by ctx and a 10-second fetch budget.
// If the fetch fails for any reason other than ctx ending, query.Fallback is
// returned when set; otherwise the error carries at most the prompt name and
// HTTP status, and wraps ErrPromptNotFound for 404. A result whose type does
// not match query.Type likewise resolves to the fallback or wraps
// ErrPromptTypeMismatch. A nil ctx or invalid query is always an error.
func (c *Client) GetPrompt(ctx context.Context, name string, query PromptQuery) (Prompt, error) {
	// Validation and the nil-context check run before every unavailable-
	// client or fallback short circuit: an invalid query or a nil context is
	// always a synchronous error, never masked by a fallback, in every
	// environment.
	if err := validatePromptQuery(name, query); err != nil {
		return Prompt{}, err
	}
	if ctx == nil {
		return Prompt{}, errors.New("langfuse: prompt context is nil")
	}
	var fallback *promptFallbackValue
	if query.Fallback != nil {
		normalized, err := normalizePromptFallback(*query.Fallback)
		if err != nil {
			return Prompt{}, err
		}
		if query.Type != "" && normalized.promptType != query.Type {
			return Prompt{}, fmt.Errorf(
				"langfuse: prompt %q fallback type %q does not match expected type %q: %w",
				name, normalized.promptType, query.Type, ErrPromptTypeMismatch)
		}
		fallback = &normalized
	}
	if c == nil || c.isDisabled() || c.prompts == nil {
		return promptFromFallback(name, query.Version, fallback,
			errors.New("langfuse: prompt requested on a disabled client"))
	}
	if c.stopped.Load() {
		return promptFromFallback(name, query.Version, fallback,
			errors.New("langfuse: prompt requested after client shutdown"))
	}
	prompt, err := c.prompts.get(ctx, name, query, fallback)
	if err != nil {
		return Prompt{}, err
	}
	if query.Type == "" || prompt.Type == query.Type {
		return prompt, nil
	}
	return promptFromFallback(name, query.Version, fallback,
		promptTypeMismatchError(name, prompt.Type, query.Type))
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
// empty, and Source set to PromptSourceFallback.
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
		Source:   PromptSourceFallback,
	}, nil
}

func promptTypeMismatchError(name string, actual, expected PromptType) error {
	return fmt.Errorf("langfuse: prompt %q has type %q, expected %q: %w",
		name, actual, expected, ErrPromptTypeMismatch)
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
	switch query.Type {
	case "", PromptTypeText, PromptTypeChat:
	default:
		return errors.New("langfuse: unsupported expected prompt type")
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
	if len(fallback.Messages) > maxPromptFallbackMessages {
		return promptFallbackValue{}, errors.New("langfuse: prompt fallback carries too many messages")
	}
	// Gate on an in-memory size budget — raw field bytes plus per-message
	// structural overhead — before any O(n) UTF-8 or JSON scan, so an
	// oversized fallback cannot burn validation CPU or force a large copy on
	// the request path. It is a conservative in-memory bound, not the exact
	// JSON-escaped wire size, and is paired with the message-count cap above.
	size := len(fallback.Text) + len(fallback.Config)
	for _, message := range fallback.Messages {
		size += promptMessageOverhead + len(message.Role) + len(message.Content) +
			len(message.PlaceholderName) + len(message.Extra)
		if size > maxPromptBodyBytes {
			return promptFallbackValue{}, errors.New("langfuse: prompt fallback exceeds the 1 MiB limit")
		}
	}
	// The input is now bounded; validate shapes and UTF-8.
	if !utf8.ValidString(fallback.Text) {
		return promptFallbackValue{}, errors.New("langfuse: prompt fallback text is not valid UTF-8")
	}
	if len(fallback.Config) > 0 {
		if !json.Valid(fallback.Config) || !utf8.Valid(fallback.Config) {
			return promptFallbackValue{}, errors.New("langfuse: prompt fallback config must be valid UTF-8 JSON")
		}
	}
	for _, message := range fallback.Messages {
		if err := validPromptMessage(message); err != nil {
			return promptFallbackValue{}, err
		}
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
		if !strings.HasPrefix(trimmed, "{") || !json.Valid(message.Extra) || !utf8.Valid(message.Extra) {
			return errors.New("langfuse: prompt message extra must be one valid UTF-8 JSON object")
		}
	}
	return nil
}

// DecodeConfig decodes Config into v. An absent config is a no-op, allowing
// callers to set defaults on v before decoding prompt-owned overrides.
func (p Prompt) DecodeConfig(v any) error {
	if len(p.Config) == 0 {
		return nil
	}
	if err := json.Unmarshal(p.Config, v); err != nil {
		return fmt.Errorf("langfuse: decode prompt config: %w", err)
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
	result, _ := p.compile(vars, nil)
	return result
}

// CompileStrict compiles the same copy as Compile and also reports missing
// content variables, values that cannot be stringified, and unfilled chat
// placeholders. The returned prompt contains every successful substitution
// even when the error is non-nil.
func (p Prompt) CompileStrict(vars map[string]any) (Prompt, error) {
	problems := &promptCompileProblems{}
	return p.compile(vars, problems)
}

func (p Prompt) compile(vars map[string]any, problems *promptCompileProblems) (Prompt, error) {
	result := deepCopyPrompt(p)
	if len(vars) == 0 && problems == nil {
		return result, nil
	}
	result.Text = substitutePromptVariables(result.Text, vars, problems)
	if result.Messages == nil {
		return result, problems.err()
	}
	compiled := make([]PromptMessage, 0, len(result.Messages))
	for _, message := range result.Messages {
		if message.PlaceholderName != "" {
			if fill, ok := promptPlaceholderFill(vars[message.PlaceholderName]); ok {
				compiled = append(compiled, deepCopyPromptMessages(fill)...)
			} else {
				compiled = append(compiled, message)
				problems.addUnfilledPlaceholder(message.PlaceholderName)
			}
			continue
		}
		message.Content = substitutePromptVariables(message.Content, vars, problems)
		compiled = append(compiled, message)
	}
	result.Messages = compiled
	return result, problems.err()
}

type promptCompileProblems struct {
	missingVariables      map[string]struct{}
	unstringifiableValues map[string]struct{}
	unfilledPlaceholders  map[string]struct{}
}

func (p *promptCompileProblems) addMissingVariable(name string) {
	if p == nil {
		return
	}
	if p.missingVariables == nil {
		p.missingVariables = make(map[string]struct{})
	}
	p.missingVariables[name] = struct{}{}
}

func (p *promptCompileProblems) addUnstringifiableValue(name string) {
	if p == nil {
		return
	}
	if p.unstringifiableValues == nil {
		p.unstringifiableValues = make(map[string]struct{})
	}
	p.unstringifiableValues[name] = struct{}{}
}

func (p *promptCompileProblems) addUnfilledPlaceholder(name string) {
	if p == nil {
		return
	}
	if p.unfilledPlaceholders == nil {
		p.unfilledPlaceholders = make(map[string]struct{})
	}
	p.unfilledPlaceholders[name] = struct{}{}
}

func (p *promptCompileProblems) err() error {
	if p == nil {
		return nil
	}
	parts := make([]string, 0, 3)
	if len(p.missingVariables) > 0 {
		parts = append(parts, fmt.Sprintf("missing variables %q", sortedPromptIdentifiers(p.missingVariables)))
	}
	if len(p.unstringifiableValues) > 0 {
		parts = append(parts, fmt.Sprintf("unstringifiable variables %q", sortedPromptIdentifiers(p.unstringifiableValues)))
	}
	if len(p.unfilledPlaceholders) > 0 {
		parts = append(parts, fmt.Sprintf("unfilled placeholders %q", sortedPromptIdentifiers(p.unfilledPlaceholders)))
	}
	if len(parts) == 0 {
		return nil
	}
	return errors.New("langfuse: strict prompt compile failed: " + strings.Join(parts, "; "))
}

func sortedPromptIdentifiers(set map[string]struct{}) []string {
	identifiers := make([]string, 0, len(set))
	for identifier := range set {
		identifiers = append(identifiers, identifier)
	}
	slices.Sort(identifiers)
	return identifiers
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
func substitutePromptVariables(text string, vars map[string]any, problems *promptCompileProblems) string {
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
			if name != "" {
				problems.addMissingVariable(name)
			}
			continue
		}
		if replacement, ok := promptVariableString(value); ok {
			builder.WriteString(replacement)
		} else {
			builder.WriteString(token)
			problems.addUnstringifiableValue(name)
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
