package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// PromptType is the prompt's wire discriminator.
type PromptType string

const (
	PromptText PromptType = "text"
	PromptChat PromptType = "chat"
)

// ChatMessage is an ordinary message. Roles are not restricted to a closed enum.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// PromptPlaceholder names a message-list placeholder, not a text variable.
type PromptPlaceholder struct {
	Name string `json:"name"`
}

// ChatEntry is exactly one ordinary chat message or message-list placeholder.
// MarshalJSON emits the official chatmessage/placeholder discriminator.
type ChatEntry struct {
	Message     *ChatMessage
	Placeholder *PromptPlaceholder
	// Extra preserves additional wire fields as a JSON object. It cannot
	// override type or the selected variant's fields. Numbers stay lossless.
	Extra json.RawMessage
}

// MessageEntry constructs an ordinary chat message, including empty content.
func MessageEntry(role, content string) ChatEntry {
	return ChatEntry{Message: &ChatMessage{Role: role, Content: content}}
}

// PlaceholderEntry constructs a named message-list placeholder.
func PlaceholderEntry(name string) ChatEntry {
	return ChatEntry{Placeholder: &PromptPlaceholder{Name: name}}
}

// MarshalJSON encodes the selected chat-message variant without inventing fields.
func (entry ChatEntry) MarshalJSON() ([]byte, error) {
	if (entry.Message == nil) == (entry.Placeholder == nil) {
		return nil, errors.New("langfuse api: chat entry needs exactly one variant")
	}
	if entry.Message != nil {
		if entry.Message.Role == "" {
			return nil, errors.New("langfuse api: chat message requires a role")
		}
		return marshalChatEntry(struct {
			Type    string `json:"type"`
			Role    string `json:"role"`
			Content string `json:"content"`
		}{Type: "chatmessage", Role: entry.Message.Role, Content: entry.Message.Content}, entry.Extra, "role", "content")
	}
	if entry.Placeholder.Name == "" {
		return nil, errors.New("langfuse api: placeholder requires a name")
	}
	return marshalChatEntry(struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}{Type: "placeholder", Name: entry.Placeholder.Name}, entry.Extra, "name", "role", "content")
}

// UnmarshalJSON accepts the contract's optional discriminator and infers the
// variant only from unambiguous required fields. Unknown variants are rejected.
func (entry *ChatEntry) UnmarshalJSON(data []byte) error {
	*entry = ChatEntry{}
	var wire struct {
		Type    *string `json:"type"`
		Role    *string `json:"role"`
		Content *string `json:"content"`
		Name    *string `json:"name"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	kind := ""
	if wire.Type != nil {
		kind = *wire.Type
	}
	if wire.Role != nil && wire.Content != nil && (wire.Name == nil || kind == "chatmessage") && (kind == "" || kind == "chatmessage") && *wire.Role != "" {
		entry.Message = &ChatMessage{Role: *wire.Role, Content: *wire.Content}
		return entry.decodeExtra(data, "type", "role", "content")
	}
	if wire.Name != nil && *wire.Name != "" && wire.Role == nil && wire.Content == nil && (kind == "" || kind == "placeholder") {
		entry.Placeholder = &PromptPlaceholder{Name: *wire.Name}
		return entry.decodeExtra(data, "type", "name")
	}
	return errors.New("langfuse api: invalid chat entry union")
}

// marshalChatEntry merges opaque fields only after validating the variant.
func marshalChatEntry(wire any, extra json.RawMessage, reserved ...string) ([]byte, error) {
	encoded, err := json.Marshal(wire)
	if err != nil || len(extra) == 0 {
		return encoded, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(extra, &fields); err != nil || fields == nil {
		return nil, errors.New("langfuse api: chat extra fields must be a JSON object")
	}
	for _, key := range append(reserved, "type") {
		if _, exists := fields[key]; exists {
			return nil, errors.New("langfuse api: chat extra fields cannot override variant fields")
		}
	}
	var base map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &base); err != nil {
		return nil, err
	}
	maps.Copy(base, fields)
	return json.Marshal(base)
}

func (entry *ChatEntry) decodeExtra(data []byte, known ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, key := range known {
		delete(fields, key)
	}
	if len(fields) == 0 {
		return nil
	}
	extra, err := json.Marshal(fields)
	entry.Extra = extra
	return err
}

// PromptContent is exactly one text string or a non-nil chat slice. Empty text
// and an explicitly empty chat array are distinct valid JSON values.
type PromptContent struct {
	Text *string
	Chat []ChatEntry
}

// TextContent constructs a text prompt, including an empty one.
func TextContent(text string) PromptContent { return PromptContent{Text: &text} }

// ChatContent constructs a chat prompt and copies its top-level entry slice.
func ChatContent(messages ...ChatEntry) PromptContent {
	result := make([]ChatEntry, len(messages))
	copy(result, messages)
	return PromptContent{Chat: result}
}

func (content PromptContent) kind() (PromptType, error) {
	if (content.Text == nil) == (content.Chat == nil) {
		return "", errors.New("langfuse api: prompt content needs exactly one variant")
	}
	if content.Text != nil {
		return PromptText, nil
	}
	return PromptChat, nil
}

// MarshalJSON emits a string or array according to the selected variant.
func (content PromptContent) MarshalJSON() ([]byte, error) {
	kind, err := content.kind()
	if err != nil {
		return nil, err
	}
	if kind == PromptText {
		return json.Marshal(content.Text)
	}
	return json.Marshal(content.Chat)
}

// UnmarshalJSON decodes the prompt union without flattening chat placeholders.
func (content *PromptContent) UnmarshalJSON(data []byte) error {
	*content = PromptContent{}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return errors.New("langfuse api: empty prompt content")
	}
	switch data[0] {
	case '"':
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		content.Text = &text
	case '[':
		if err := json.Unmarshal(data, &content.Chat); err != nil {
			return err
		}
	default:
		return errors.New("langfuse api: invalid prompt content type")
	}
	return nil
}

// Prompt is a typed prompt version. Config and ResolutionGraph retain arbitrary
// JSON losslessly. API reads do not use or modify the runtime SDK's prompt cache.
type Prompt struct {
	Name            string                 `json:"name"`
	Version         int                    `json:"version"`
	Type            PromptType             `json:"type"`
	Prompt          PromptContent          `json:"prompt"`
	Config          json.RawMessage        `json:"config"`
	Labels          []string               `json:"labels"`
	Tags            []string               `json:"tags"`
	CommitMessage   Field[string]          `json:"commitMessage,omitzero"`
	ResolutionGraph Field[json.RawMessage] `json:"resolutionGraph,omitzero"`
}

// UnmarshalJSON verifies the discriminator and required response identity.
func (prompt *Prompt) UnmarshalJSON(data []byte) error {
	type wire Prompt
	var value wire
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	kind, err := value.Prompt.kind()
	if err != nil || kind != value.Type || value.Name == "" || value.Version < 1 || len(value.Config) == 0 || value.Labels == nil || value.Tags == nil {
		return ErrInvalidResponse
	}
	*prompt = Prompt(value)
	return nil
}

// CreatePromptRequest creates a new version, never an in-place content update.
// Type may be omitted in Go and is inferred from Prompt. Set empty slices to
// explicitly include [], rather than relying on server-side defaults.
type CreatePromptRequest struct {
	Name          string              `json:"name"`
	Prompt        PromptContent       `json:"prompt"`
	Type          PromptType          `json:"type,omitempty"`
	Config        json.RawMessage     `json:"config,omitempty"`
	Labels        *Optional[[]string] `json:"labels,omitempty"`
	Tags          *Optional[[]string] `json:"tags,omitempty"`
	CommitMessage *Optional[string]   `json:"commitMessage,omitempty"`
}

func (request CreatePromptRequest) validate() error {
	if _, err := escapedSegment(request.Name); err != nil {
		return err
	}
	kind, err := request.Prompt.kind()
	if err != nil {
		return err
	}
	if request.Type != "" && request.Type != kind {
		return errors.New("langfuse api: prompt type does not match its content")
	}
	if !validJSON(request.Config) {
		return errors.New("langfuse api: invalid prompt config JSON")
	}
	if labels, ok := request.Labels.Value(); ok {
		if err := validatePromptLabels(labels); err != nil {
			return err
		}
	}
	return nil
}

// MarshalJSON emits the canonical inferred type and preserves request presence.
func (request CreatePromptRequest) MarshalJSON() ([]byte, error) {
	if err := request.validate(); err != nil {
		return nil, err
	}
	request.Type, _ = request.Prompt.kind()
	type wire CreatePromptRequest
	return json.Marshal(wire(request))
}

// GetPromptOptions selects a version or label, but never both. With neither,
// Langfuse selects production. Resolve=false returns raw dependency tags for
// one-off export/debugging; it is not the runtime cache's retrieval mode.
type GetPromptOptions struct {
	Version int
	Label   string
	Resolve *bool
}

// ListPromptsOptions filters prompt names/versions. The updated-at interval is
// inclusive at FromUpdatedAt and exclusive at ToUpdatedAt.
type ListPromptsOptions struct {
	PageOptions
	Name          string
	Label         string
	Tag           string
	FromUpdatedAt time.Time
	ToUpdatedAt   time.Time
}

// PromptMeta is the prompt list entry, not a compiled or complete prompt version.
type PromptMeta struct {
	Name          string          `json:"name"`
	Type          PromptType      `json:"type"`
	Versions      []int           `json:"versions"`
	Labels        []string        `json:"labels"`
	Tags          []string        `json:"tags"`
	LastUpdatedAt time.Time       `json:"lastUpdatedAt"`
	LastConfig    json.RawMessage `json:"lastConfig"`
}

// DeletePromptOptions requires exactly one selector. The dangerous server
// default of deleting all versions is available only through AllVersions=true.
// Label deletion deletes versions carrying that label; it does not detach it.
type DeletePromptOptions struct {
	Version     int
	Label       string
	AllVersions bool
}

// Create creates one version in one HTTP attempt. A timeout is an ambiguous
// write outcome: inspect the server before manually retrying. It does not
// invalidate any runtime client's independent prompt cache.
func (s *PromptsService) Create(ctx context.Context, request CreatePromptRequest) (Prompt, error) {
	var result Prompt
	if err := request.validate(); err != nil {
		return result, err
	}
	err := clientFor((*service)(s)).do(ctx, "prompts_create", http.MethodPost, "/v2/prompts", nil, request, &result)
	if err == nil && result.Name != request.Name {
		return Prompt{}, &RequestError{Operation: "prompts_create", Kind: "invalid response", OutcomeUnknown: true, cause: ErrInvalidResponse}
	}
	return result, err
}

// Get retrieves one selected prompt version without runtime cache side effects.
func (s *PromptsService) Get(ctx context.Context, name string, options GetPromptOptions) (Prompt, error) {
	var result Prompt
	path, err := escapedSegment(name)
	if err != nil {
		return result, err
	}
	if options.Version < 0 || (options.Version != 0 && options.Label != "") {
		return result, errors.New("langfuse api: select a prompt version or label, not both")
	}
	query := url.Values{}
	if options.Version != 0 {
		query.Set("version", strconv.Itoa(options.Version))
	}
	if options.Label != "" {
		query.Set("label", options.Label)
	}
	if options.Resolve != nil {
		query.Set("resolve", strconv.FormatBool(*options.Resolve))
	}
	err = clientFor((*service)(s)).do(ctx, "prompts_get", http.MethodGet, "/v2/prompts/"+path, query, nil, &result)
	if err == nil && (result.Name != name || (options.Version != 0 && result.Version != options.Version)) {
		return Prompt{}, ErrInvalidResponse
	}
	return result, err
}

// List retrieves one bounded page; it never materializes the full project.
func (s *PromptsService) List(ctx context.Context, options ListPromptsOptions) (Page[PromptMeta], error) {
	var result Page[PromptMeta]
	query, err := pageValues(options.PageOptions)
	if err != nil {
		return result, err
	}
	if err := addTimeRange(query, "fromUpdatedAt", "toUpdatedAt", options.FromUpdatedAt, options.ToUpdatedAt); err != nil {
		return result, err
	}
	addString(query, "name", options.Name)
	addString(query, "label", options.Label)
	addString(query, "tag", options.Tag)
	err = clientFor((*service)(s)).do(ctx, "prompts_list", http.MethodGet, "/v2/prompts", query, nil, &result)
	return result, err
}

// UpdateLabels adds or moves labels to this version; it does NOT replace the
// existing label set. An empty array removes nothing. The latest label is
// server-managed. Explicitly invalidate all selectors for this prompt name in
// each runtime client after a successful deployment or rollback.
func (s *PromptsService) UpdateLabels(ctx context.Context, name string, version int, labels []string) (Prompt, error) {
	var result Prompt
	path, err := escapedSegment(name)
	if err != nil {
		return result, err
	}
	if version < 1 {
		return result, errors.New("langfuse api: prompt version must be positive")
	}
	if err := validatePromptLabels(labels); err != nil {
		return result, err
	}
	if labels == nil {
		labels = []string{}
	}
	request := struct {
		NewLabels []string `json:"newLabels"`
	}{NewLabels: labels}
	err = clientFor((*service)(s)).do(ctx, "promptVersion_update", http.MethodPatch, "/v2/prompts/"+path+"/versions/"+strconv.Itoa(version), nil, request, &result)
	if err == nil && (result.Name != name || result.Version != version) {
		return Prompt{}, &RequestError{Operation: "promptVersion_update", Kind: "invalid response", OutcomeUnknown: true, cause: ErrInvalidResponse}
	}
	return result, err
}

// Delete performs an explicitly selected deletion in one HTTP attempt.
func (s *PromptsService) Delete(ctx context.Context, name string, options DeletePromptOptions) error {
	path, err := escapedSegment(name)
	if err != nil {
		return err
	}
	selected := 0
	if options.Version > 0 {
		selected++
	}
	if options.Label != "" {
		selected++
	}
	if options.AllVersions {
		selected++
	}
	if options.Version < 0 || selected != 1 {
		return errors.New("langfuse api: prompt deletion requires exactly one explicit selector")
	}
	query := url.Values{}
	if options.Version > 0 {
		query.Set("version", strconv.Itoa(options.Version))
	}
	if options.Label != "" {
		query.Set("label", options.Label)
	}
	return clientFor((*service)(s)).do(ctx, "prompts_delete", http.MethodDelete, "/v2/prompts/"+path, query, nil, nil)
}

func validatePromptLabels(labels []string) error {
	for _, label := range labels {
		if strings.TrimSpace(label) == "" || label == "latest" {
			return errors.New("langfuse api: labels must be non-empty and latest is server-managed")
		}
	}
	return nil
}

func addString(query url.Values, key, value string) {
	if value != "" {
		query.Set(key, value)
	}
}

func addTimeRange(query url.Values, fromKey, toKey string, from, to time.Time) error {
	if !from.IsZero() && !to.IsZero() && !from.Before(to) {
		return errors.New("langfuse api: time range must increase")
	}
	if !from.IsZero() {
		query.Set(fromKey, from.UTC().Format(time.RFC3339Nano))
	}
	if !to.IsZero() {
		query.Set(toKey, to.UTC().Format(time.RFC3339Nano))
	}
	return nil
}
