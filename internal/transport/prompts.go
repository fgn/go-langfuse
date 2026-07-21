package transport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	// maxPromptResponseBytes bounds one prompt response body, matching the
	// SDK's 1 MiB attribute preflight bound; a larger prompt is pathological.
	// An over-limit body is a terminal failure: it is deterministic, so
	// retrying it would never converge.
	maxPromptResponseBytes = 1 << 20
	// promptRetryLimit and promptRetryInitialInterval shape the short
	// blocking retry loop. GetPrompt blocks its caller, so the budget is
	// seconds (two retries at 500 ms then 1 s with jitter), not the score
	// pipeline's one-minute asynchronous budget.
	promptRetryLimit           = 2
	promptRetryInitialInterval = 500 * time.Millisecond
)

// ErrPromptNotFound marks a 404 for the requested prompt name, version, or
// label. The root package wraps it into the exported sentinel.
var ErrPromptNotFound = errors.New("langfuse transport: prompt not found")

// Prompt is one decoded, semantically validated prompt version.
type Prompt struct {
	Name          string
	Version       int
	Type          string // "text" or "chat"
	Text          string
	Messages      []PromptMessage
	Config        json.RawMessage
	Labels        []string
	Tags          []string
	CommitMessage string
}

// PromptMessage is one decoded chat message: either a regular message with
// Role, optional Content, and optional Extra (a JSON object of additional
// provider fields), or a placeholder slot named by PlaceholderName.
type PromptMessage struct {
	Role            string
	Content         string
	PlaceholderName string
	Extra           json.RawMessage
}

// PromptsClient fetches prompt versions from the Langfuse prompts API. It is
// stateless and safe for concurrent use; caching lives in the root package.
type PromptsClient struct {
	endpoint  string
	publicKey string
	secretKey string
	client    *http.Client

	// retryLimit and retryInterval are test seams; production values come
	// from the constants above.
	retryLimit    int
	retryInterval time.Duration
}

// NewPromptsClient builds a prompts client from an already validated
// transport configuration. It performs no network I/O.
func NewPromptsClient(cfg Config) (*PromptsClient, error) {
	endpoint, err := NormalizePromptsEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &PromptsClient{
		endpoint:  endpoint,
		publicKey: cfg.PublicKey,
		secretKey: cfg.SecretKey,
		client: &http.Client{
			Timeout: timeout,
			// Never follow redirects: a Location target is server-controlled
			// and following it would re-send the credentials to another URL.
			// A 3xx response is returned as-is and fails as terminal.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		retryLimit:    promptRetryLimit,
		retryInterval: promptRetryInitialInterval,
	}, nil
}

// Fetch retrieves one prompt version, retrying transient failures within the
// bounds of ctx. version > 0 selects an exact version; otherwise label (which
// the caller has already normalized to be non-empty) selects by deployment
// label. Error messages contain only static text and numeric status codes.
func (p *PromptsClient) Fetch(ctx context.Context, name string, version int, label string) (Prompt, error) {
	requestURL := p.endpoint + "/" + url.PathEscape(name)
	if version > 0 {
		requestURL += "?version=" + strconv.Itoa(version)
	} else {
		requestURL += "?label=" + url.QueryEscape(label)
	}
	interval := p.retryInterval
	for attempt := 0; ; attempt++ {
		prompt, retryable, retryAfter, err := p.attempt(ctx, requestURL, name, version, label)
		if err == nil {
			return prompt, nil
		}
		if !retryable || attempt >= p.retryLimit || ctx.Err() != nil {
			return Prompt{}, err
		}
		delay := max(interval/2+rand.N(interval), retryAfter)
		// A retry whose delay exceeds the remaining budget is declined: the
		// last observed failure is more useful than a guaranteed timeout.
		if deadline, ok := ctx.Deadline(); ok && time.Now().Add(delay).After(deadline) {
			return Prompt{}, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return Prompt{}, err
		}
		interval *= 2
	}
}

// attempt performs one request. Retryable failures are network errors and
// 408/429/5xx statuses; everything else — including 3xx (redirects are never
// followed), 404 (ErrPromptNotFound), and any decode or semantic-validation
// failure — is terminal because retrying a deterministic failure would never
// converge.
func (p *PromptsClient) attempt(ctx context.Context, requestURL, name string, version int, label string) (prompt Prompt, retryable bool, retryAfter time.Duration, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return Prompt{}, false, 0, errors.New("langfuse transport: the prompt request could not be built")
	}
	request.SetBasicAuth(p.publicKey, p.secretKey)
	response, err := p.client.Do(request)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return Prompt{}, true, 0, errors.New("langfuse transport: the prompt request timed out")
		}
		return Prompt{}, true, 0, errors.New("langfuse transport: the prompt request failed before a response")
	}
	defer func() { _ = response.Body.Close() }()
	retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxScoreDrainBytes))
		if response.StatusCode == http.StatusNotFound {
			return Prompt{}, false, 0, ErrPromptNotFound
		}
		failure := errors.New("langfuse transport: the prompt endpoint returned status " +
			strconv.Itoa(response.StatusCode))
		if retryableStatus(response.StatusCode) {
			return Prompt{}, true, retryAfter, failure
		}
		return Prompt{}, false, 0, failure
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxPromptResponseBytes+1))
	if readErr != nil {
		return Prompt{}, true, retryAfter, errors.New("langfuse transport: the prompt response could not be read")
	}
	if len(body) > maxPromptResponseBytes {
		return Prompt{}, false, 0, errors.New("langfuse transport: the prompt response exceeds the 1 MiB limit")
	}
	prompt, err = decodePrompt(body, name, version, label)
	if err != nil {
		return Prompt{}, false, 0, err
	}
	return prompt, false, 0, nil
}

// wirePrompt is the subset of the documented GET /api/public/v2/prompts
// response the SDK consumes; its camelCase wire keys are extracted
// explicitly in decodePrompt, and unknown top-level fields are ignored.
type wirePrompt struct {
	Name          string
	Version       int
	Type          string
	Prompt        json.RawMessage
	Config        json.RawMessage
	Labels        []string
	Tags          []string
	CommitMessage string
}

var errPromptDecode = errors.New("langfuse transport: the prompt response could not be decoded")

// promptField decodes fields[key] into target when the key is present and
// non-null; an absent key keeps the target's zero value.
func promptField(fields map[string]json.RawMessage, key string, target any) error {
	raw, ok := fields[key]
	if !ok || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, target)
}

// decodePrompt decodes and semantically validates a 2xx body against the
// request, so a mismatched response can never poison the cache or mint a
// wrong prompt reference. The raw body must be valid UTF-8 before
// encoding/json can silently replace malformed bytes, and json.Unmarshal
// already rejects trailing JSON values.
func decodePrompt(body []byte, name string, version int, label string) (Prompt, error) {
	if !utf8.Valid(body) {
		return Prompt{}, errPromptDecode
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return Prompt{}, errPromptDecode
	}
	var wire wirePrompt
	for key, target := range map[string]any{
		"name":          &wire.Name,
		"version":       &wire.Version,
		"type":          &wire.Type,
		"labels":        &wire.Labels,
		"tags":          &wire.Tags,
		"commitMessage": &wire.CommitMessage,
	} {
		if err := promptField(fields, key, target); err != nil {
			return Prompt{}, errPromptDecode
		}
	}
	wire.Prompt = fields["prompt"]
	wire.Config = fields["config"]
	if wire.Name != name || wire.Version <= 0 {
		return Prompt{}, errors.New("langfuse transport: the prompt response did not match the request")
	}
	if version > 0 && wire.Version != version {
		return Prompt{}, errors.New("langfuse transport: the prompt response did not match the requested version")
	}
	if version == 0 && !slices.Contains(wire.Labels, label) {
		return Prompt{}, errors.New("langfuse transport: the prompt response did not carry the requested label")
	}
	prompt := Prompt{
		Name:          wire.Name,
		Version:       wire.Version,
		Type:          wire.Type,
		Labels:        wire.Labels,
		Tags:          wire.Tags,
		CommitMessage: wire.CommitMessage,
	}
	if len(wire.Config) > 0 && string(wire.Config) != "null" {
		prompt.Config = wire.Config
	}
	switch wire.Type {
	case "text":
		if err := json.Unmarshal(wire.Prompt, &prompt.Text); err != nil {
			return Prompt{}, errPromptDecode
		}
	case "chat":
		messages, err := decodePromptMessages(wire.Prompt)
		if err != nil {
			return Prompt{}, err
		}
		prompt.Messages = messages
	default:
		return Prompt{}, errPromptDecode
	}
	return prompt, nil
}

// decodePromptMessages decodes the chat body: an array of regular messages
// ({role, content, ...extra}, optionally discriminated with
// type "chatmessage") and placeholder slots ({type: "placeholder", name}).
// Unknown regular-message keys are preserved verbatim in Extra; an unknown
// message shape is a decode error — failing loud beats dropping content.
func decodePromptMessages(raw json.RawMessage) ([]PromptMessage, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, errPromptDecode
	}
	messages := make([]PromptMessage, 0, len(items))
	for _, item := range items {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(item, &fields); err != nil {
			return nil, errPromptDecode
		}
		kind := ""
		if rawKind, ok := fields["type"]; ok {
			if err := json.Unmarshal(rawKind, &kind); err != nil {
				return nil, errPromptDecode
			}
		}
		switch kind {
		case "placeholder":
			var placeholder string
			rawName, ok := fields["name"]
			if !ok || json.Unmarshal(rawName, &placeholder) != nil || placeholder == "" {
				return nil, errPromptDecode
			}
			messages = append(messages, PromptMessage{PlaceholderName: placeholder})
		case "", "chatmessage":
			message := PromptMessage{}
			rawRole, ok := fields["role"]
			if !ok || json.Unmarshal(rawRole, &message.Role) != nil || message.Role == "" {
				return nil, errPromptDecode
			}
			if rawContent, ok := fields["content"]; ok {
				if err := json.Unmarshal(rawContent, &message.Content); err != nil {
					return nil, errPromptDecode
				}
			}
			delete(fields, "type")
			delete(fields, "role")
			delete(fields, "content")
			if len(fields) > 0 {
				// Maps marshal with sorted keys, so Extra is deterministic.
				extra, err := json.Marshal(fields)
				if err != nil {
					return nil, errPromptDecode
				}
				message.Extra = extra
			}
			messages = append(messages, message)
		default:
			return nil, errPromptDecode
		}
	}
	return messages, nil
}
