package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newPromptsTestClient(t *testing.T, baseURL string) *PromptsClient {
	t.Helper()
	client, err := NewPromptsClient(Config{
		BaseURL:    baseURL,
		PublicKey:  "pk-lf-prompts",
		SecretKey:  "sk-lf-prompts",
		SDKVersion: "test",
	})
	if err != nil {
		t.Fatalf("NewPromptsClient() error = %v", err)
	}
	client.retryInterval = time.Millisecond
	return client
}

func promptWireJSON(name string, version int, labels []string) string {
	encoded, err := json.Marshal(map[string]any{
		"id":            "wire-id",
		"name":          name,
		"version":       version,
		"type":          "text",
		"prompt":        "Hello {{subject}}",
		"config":        map[string]any{"temperature": 0.2},
		"labels":        labels,
		"tags":          []string{"wire"},
		"commitMessage": "initial",
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func TestPromptsFetchSendsAuthenticatedRequest(t *testing.T) {
	t.Parallel()
	var path, rawQuery, username, password atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.EscapedPath())
		rawQuery.Store(r.URL.RawQuery)
		user, pass, _ := r.BasicAuth()
		username.Store(user)
		password.Store(pass)
		_, _ = w.Write([]byte(promptWireJSON("folder/movie critic", 3, []string{"production"})))
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	prompt, err := client.Fetch(context.Background(), "folder/movie critic", 3, "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if got := path.Load(); got != "/api/public/v2/prompts/folder%2Fmovie%20critic" {
		t.Fatalf("request path = %q, want the escaped prompt name", got)
	}
	if got := rawQuery.Load(); got != "version=3" {
		t.Fatalf("request query = %q, want version=3", got)
	}
	if username.Load() != "pk-lf-prompts" || password.Load() != "sk-lf-prompts" {
		t.Fatalf("basic auth = (%v, ...), want the client credentials", username.Load())
	}
	if prompt.Name != "folder/movie critic" || prompt.Version != 3 || prompt.Type != "text" ||
		prompt.Text != "Hello {{subject}}" || prompt.CommitMessage != "initial" {
		t.Fatalf("decoded prompt = %+v, want the wire fields", prompt)
	}
	if string(prompt.Config) != `{"temperature":0.2}` {
		t.Fatalf("decoded config = %s, want the wire config", prompt.Config)
	}
}

func TestPromptsFetchSelectsByLabel(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RawQuery; got != "label=staging" {
			t.Errorf("request query = %q, want label=staging", got)
		}
		_, _ = w.Write([]byte(promptWireJSON("n", 7, []string{"staging", "production"})))
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	prompt, err := client.Fetch(context.Background(), "n", 0, "staging")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if prompt.Version != 7 {
		t.Fatalf("prompt version = %d, want 7", prompt.Version)
	}
}

func TestPromptsFetchRetriesTransientFailures(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(promptWireJSON("n", 1, []string{"production"})))
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	if _, err := client.Fetch(context.Background(), "n", 0, "production"); err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want 3 (two 500s then success)", got)
	}
}

func TestPromptsFetchStopsAfterRetryLimit(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	_, err := client.Fetch(context.Background(), "n", 0, "production")
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("Fetch() error = %v, want a status 503 failure", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want the initial attempt plus two retries", got)
	}
}

func TestPromptsFetchDoesNotRetryTerminalStatuses(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	_, err := client.Fetch(context.Background(), "n", 0, "production")
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("Fetch() error = %v, want a status 400 failure", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 (a 400 must not be retried)", got)
	}
}

func TestPromptsFetchNotFound(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	_, err := client.Fetch(context.Background(), "n", 0, "production")
	if !errors.Is(err, ErrPromptNotFound) {
		t.Fatalf("Fetch() error = %v, want ErrPromptNotFound", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}

func TestPromptsFetchDeclinesRetryBeyondDeadline(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := client.Fetch(ctx, "n", 0, "production")
	if err == nil || !strings.Contains(err.Error(), "status 429") {
		t.Fatalf("Fetch() error = %v, want a status 429 failure", err)
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("Fetch() took %v, want an immediate decline of the 30 s Retry-After", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}

func TestPromptsFetchRefusesRedirects(t *testing.T) {
	t.Parallel()
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		_, _ = w.Write([]byte(promptWireJSON("n", 1, []string{"production"})))
	}))
	t.Cleanup(target.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	_, err := client.Fetch(context.Background(), "n", 0, "production")
	if err == nil || !strings.Contains(err.Error(), "status 302") {
		t.Fatalf("Fetch() error = %v, want a status 302 failure", err)
	}
	if got := redirected.Load(); got != 0 {
		t.Fatalf("redirect target request count = %d, want 0 (credentials must not follow redirects)", got)
	}
}

func TestPromptsFetchRejectsOversizedBodies(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"name":"n","version":1,"type":"text","prompt":"`))
		_, _ = w.Write([]byte(strings.Repeat("a", maxPromptResponseBytes)))
		_, _ = w.Write([]byte(`","labels":["production"]}`))
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	_, err := client.Fetch(context.Background(), "n", 0, "production")
	if err == nil || !strings.Contains(err.Error(), "1 MiB") {
		t.Fatalf("Fetch() error = %v, want the size-limit failure", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 (an oversized body must not be retried)", got)
	}
}

func TestPromptsFetchRejectsMismatchedResponses(t *testing.T) {
	t.Parallel()
	valid := func(overrides map[string]any) string {
		body := map[string]any{
			"name":    "n",
			"version": 2,
			"type":    "text",
			"prompt":  "hello",
			"labels":  []string{"production"},
		}
		for key, value := range overrides {
			if value == nil {
				delete(body, key)
			} else {
				body[key] = value
			}
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal test body: %v", err)
		}
		return string(encoded)
	}
	cases := map[string]struct {
		version int
		label   string
		body    string
	}{
		"wrong name":          {0, "production", valid(map[string]any{"name": "other"})},
		"non-positive verson": {0, "production", valid(map[string]any{"version": 0})},
		"wrong exact version": {3, "", valid(map[string]any{})},
		"missing label":       {0, "staging", valid(map[string]any{})},
		"unknown type":        {0, "production", valid(map[string]any{"type": "image"})},
		"chat body for text":  {0, "production", valid(map[string]any{"prompt": []string{}})},
		"invalid utf8":        {0, "production", "{\"name\":\"n\",\"version\":2,\"type\":\"text\",\"prompt\":\"\xff\",\"labels\":[\"production\"]}"},
		"trailing json":       {0, "production", valid(map[string]any{}) + `{}`},
		"not json":            {0, "production", "not json"},
	}
	for label, test := range cases {
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(server.Close)
			client := newPromptsTestClient(t, server.URL)
			if _, err := client.Fetch(context.Background(), "n", test.version, test.label); err == nil {
				t.Fatal("Fetch() error = nil, want a decode or mismatch failure")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("request count = %d, want 1 (deterministic failures must not retry)", got)
			}
		})
	}
}

func TestPromptsDecodeChatMessages(t *testing.T) {
	t.Parallel()
	body := `{
		"name": "chat", "version": 1, "type": "chat", "labels": ["production"],
		"prompt": [
			{"role": "system", "content": "Be {{tone}}."},
			{"type": "chatmessage", "role": "assistant", "content": "", "tool_calls": [{"id": "call-1"}], "name": "helper"},
			{"type": "placeholder", "name": "history"}
		]
	}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client := newPromptsTestClient(t, server.URL)

	prompt, err := client.Fetch(context.Background(), "chat", 0, "production")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(prompt.Messages) != 3 {
		t.Fatalf("message count = %d, want 3", len(prompt.Messages))
	}
	if m := prompt.Messages[0]; m.Role != "system" || m.Content != "Be {{tone}}." || m.Extra != nil {
		t.Fatalf("first message = %+v, want plain role and content", m)
	}
	if m := prompt.Messages[1]; m.Role != "assistant" || m.Content != "" ||
		string(m.Extra) != `{"name":"helper","tool_calls":[{"id":"call-1"}]}` {
		t.Fatalf("second message = %+v (extra %s), want preserved extra fields", m, m.Extra)
	}
	if m := prompt.Messages[2]; m.PlaceholderName != "history" || m.Role != "" {
		t.Fatalf("third message = %+v, want the history placeholder", m)
	}
}

func TestPromptsDecodeRejectsUnknownMessageShapes(t *testing.T) {
	t.Parallel()
	bodies := map[string]string{
		"missing role":       `[{"content": "x"}]`,
		"empty role":         `[{"role": "", "content": "x"}]`,
		"unknown type":       `[{"type": "image", "url": "x"}]`,
		"placeholder noname": `[{"type": "placeholder"}]`,
		"not an object":      `["hello"]`,
	}
	for label, messages := range bodies {
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			body := fmt.Sprintf(`{"name":"n","version":1,"type":"chat","labels":["production"],"prompt":%s}`, messages)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(server.Close)
			client := newPromptsTestClient(t, server.URL)
			if _, err := client.Fetch(context.Background(), "n", 0, "production"); err == nil {
				t.Fatal("Fetch() error = nil, want a decode failure")
			}
		})
	}
}

func TestNormalizePromptsEndpoint(t *testing.T) {
	t.Parallel()
	got, err := NormalizePromptsEndpoint("https://cloud.langfuse.com/api/public/otel")
	if err != nil {
		t.Fatalf("NormalizePromptsEndpoint() error = %v", err)
	}
	if got != "https://cloud.langfuse.com/api/public/v2/prompts" {
		t.Fatalf("NormalizePromptsEndpoint() = %q, want the prompts API base", got)
	}
}
