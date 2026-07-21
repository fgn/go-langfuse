package langfuse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// promptClock is a thread-safe fake clock for cache-freshness tests.
type promptClock struct {
	mu sync.Mutex
	at time.Time
}

func newPromptClock() *promptClock {
	return &promptClock{at: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)}
}

func (c *promptClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *promptClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

type promptWireReceiver struct {
	mu       sync.Mutex
	requests []*http.Request
	handler  func(w http.ResponseWriter, r *http.Request, call int)
}

func (r *promptWireReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.requests = append(r.requests, req)
	call := len(r.requests)
	handler := r.handler
	r.mu.Unlock()
	if handler != nil {
		handler(w, req, call)
		return
	}
	writePromptWire(w, "greeting", 1, "Hello {{name}}!")
}

func (r *promptWireReceiver) setHandler(handler func(w http.ResponseWriter, r *http.Request, call int)) {
	r.mu.Lock()
	r.handler = handler
	r.mu.Unlock()
}

func (r *promptWireReceiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func writePromptWire(w http.ResponseWriter, name string, version int, text string) {
	encoded, err := json.Marshal(map[string]any{
		"name":    name,
		"version": version,
		"type":    "text",
		"prompt":  text,
		"labels":  []string{"production", "staging"},
		"tags":    []string{"wire"},
	})
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(encoded)
}

func newPromptWireClient(t *testing.T) (*langfuse.Client, *promptWireReceiver, *promptClock) {
	t.Helper()
	receiver := &promptWireReceiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:   server.URL,
		PublicKey: "pk-lf-prompt-wire",
		SecretKey: "sk-lf-prompt-wire",
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	clock := newPromptClock()
	langfuse.SetPromptClock(client, clock.Now)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Client.Shutdown() error = %v", err)
		}
	})
	return client, receiver, clock
}

// awaitPromptCondition polls until check succeeds or the deadline passes,
// for conditions resolved by background refreshes.
func awaitPromptCondition(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("condition %q was not reached within 5s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPromptWireFetchesAndCaches(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)

	first, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	if first.Name != "greeting" || first.Version != 1 || first.Type != langfuse.PromptTypeText ||
		first.Text != "Hello {{name}}!" || first.Fallback {
		t.Fatalf("GetPrompt() = %+v, want the wire prompt", first)
	}
	if ref := first.Ref(); ref == nil || ref.Name != "greeting" || ref.Version != 1 {
		t.Fatalf("Ref() = %+v, want the fetched name and version", first.Ref())
	}
	second, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil {
		t.Fatalf("GetPrompt() second call error = %v", err)
	}
	if second.Text != first.Text {
		t.Fatalf("cached prompt text = %q, want %q", second.Text, first.Text)
	}
	if got := receiver.count(); got != 1 {
		t.Fatalf("request count = %d, want 1 (the second call must be served from cache)", got)
	}

	request := receiver.requests[0]
	if request.URL.EscapedPath() != "/api/public/v2/prompts/greeting" {
		t.Fatalf("request path = %q, want the prompts API path", request.URL.EscapedPath())
	}
	if request.URL.RawQuery != "label=production" {
		t.Fatalf("request query = %q, want the production default label", request.URL.RawQuery)
	}
	if user, pass, ok := request.BasicAuth(); !ok || user != "pk-lf-prompt-wire" || pass != "sk-lf-prompt-wire" {
		t.Fatalf("basic auth = (%q, ok %v), want the client credentials", user, ok)
	}
}

func TestPromptWireCacheKeySeparatesSelectors(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		version := 1
		if r.URL.Query().Get("version") == "2" {
			version = 2
		}
		writePromptWire(w, "greeting", version, "hi")
	})

	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt(label) error = %v", err)
	}
	byVersion, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{Version: 2})
	if err != nil {
		t.Fatalf("GetPrompt(version) error = %v", err)
	}
	if byVersion.Version != 2 {
		t.Fatalf("versioned prompt = %d, want 2", byVersion.Version)
	}
	if got := receiver.count(); got != 2 {
		t.Fatalf("request count = %d, want 2 (distinct cache keys)", got)
	}
}

func TestPromptWireStaleServesOldValueAndRefreshes(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		writePromptWire(w, "greeting", call, "v")
	})

	first, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	if first.Version != 1 {
		t.Fatalf("first fetch version = %d, want 1", first.Version)
	}

	clock.Advance(2 * time.Minute)
	stale, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil {
		t.Fatalf("GetPrompt() stale error = %v", err)
	}
	if stale.Version != 1 {
		t.Fatalf("stale hit version = %d, want the old value 1 while refreshing", stale.Version)
	}
	awaitPromptCondition(t, "background refresh request", func() bool { return receiver.count() >= 2 })
	awaitPromptCondition(t, "refreshed value visible", func() bool {
		prompt, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
		return err == nil && prompt.Version == 2
	})
	if got := receiver.count(); got != 2 {
		t.Fatalf("request count = %d, want 2 (one fetch and one refresh)", got)
	}
}

func TestPromptWireRefreshFailureCoolsDownAndKeepsStale(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		if call == 1 {
			writePromptWire(w, "greeting", 1, "v")
			return
		}
		// Terminal status: no transport retries, exactly one request per
		// admitted refresh.
		w.WriteHeader(http.StatusBadRequest)
	})

	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	clock.Advance(2 * time.Minute)
	stale, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil || stale.Version != 1 {
		t.Fatalf("stale hit = (%+v, %v), want the cached version despite the failing refresh", stale, err)
	}
	awaitPromptCondition(t, "failed refresh request", func() bool { return receiver.count() == 2 })

	// Within the cooldown no further refresh is admitted.
	for range 5 {
		if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
			t.Fatalf("GetPrompt() during cooldown error = %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := receiver.count(); got != 2 {
		t.Fatalf("request count = %d, want 2 (cooldown must suppress refresh attempts)", got)
	}

	clock.Advance(langfuse.PromptRefreshCooldown + time.Second)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() after cooldown error = %v", err)
	}
	awaitPromptCondition(t, "refresh after cooldown", func() bool { return receiver.count() == 3 })
}

func TestPromptWireRefreshNotFoundEvicts(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		if call == 1 {
			writePromptWire(w, "greeting", 1, "v")
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	clock.Advance(2 * time.Minute)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() stale error = %v", err)
	}
	awaitPromptCondition(t, "eviction after 404 refresh", func() bool {
		_, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
		return errors.Is(err, langfuse.ErrPromptNotFound)
	})
}

func TestPromptWireMissSingleflightCollapsesConcurrentCallers(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, _ int) {
		<-release
		writePromptWire(w, "greeting", 1, "v")
	})

	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
			results <- err
		}()
	}
	awaitPromptCondition(t, "flight request started", func() bool { return receiver.count() == 1 })
	close(release)
	for range callers {
		if err := <-results; err != nil {
			t.Fatalf("concurrent GetPrompt() error = %v", err)
		}
	}
	if got := receiver.count(); got != 1 {
		t.Fatalf("request count = %d, want 1 (concurrent misses must share one flight)", got)
	}
}

func TestPromptWireCanceledWaiterLeavesFlight(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, _ int) {
		<-release
		writePromptWire(w, "greeting", 1, "v")
	})

	steady := make(chan error, 1)
	go func() {
		_, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
		steady <- err
	}()
	awaitPromptCondition(t, "flight request started", func() bool { return receiver.count() == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, err := client.GetPrompt(ctx, "greeting", langfuse.PromptQuery{})
		canceled <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not return")
	}
	close(release)
	if err := <-steady; err != nil {
		t.Fatalf("remaining waiter error = %v, want the shared fetch to complete", err)
	}
}

func TestPromptWireDisableCacheBypassesReadsAndWrites(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)

	for i := range 2 {
		prompt, err := client.GetPrompt(context.Background(), "greeting",
			langfuse.PromptQuery{DisableCache: true})
		if err != nil {
			t.Fatalf("GetPrompt(DisableCache) call %d error = %v", i, err)
		}
		if prompt.Version != 1 {
			t.Fatalf("prompt version = %d, want 1", prompt.Version)
		}
	}
	if got := receiver.count(); got != 2 {
		t.Fatalf("request count = %d, want 2 (DisableCache must not read the cache)", got)
	}
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	if got := receiver.count(); got != 3 {
		t.Fatalf("request count = %d, want 3 (DisableCache must not have written the cache)", got)
	}
}

func TestPromptWireFallbackOnFetchFailure(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(http.StatusBadRequest)
	})

	fallback := &langfuse.PromptFallback{Text: "fallback body"}
	prompt, err := client.GetPrompt(context.Background(), "greeting",
		langfuse.PromptQuery{Version: 7, Fallback: fallback})
	if err != nil {
		t.Fatalf("GetPrompt() with fallback error = %v, want the fallback instead", err)
	}
	if !prompt.Fallback || prompt.Name != "greeting" || prompt.Version != 7 ||
		prompt.Type != langfuse.PromptTypeText || prompt.Text != "fallback body" {
		t.Fatalf("fallback prompt = %+v, want the projected fallback", prompt)
	}
	if prompt.Ref() != nil {
		t.Fatalf("fallback Ref() = %+v, want nil so linking is skipped", prompt.Ref())
	}
	if len(prompt.Labels) != 0 || len(prompt.Tags) != 0 || prompt.CommitMessage != "" {
		t.Fatalf("fallback prompt = %+v, want empty server-owned metadata", prompt)
	}

	_, err = client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{Version: 7})
	if err == nil || !strings.Contains(err.Error(), "status 400") ||
		!strings.Contains(err.Error(), `"greeting"`) {
		t.Fatalf("GetPrompt() without fallback error = %v, want the named status failure", err)
	}
}

func TestPromptWireNotFoundWrapsSentinel(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := client.GetPrompt(context.Background(), "missing", langfuse.PromptQuery{})
	if !errors.Is(err, langfuse.ErrPromptNotFound) {
		t.Fatalf("GetPrompt() error = %v, want ErrPromptNotFound in the chain", err)
	}
}

func TestPromptWireCancellationBeatsFallback(t *testing.T) {
	t.Parallel()
	client, _, _ := newPromptWireClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.GetPrompt(ctx, "greeting",
		langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Text: "fb"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetPrompt() error = %v, want context.Canceled (fallback must not mask cancellation)", err)
	}
}

func TestPromptWireStaleHitIgnoresCanceledContext(t *testing.T) {
	t.Parallel()
	client, _, _ := newPromptWireClient(t)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prompt, err := client.GetPrompt(ctx, "greeting", langfuse.PromptQuery{})
	if err != nil || prompt.Version != 1 {
		t.Fatalf("cache hit with canceled ctx = (%+v, %v), want the local value", prompt, err)
	}
}

func TestPromptWireValidationErrors(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	cases := map[string]struct {
		name  string
		query langfuse.PromptQuery
	}{
		"empty name":          {"", langfuse.PromptQuery{}},
		"oversized name":      {strings.Repeat("n", 501), langfuse.PromptQuery{}},
		"invalid name":        {"\xff", langfuse.PromptQuery{}},
		"negative version":    {"n", langfuse.PromptQuery{Version: -1}},
		"version and label":   {"n", langfuse.PromptQuery{Version: 1, Label: "production"}},
		"oversized label":     {"n", langfuse.PromptQuery{Label: strings.Repeat("l", 201)}},
		"negative ttl":        {"n", langfuse.PromptQuery{CacheTTL: -time.Second}},
		"fallback both":       {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Text: "t", Messages: []langfuse.PromptMessage{{Role: "user"}}}}},
		"fallback type clash": {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Type: langfuse.PromptTypeChat, Text: "t"}}},
		"fallback bad type":   {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Type: "image"}}},
		"fallback bad config": {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Text: "t", Config: json.RawMessage("{")}}},
		"fallback bad message": {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{
			Messages: []langfuse.PromptMessage{{Content: "no role"}},
		}}},
		"fallback placeholder extra": {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{
			Messages: []langfuse.PromptMessage{{PlaceholderName: "h", Role: "user"}},
		}}},
		"fallback extra not object": {"n", langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{
			Messages: []langfuse.PromptMessage{{Role: "user", Extra: json.RawMessage(`[1]`)}},
		}}},
	}
	for label, test := range cases {
		if _, err := client.GetPrompt(context.Background(), test.name, test.query); err == nil {
			t.Errorf("GetPrompt(%s) error = nil, want a validation error", label)
		}
	}
	if got := receiver.count(); got != 0 {
		t.Fatalf("request count = %d, want 0 (validation must precede I/O)", got)
	}
}

func TestPromptWireDisabledAndNilClients(t *testing.T) {
	t.Parallel()
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatalf("langfuse.New(disabled) error = %v", err)
	}
	fallback := &langfuse.PromptFallback{Text: "fb"}
	for label, client := range map[string]*langfuse.Client{"disabled": disabled, "nil": nil} {
		prompt, err := client.GetPrompt(context.Background(), "greeting",
			langfuse.PromptQuery{Fallback: fallback})
		if err != nil || !prompt.Fallback || prompt.Text != "fb" {
			t.Fatalf("%s client with fallback = (%+v, %v), want the fallback", label, prompt, err)
		}
		if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err == nil {
			t.Fatalf("%s client without fallback error = nil, want an error", label)
		}
		if _, err := client.GetPrompt(context.Background(), "", langfuse.PromptQuery{}); err == nil {
			t.Fatalf("%s client with invalid query error = nil, want a validation error", label)
		}
	}
}

func TestPromptWireNilContext(t *testing.T) {
	t.Parallel()
	client, _, _ := newPromptWireClient(t)
	//nolint:staticcheck // deliberately exercising the nil-context contract.
	if _, err := client.GetPrompt(nil, "greeting", langfuse.PromptQuery{}); err == nil {
		t.Fatal("GetPrompt(nil ctx) error = nil, want an error")
	}
}

func TestPromptWireShutdownCancelsFlightsAndRejects(t *testing.T) {
	t.Parallel()
	receiver := &promptWireReceiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:   server.URL,
		PublicKey: "pk-lf-prompt-wire",
		SecretKey: "sk-lf-prompt-wire",
	})
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	var inFlight atomic.Int32
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		inFlight.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusInternalServerError)
	})

	fetchErr := make(chan error, 1)
	go func() {
		_, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
		fetchErr <- err
	}()
	awaitPromptCondition(t, "flight in progress", func() bool { return inFlight.Load() == 1 })

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := client.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Shutdown() took %v, want the canceled flight to drain promptly", elapsed)
	}
	select {
	case err := <-fetchErr:
		if err == nil {
			t.Fatal("GetPrompt() during shutdown error = nil, want a failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight GetPrompt never returned after Shutdown")
	}
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err == nil {
		t.Fatal("GetPrompt() after Shutdown error = nil, want an error")
	}
	prompt, err := client.GetPrompt(context.Background(), "greeting",
		langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Text: "fb"}})
	if err != nil || !prompt.Fallback {
		t.Fatalf("GetPrompt() after Shutdown with fallback = (%+v, %v), want the fallback", prompt, err)
	}
	close(release)
}

func TestPromptWireReturnedPromptIsIsolatedFromCache(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, _ int) {
		_, _ = fmt.Fprint(w, `{
			"name": "greeting", "version": 1, "type": "chat",
			"labels": ["production"], "tags": ["a"],
			"config": {"k": "v"},
			"prompt": [{"role": "user", "content": "hi", "extra_field": 1}]
		}`)
	})

	first, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	first.Labels[0] = "mutated"
	first.Tags[0] = "mutated"
	first.Config[1] = 'X'
	first.Messages[0].Content = "mutated"
	first.Messages[0].Extra[1] = 'X'

	second, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil {
		t.Fatalf("GetPrompt() second call error = %v", err)
	}
	if second.Labels[0] != "production" || second.Tags[0] != "a" ||
		string(second.Config) != `{"k": "v"}` || second.Messages[0].Content != "hi" ||
		string(second.Messages[0].Extra) != `{"extra_field":1}` {
		t.Fatalf("cached prompt was mutated through a returned copy: %+v", second)
	}
	if got := receiver.count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}
