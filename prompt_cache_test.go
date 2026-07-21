package langfuse_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// promptNameFromRequest recovers the requested prompt name from a wire
// request path.
func promptNameFromRequest(r *http.Request) string {
	return strings.TrimPrefix(r.URL.EscapedPath(), "/api/public/v2/prompts/")
}

// TestPromptCacheGenerationGuardDiscardsStaleRefresh drives the exact
// evict-then-miss schedule the generation guard defends against: a refresh
// fetches a new value, the entry it refreshed is replaced underneath it, and
// its result must be discarded rather than overwriting the replacement.
func TestPromptCacheGenerationGuardDiscardsStaleRefresh(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		// call 1: initial v1. call 2: the refresh fetches v2 (to be discarded).
		writePromptWire(w, "greeting", call, "v")
	})

	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	// The commit hook fires after the refresh fetches v2 but before it
	// commits; replace the entry with a distinct v99 pointer so the guard
	// sees a mismatch and discards v2.
	var once sync.Once
	langfuse.SetPromptRefreshCommitHook(client, func() {
		once.Do(func() {
			langfuse.ReplaceProductionPromptEntry(client, "greeting", 99)
		})
	})

	clock.Advance(2 * time.Minute)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() stale error = %v", err)
	}
	awaitPromptCondition(t, "refresh completed", func() bool { return receiver.count() == 2 })
	// Give the discarded commit a moment; the replacement (v99) must win.
	awaitPromptCondition(t, "replacement survives the discarded refresh", func() bool {
		prompt, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
		return err == nil && prompt.Version == 99
	})
	prompt, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil || prompt.Version != 99 {
		t.Fatalf("prompt after discarded refresh = (%+v, %v), want the v99 replacement", prompt, err)
	}
}

func TestPromptCacheConcurrentStaleHitsDedupeRefresh(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		if call >= 2 {
			<-release // hold the single refresh open while stale hits pile up
		}
		writePromptWire(w, "greeting", 1, "v")
	})

	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	clock.Advance(2 * time.Minute)

	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
				t.Errorf("stale GetPrompt() error = %v", err)
			}
		})
	}
	wg.Wait() // every stale hit returned the cached value immediately

	// The initial fetch was request 1; exactly one refresh was admitted, so
	// wait for that (blocked) second request.
	awaitPromptCondition(t, "single refresh request", func() bool { return receiver.count() == 2 })
	// The refresh is still in flight (blocked), so further stale hits find the
	// refreshing flag set and admit no additional refresh.
	for range 5 {
		if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
			t.Fatalf("stale GetPrompt() error = %v", err)
		}
	}
	if got := receiver.count(); got != 2 {
		t.Fatalf("request count = %d, want 2 (concurrent stale hits share one refresh)", got)
	}
	close(release)
}

func TestPromptCacheRefreshCapBoundsConcurrency(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	var inFlight, peak atomic.Int32
	release := make(chan struct{})

	// Seed more distinct keys than the refresh cap with a non-blocking
	// handler.
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		writePromptWire(w, promptNameFromRequest(r), 1, "v")
	})
	const keys = langfuse.MaxPromptRefreshes + 4
	names := make([]string, keys)
	for i := range names {
		names[i] = fmt.Sprintf("prompt-%d", i)
		if _, err := client.GetPrompt(context.Background(), names[i], langfuse.PromptQuery{}); err != nil {
			t.Fatalf("seed GetPrompt(%s) error = %v", names[i], err)
		}
	}
	// Reconfigure so every refresh blocks and reports its concurrency.
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		cur := inFlight.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		writePromptWire(w, promptNameFromRequest(r), 1, "v")
	})

	clock.Advance(2 * time.Minute)
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Go(func() {
			_, _ = client.GetPrompt(context.Background(), name, langfuse.PromptQuery{})
		})
	}
	wg.Wait() // stale hits return immediately

	awaitPromptCondition(t, "refreshes reach the cap", func() bool {
		return inFlight.Load() >= int32(langfuse.MaxPromptRefreshes)
	})
	time.Sleep(20 * time.Millisecond) // allow any over-cap admission to surface
	if got := peak.Load(); got > int32(langfuse.MaxPromptRefreshes) {
		t.Fatalf("peak concurrent refreshes = %d, want at most %d", got, langfuse.MaxPromptRefreshes)
	}
	close(release)
}

func TestPromptCacheForegroundCapRejects(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	var inFlight atomic.Int32
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		inFlight.Add(1)
		<-release
		writePromptWire(w, strings.TrimPrefix(r.URL.Path, "/api/public/v2/prompts/"), 1, "v")
	})

	const capacity = langfuse.MaxPromptForeground
	var wg sync.WaitGroup
	for i := range capacity {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = client.GetPrompt(context.Background(), fmt.Sprintf("p-%d", i), langfuse.PromptQuery{})
		}(i)
	}
	awaitPromptCondition(t, "foreground cap occupied", func() bool {
		return inFlight.Load() == int32(capacity)
	})
	// The next miss exceeds the cap and fails immediately without a fallback.
	_, err := client.GetPrompt(context.Background(), "overflow", langfuse.PromptQuery{})
	if err == nil || !strings.Contains(err.Error(), "too many concurrent prompt fetches") {
		t.Fatalf("over-cap GetPrompt() error = %v, want the overload error", err)
	}
	close(release)
	wg.Wait()
}

func TestPromptCachePreCanceledOverflowReturnsContextError(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	var inFlight atomic.Int32
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		inFlight.Add(1)
		<-release
		writePromptWire(w, strings.TrimPrefix(r.URL.Path, "/api/public/v2/prompts/"), 1, "v")
	})
	const capacity = langfuse.MaxPromptForeground
	var wg sync.WaitGroup
	for i := range capacity {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = client.GetPrompt(context.Background(), fmt.Sprintf("p-%d", i), langfuse.PromptQuery{})
		}(i)
	}
	awaitPromptCondition(t, "foreground cap occupied", func() bool {
		return inFlight.Load() == int32(capacity)
	})
	// A pre-canceled caller hitting the cap must get its context error, never
	// the overload error or a fallback.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.GetPrompt(ctx, "overflow", langfuse.PromptQuery{
		Fallback: &langfuse.PromptFallback{Text: "fb"},
	})
	if !isContextCanceled(err) {
		t.Fatalf("pre-canceled over-cap GetPrompt() error = %v, want context.Canceled", err)
	}
	close(release)
	wg.Wait()
}

func TestPromptCacheLastWaiterAbandonmentThenNewCaller(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, call int) {
		if call == 1 {
			// The first flight blocks until either the last waiter cancels it
			// or the test releases it.
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		writePromptWire(w, "greeting", 1, "v")
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.GetPrompt(ctx, "greeting", langfuse.PromptQuery{})
		done <- err
	}()
	// Wait until the first caller's request is actually blocked in the
	// handler (its request reached the server), so canceling it — not a
	// later caller — is what abandons the flight.
	awaitPromptCondition(t, "first flight blocked in the handler", func() bool {
		return receiver.count() == 1 && langfuse.ProductionFlightWaiters(client, "greeting") == 1
	})
	cancel() // last waiter leaves → flight abandoned and canceled
	if err := <-done; !isContextCanceled(err) {
		t.Fatalf("canceled caller error = %v, want context.Canceled", err)
	}

	// A new caller must not attach to the abandoned flight; it waits for the
	// worker to release the key, then starts a fresh flight.
	prompt, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{})
	if err != nil || prompt.Version != 1 {
		t.Fatalf("new caller after abandonment = (%+v, %v), want a fresh successful fetch", prompt, err)
	}
	if got := receiver.count(); got != 2 {
		t.Fatalf("request count = %d, want 2 (abandoned flight plus a fresh one)", got)
	}
}

func TestPromptCacheDirectFetchShutdownDrains(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	started := make(chan struct{})
	var once sync.Once
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		once.Do(func() { close(started) })
		<-r.Context().Done() // released only by lifecycle cancellation
	})

	done := make(chan error, 1)
	go func() {
		_, err := client.GetPrompt(context.Background(), "greeting",
			langfuse.PromptQuery{DisableCache: true})
		done <- err
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Shutdown() took %v, want the direct fetch to drain promptly", elapsed)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("direct fetch during shutdown error = nil, want a failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("direct fetch never returned after Shutdown")
	}
}

func TestPromptCacheReentrantDiagnosticHandlerDoesNotDeadlock(t *testing.T) {
	// Not parallel: it installs a process-global OpenTelemetry error handler,
	// so a parallel test's diagnostic must not fire it.
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		if call == 1 {
			writePromptWire(w, "greeting", 1, "v")
			return
		}
		w.WriteHeader(http.StatusBadRequest) // failed refresh → diagnostic
	})
	// A diagnostic handler that re-enters Shutdown must not deadlock, since
	// diagnostics run off the prompt worker goroutines. The handler signals
	// both entry and return so the test proves the reentrant Shutdown
	// actually ran to completion rather than passing vacuously.
	entered := make(chan struct{}, 1)
	returned := make(chan struct{}, 1)
	restore := langfuse.SetTestErrorHandler(func(error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
		select {
		case returned <- struct{}{}:
		default:
		}
	})
	t.Cleanup(restore)

	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	clock.Advance(2 * time.Minute)
	// The stale hit admits the refresh, which fails and reports a diagnostic;
	// the test must not begin shutdown first, or the refresh would take its
	// lifecycle-ended branch and never invoke the handler.
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("stale GetPrompt() error = %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the failed refresh never invoked the diagnostic handler")
	}
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the reentrant Shutdown from the diagnostic handler did not complete (deadlock)")
	}
}

func TestPromptCacheTTLBoundaryAndOverride(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		writePromptWire(w, "greeting", call, "v")
	})
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}

	// Age exactly equal to the TTL is fresh: no refresh admitted.
	clock.Advance(time.Minute)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() at TTL boundary error = %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := receiver.count(); got != 1 {
		t.Fatalf("request count = %d, want 1 (age == TTL is still fresh)", got)
	}

	// A shorter per-call TTL makes the same entry stale and refreshes it.
	if _, err := client.GetPrompt(context.Background(), "greeting",
		langfuse.PromptQuery{CacheTTL: 30 * time.Second}); err != nil {
		t.Fatalf("GetPrompt() with short TTL error = %v", err)
	}
	awaitPromptCondition(t, "short TTL forces a refresh", func() bool { return receiver.count() == 2 })
}

func TestPromptCacheLRUEvictionAtCap(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		writePromptWire(w, strings.TrimPrefix(r.URL.Path, "/api/public/v2/prompts/"), 1, "v")
	})
	const total = langfuse.MaxPromptCacheEntries + 5
	for i := range total {
		if _, err := client.GetPrompt(context.Background(), fmt.Sprintf("p-%d", i), langfuse.PromptQuery{}); err != nil {
			t.Fatalf("GetPrompt(p-%d) error = %v", i, err)
		}
	}
	if got := langfuse.PromptCacheEntryCount(client); got != langfuse.MaxPromptCacheEntries {
		t.Fatalf("cache entry count = %d, want the %d cap", got, langfuse.MaxPromptCacheEntries)
	}
	// The oldest key was evicted; fetching it again is a fresh request.
	before := receiver.count()
	if _, err := client.GetPrompt(context.Background(), "p-0", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt(p-0) error = %v", err)
	}
	if got := receiver.count(); got != before+1 {
		t.Fatalf("request count = %d, want %d (evicted key refetched)", got, before+1)
	}
}

func TestPromptCacheTransientRefreshCoolsDown(t *testing.T) {
	t.Parallel()
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		if call == 1 {
			writePromptWire(w, "greeting", 1, "v")
			return
		}
		// Transient 503 on refresh: the transport does not retry a background
		// refresh beyond its budget here (server never recovers), so the
		// refresh fails and the key must cool down like a terminal failure.
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() error = %v", err)
	}
	clock.Advance(2 * time.Minute)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("stale GetPrompt() error = %v", err)
	}
	awaitPromptCondition(t, "transient refresh records cooldown", func() bool {
		return langfuse.ProductionPromptCoolingDown(client, "greeting")
	})
	countAfterFail := receiver.count()
	for range 5 {
		if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
			t.Fatalf("GetPrompt() during cooldown error = %v", err)
		}
	}
	if got := receiver.count(); got != countAfterFail {
		t.Fatalf("request count = %d, want %d (cooldown suppresses refresh)", got, countAfterFail)
	}
	clock.Advance(langfuse.PromptRefreshCooldown + time.Second)
	if _, err := client.GetPrompt(context.Background(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatalf("GetPrompt() after cooldown error = %v", err)
	}
	awaitPromptCondition(t, "refresh resumes after cooldown", func() bool {
		return receiver.count() > countAfterFail
	})
}

func TestPromptCacheFallbackInferenceAndBounds(t *testing.T) {
	t.Parallel()
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatalf("langfuse.New(disabled) error = %v", err)
	}
	// nil Messages, no text → text prompt; non-nil empty Messages → chat.
	textPrompt, err := disabled.GetPrompt(context.Background(), "n",
		langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{}})
	if err != nil || textPrompt.Type != langfuse.PromptTypeText {
		t.Fatalf("empty fallback = (%+v, %v), want an inferred text prompt", textPrompt, err)
	}
	chatPrompt, err := disabled.GetPrompt(context.Background(), "n",
		langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Messages: []langfuse.PromptMessage{}}})
	if err != nil || chatPrompt.Type != langfuse.PromptTypeChat {
		t.Fatalf("empty-slice fallback = (%+v, %v), want an inferred chat prompt", chatPrompt, err)
	}

	// A pathological count of tiny messages is rejected by the structural
	// bound even though each field is short.
	many := make([]langfuse.PromptMessage, 200000)
	for i := range many {
		many[i] = langfuse.PromptMessage{Role: "user", Content: "x"}
	}
	if _, err := disabled.GetPrompt(context.Background(), "n",
		langfuse.PromptQuery{Fallback: &langfuse.PromptFallback{Messages: many}}); err == nil {
		t.Fatal("huge-message-count fallback error = nil, want the size bound to reject it")
	}
}

func TestPromptCacheFallbackSnapshotIsolatedFromCallerMutation(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	release := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, _ int) {
		<-release
		w.WriteHeader(http.StatusBadRequest) // force the fallback path
	})

	messages := []langfuse.PromptMessage{{Role: "system", Content: "original"}}
	fallback := &langfuse.PromptFallback{Messages: messages}
	done := make(chan langfuse.Prompt, 1)
	go func() {
		prompt, err := client.GetPrompt(context.Background(), "greeting",
			langfuse.PromptQuery{Version: 1, Fallback: fallback})
		if err != nil {
			t.Errorf("GetPrompt() with fallback error = %v", err)
		}
		done <- prompt
	}()
	// Once the fetch has started, the synchronous fallback snapshot has
	// already been taken; mutating the caller's slice now must not affect the
	// returned prompt.
	awaitPromptCondition(t, "fetch started", func() bool { return receiver.count() == 1 })
	messages[0].Content = "mutated"
	close(release)
	prompt := <-done
	if prompt.Source != langfuse.PromptSourceFallback || len(prompt.Messages) != 1 || prompt.Messages[0].Content != "original" {
		t.Fatalf("fallback prompt = %+v, want the snapshot taken before mutation", prompt)
	}
}

func TestPromptCacheMultibyteNameAndLabelBounds(t *testing.T) {
	t.Parallel()
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		// Echo the requested label so validation passes.
		label := r.URL.Query().Get("label")
		_, _ = fmt.Fprintf(w, `{"name":%q,"version":1,"type":"text","prompt":"x","labels":[%q]}`,
			strings.TrimPrefix(r.URL.EscapedPath(), "/api/public/v2/prompts/"), label)
	})

	// A 200-rune multibyte label is within bounds and reaches the server.
	label200 := strings.Repeat("é", 200)
	if _, err := client.GetPrompt(context.Background(), "n", langfuse.PromptQuery{Label: label200}); err != nil {
		t.Fatalf("GetPrompt() with 200-rune label error = %v", err)
	}
	// 201 runes exceeds the character bound and is rejected before I/O.
	label201 := strings.Repeat("é", 201)
	if _, err := client.GetPrompt(context.Background(), "n", langfuse.PromptQuery{Label: label201}); err == nil {
		t.Fatal("GetPrompt() with 201-rune label error = nil, want a validation error")
	}
}

// isContextCanceled reports whether err wraps context.Canceled.
func isContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}
