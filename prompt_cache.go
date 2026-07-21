package langfuse

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fgn/go-langfuse/internal/diagnostic"
	"github.com/fgn/go-langfuse/internal/transport"
)

const (
	// maxPromptCacheEntries caps the cache; keys are code-authored
	// (name × version/label), so 256 far exceeds real cardinality while
	// bounding request-derived abuse. LRU eviction is not data loss and
	// emits no diagnostic.
	maxPromptCacheEntries = 256
	// maxPromptRefreshes bounds concurrent background refreshes; one skipped
	// at the cap retries on a later stale hit.
	maxPromptRefreshes = 8
	// maxPromptForeground bounds concurrent foreground fetches (miss flights
	// and cache-disabled fetches); at the cap a fetch fails immediately and
	// the fallback rules apply.
	maxPromptForeground = 64
	// promptRefreshCooldown suppresses refresh admission for a key after a
	// failed refresh, so a fast failure cannot be retried once per request.
	promptRefreshCooldown = 10 * time.Second
	// maxConcurrentPromptDiagnostics mirrors the scores pipeline's bound on
	// concurrently running application-pluggable error handlers.
	maxConcurrentPromptDiagnostics = 4
)

// promptKey identifies one cache entry: an exact version when version > 0,
// a deployment label otherwise.
type promptKey struct {
	name    string
	version int
	label   string
}

// promptEntry is one cached prompt. The master is immutable by construction:
// commits replace it wholesale and every reader deep-copies, so a shallow
// struct copy under the mutex is safe.
type promptEntry struct {
	master     Prompt
	fetchedAt  time.Time
	generation uint64
	refreshing bool
	// cooldownUntil suppresses refresh admission after a failed refresh.
	cooldownUntil time.Time
	element       *list.Element
}

// promptFlight is one in-progress miss fetch shared by concurrent callers of
// the same key. Its request runs on the client lifecycle context with the
// fetch budget — never any single caller's context — so one caller canceling
// cannot fail the fetch for the others. When the last waiter departs the
// flight is canceled; its map reservation stays until the worker exits, so a
// later caller can never attach to a canceled flight.
type promptFlight struct {
	done      chan struct{}
	prompt    Prompt
	err       error
	waiters   int
	abandoned bool
	cancel    context.CancelFunc
}

// promptCache serves GetPrompt: TTL freshness with stale-while-revalidate,
// per-key miss singleflight, bounded background refresh with failure
// cooldown, LRU eviction, and a single admission gate that lets Shutdown
// cancel and drain every class of prompt I/O.
type promptCache struct {
	fetcher *transport.PromptsClient
	// now is a test seam; production uses time.Now.
	now func() time.Time
	// refreshCommitHook is a test seam fired inside refresh after the fetch
	// returns and before the commit lock, so a test can drive the exact
	// evict-then-miss schedule the generation guard defends against. Nil in
	// production.
	refreshCommitHook func(promptKey)
	diagSlots         chan struct{}

	// newFetchContext derives one fetch's context: bounded by the fetch
	// budget, canceled by client shutdown, and — when parent is non-nil — by
	// that caller context too. Background work passes nil to run on the
	// client lifecycle alone. The closure owns the lifecycle context, so no
	// context lives in the struct.
	newFetchContext func(parent context.Context) (context.Context, context.CancelFunc)
	cancelLifecycle context.CancelFunc
	// stop is closed when the client lifecycle ends.
	stop <-chan struct{}

	mu         sync.Mutex
	closing    bool
	entries    map[promptKey]*promptEntry
	lru        *list.List // front = most recently used; values are promptKey
	flights    map[promptKey]*promptFlight
	refreshing int
	foreground int
	wg         sync.WaitGroup
}

func newPromptCache(fetcher *transport.PromptsClient) *promptCache {
	lifecycle, cancel := context.WithCancel(context.Background())
	cache := &promptCache{
		fetcher:         fetcher,
		now:             time.Now,
		diagSlots:       make(chan struct{}, maxConcurrentPromptDiagnostics),
		cancelLifecycle: cancel,
		stop:            lifecycle.Done(),
		entries:         make(map[promptKey]*promptEntry),
		lru:             list.New(),
		flights:         make(map[promptKey]*promptFlight),
	}
	cache.newFetchContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		if parent == nil {
			parent = lifecycle
		}
		fetchCtx, cancelFetch := context.WithTimeout(parent, promptFetchBudget)
		detach := context.AfterFunc(lifecycle, cancelFetch)
		return fetchCtx, func() { detach(); cancelFetch() }
	}
	return cache
}

// lifecycleEnded reports whether client shutdown has canceled prompt I/O.
func (pc *promptCache) lifecycleEnded() bool {
	select {
	case <-pc.stop:
		return true
	default:
		return false
	}
}

var errPromptShutdown = errors.New("langfuse: prompt requested after client shutdown")

func (pc *promptCache) get(ctx context.Context, name string, query PromptQuery, fallback *promptFallbackValue) (Prompt, error) {
	ttl := query.CacheTTL
	if ttl == 0 {
		ttl = defaultPromptCacheTTL
	}
	key := promptKey{name: name, version: query.Version}
	if query.Version == 0 {
		key.label = query.Label
		if key.label == "" {
			key.label = defaultPromptLabel
		}
	}
	if query.DisableCache {
		return pc.fetchDirect(ctx, key, query.Version, fallback)
	}

	for {
		pc.mu.Lock()
		if entry, ok := pc.entries[key]; ok {
			// Fresh and stale hits are local memory reads and deliberately
			// ignore ctx cancellation; a stale hit additionally admits one
			// background refresh.
			pc.lru.MoveToFront(entry.element)
			master := entry.master
			if pc.now().Sub(entry.fetchedAt) > ttl {
				pc.maybeRefreshLocked(key, entry)
			}
			pc.mu.Unlock()
			return deepCopyPrompt(master), nil
		}
		pc.mu.Unlock()
		// A miss is a blocking path, so caller cancellation wins over any
		// admission outcome: check it before admitting, and before resolving
		// an admission error, so a pre-canceled caller never receives a
		// fallback or the overload error in place of its context error.
		if err := ctx.Err(); err != nil {
			return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", err)
		}
		pc.mu.Lock()
		// Re-check the entry: a concurrent flight may have committed it while
		// the lock was released.
		if _, ok := pc.entries[key]; ok {
			pc.mu.Unlock()
			continue
		}
		flight, joined, err := pc.joinOrStartFlightLocked(key)
		pc.mu.Unlock()
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", cerr)
			}
			return promptFromFallback(name, query.Version, fallback, err)
		}
		if !joined {
			// The existing flight was abandoned (its context is canceled).
			// Wait for its worker to release the key, then retry admission —
			// rechecking cancellation before looping so a caller whose
			// context ended does not consume the newly cached value.
			select {
			case <-flight.done:
				if cerr := ctx.Err(); cerr != nil {
					return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", cerr)
				}
				continue
			case <-ctx.Done():
				return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", ctx.Err())
			}
		}
		return pc.awaitFlight(ctx, name, query.Version, flight, fallback)
	}
}

// joinOrStartFlightLocked admits this caller to the key's flight. It returns
// joined=false for an abandoned flight the caller must wait out, an error
// when the cache is closing or the foreground cap is reached, and otherwise
// a flight this caller has been counted into.
func (pc *promptCache) joinOrStartFlightLocked(key promptKey) (flight *promptFlight, joined bool, err error) {
	if pc.closing {
		return nil, false, errPromptShutdown
	}
	if flight, ok := pc.flights[key]; ok {
		if flight.abandoned {
			return flight, false, nil
		}
		flight.waiters++
		return flight, true, nil
	}
	if pc.foreground >= maxPromptForeground {
		return nil, false, errors.New("langfuse: too many concurrent prompt fetches")
	}
	fetchCtx, cancel := pc.newFetchContext(nil)
	flight = &promptFlight{
		done:    make(chan struct{}),
		waiters: 1,
		cancel:  cancel,
	}
	pc.flights[key] = flight
	pc.foreground++
	pc.wg.Add(1)
	go pc.runFlight(fetchCtx, key, flight)
	return flight, true, nil
}

// runFlight performs the shared miss fetch and commits a fresh cache entry
// on success. The flight's map reservation is released only here, after the
// result is published, preserving one flight per key.
func (pc *promptCache) runFlight(ctx context.Context, key promptKey, flight *promptFlight) {
	defer pc.wg.Done()
	defer flight.cancel()
	wire, err := pc.fetcher.Fetch(ctx, key.name, key.version, key.label)
	pc.mu.Lock()
	if err == nil {
		flight.prompt = promptFromWire(wire)
		pc.storeLocked(key, flight.prompt)
	} else {
		flight.err = err
	}
	delete(pc.flights, key)
	pc.foreground--
	close(flight.done)
	pc.mu.Unlock()
}

// awaitFlight waits for the shared fetch while honoring the caller's own
// context: cancellation deterministically wins over a concurrently available
// result or fallback, so work never continues after request teardown.
func (pc *promptCache) awaitFlight(ctx context.Context, name string, version int, flight *promptFlight, fallback *promptFallbackValue) (Prompt, error) {
	if err := ctx.Err(); err != nil {
		pc.leaveFlight(flight)
		return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", err)
	}
	select {
	case <-flight.done:
		if err := ctx.Err(); err != nil {
			return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", err)
		}
		if flight.err != nil {
			return promptFromFallback(name, version, fallback, wrapPromptError(name, flight.err))
		}
		return deepCopyPrompt(flight.prompt), nil
	case <-ctx.Done():
		pc.leaveFlight(flight)
		return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", ctx.Err())
	}
}

// leaveFlight departs a waiter; the last departure cancels the flight so no
// orphaned fetch runs its full budget for nobody.
func (pc *promptCache) leaveFlight(flight *promptFlight) {
	pc.mu.Lock()
	flight.waiters--
	if flight.waiters == 0 {
		flight.abandoned = true
		flight.cancel()
	}
	pc.mu.Unlock()
}

// fetchDirect serves DisableCache: an independent fetch with no cache read,
// write, or flight sharing, canceled by whichever of the caller's context,
// the fetch budget, or the client lifecycle ends first.
func (pc *promptCache) fetchDirect(ctx context.Context, key promptKey, version int, fallback *promptFallbackValue) (Prompt, error) {
	// Caller cancellation wins over admission: a pre-canceled caller returns
	// its context error rather than a fallback or the overload error.
	if err := ctx.Err(); err != nil {
		return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", err)
	}
	pc.mu.Lock()
	if pc.closing {
		pc.mu.Unlock()
		return promptFromFallback(key.name, version, fallback, errPromptShutdown)
	}
	if pc.foreground >= maxPromptForeground {
		pc.mu.Unlock()
		if cerr := ctx.Err(); cerr != nil {
			return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", cerr)
		}
		return promptFromFallback(key.name, version, fallback,
			errors.New("langfuse: too many concurrent prompt fetches"))
	}
	pc.foreground++
	pc.wg.Add(1)
	pc.mu.Unlock()
	defer func() {
		pc.mu.Lock()
		pc.foreground--
		pc.mu.Unlock()
		pc.wg.Done()
	}()

	fetchCtx, cancel := pc.newFetchContext(ctx)
	defer cancel()
	wire, err := pc.fetcher.Fetch(fetchCtx, key.name, key.version, key.label)
	// Check the caller context unconditionally before returning: a response
	// that completes concurrently with cancellation must not be handed back
	// after the caller has already given up.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return Prompt{}, fmt.Errorf("langfuse: prompt fetch canceled: %w", ctxErr)
	}
	if err != nil {
		return promptFromFallback(key.name, version, fallback, wrapPromptError(key.name, err))
	}
	// The result is unshared, so no defensive copy is needed.
	return promptFromWire(wire), nil
}

// storeLocked installs a fresh entry and evicts least-recently-used entries
// beyond the cap.
func (pc *promptCache) storeLocked(key promptKey, master Prompt) {
	entry := &promptEntry{
		master:     master,
		fetchedAt:  pc.now(),
		generation: 1,
	}
	entry.element = pc.lru.PushFront(key)
	pc.entries[key] = entry
	for len(pc.entries) > maxPromptCacheEntries {
		oldest := pc.lru.Back()
		if oldest == nil {
			return
		}
		oldestKey := oldest.Value.(promptKey)
		pc.removeEntryLocked(oldestKey, pc.entries[oldestKey])
	}
}

func (pc *promptCache) removeEntryLocked(key promptKey, entry *promptEntry) {
	if entry == nil {
		return
	}
	pc.lru.Remove(entry.element)
	delete(pc.entries, key)
}

// maybeRefreshLocked admits one background refresh for a stale entry when no
// refresh for this key is running, the key is not cooling down after a
// failure, the global refresh cap has room, and the cache is not closing.
func (pc *promptCache) maybeRefreshLocked(key promptKey, entry *promptEntry) {
	if entry.refreshing || pc.closing || pc.refreshing >= maxPromptRefreshes {
		return
	}
	if pc.now().Before(entry.cooldownUntil) {
		return
	}
	entry.refreshing = true
	pc.refreshing++
	pc.wg.Add(1)
	go pc.refresh(key, entry, entry.generation)
}

// refresh re-fetches one stale entry on the lifecycle context. Its commit is
// generation-guarded: every mutation — a successful store, a 404 eviction,
// or cooldown state — applies only while the exact refreshed entry is still
// installed, so a slow refresh can neither overwrite a newer result nor
// resurrect or evict a replacement.
func (pc *promptCache) refresh(key promptKey, entry *promptEntry, generation uint64) {
	defer pc.wg.Done()
	fetchCtx, cancel := pc.newFetchContext(nil)
	defer cancel()
	wire, err := pc.fetcher.Fetch(fetchCtx, key.name, key.version, key.label)
	if pc.refreshCommitHook != nil {
		pc.refreshCommitHook(key)
	}

	pc.mu.Lock()
	pc.refreshing--
	current, ok := pc.entries[key]
	if !ok || current != entry || current.generation != generation {
		pc.mu.Unlock()
		return
	}
	entry.refreshing = false
	switch {
	case err == nil:
		entry.master = promptFromWire(wire)
		entry.fetchedAt = pc.now()
		entry.generation++
		pc.mu.Unlock()
	case errors.Is(err, transport.ErrPromptNotFound):
		// An authoritative deletion or label removal: evict, so the next
		// call is a miss that resolves to the fallback or ErrPromptNotFound.
		pc.removeEntryLocked(key, entry)
		pc.mu.Unlock()
		pc.reportAsync("cached prompt evicted: not found during refresh")
	case pc.lifecycleEnded():
		// Shutdown cancellation: no cooldown, no diagnostic.
		pc.mu.Unlock()
	default:
		// Transient and terminal failures cool down alike: retrying a
		// deterministic failure on every stale hit would never converge.
		entry.cooldownUntil = pc.now().Add(promptRefreshCooldown)
		pc.mu.Unlock()
		pc.reportAsync("prompt refresh failed; serving the cached version")
	}
}

// beginShutdown stops admission and cancels all prompt I/O. It runs before
// the potentially blocking OpenTelemetry teardown in Client.Shutdown so no
// prompt work can start once shutdown has begun.
func (pc *promptCache) beginShutdown() {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.closing = true
	pc.mu.Unlock()
	pc.cancelLifecycle()
}

// shutdown drains flights, refreshes, and cache-disabled fetches, bounded by
// ctx. The workers self-terminate within the fetch budget because their
// contexts are already canceled.
func (pc *promptCache) shutdown(ctx context.Context) error {
	if pc == nil {
		return nil
	}
	pc.beginShutdown()
	done := make(chan struct{})
	go func() {
		pc.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("langfuse: prompt shutdown: %w", ctx.Err())
	}
}

// reportAsync mirrors the scores pipeline's bounded, reentrancy-safe
// diagnostics: the application-pluggable error handler never runs on a
// prompt worker goroutine, so a handler that re-enters Shutdown cannot
// deadlock the drain, and concurrent reports are bounded.
func (pc *promptCache) reportAsync(message string) {
	select {
	case pc.diagSlots <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-pc.diagSlots }()
		diagnostic.Report(message)
	}()
}
