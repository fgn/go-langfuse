package langfuse

import (
	"time"

	"go.opentelemetry.io/otel"
)

// SetTestErrorHandler installs an OpenTelemetry error handler for the process
// and returns a function that restores the previous one, so a test can
// observe payload-free diagnostics or exercise a reentrant handler.
func SetTestErrorHandler(handler func(error)) func() {
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(handler))
	return func() { otel.SetErrorHandler(previous) }
}

// SDKVersion exposes the internal version constant so external wire tests can
// assert the exact x-langfuse-sdk-version header value.
const SDKVersion = sdkVersion

// SetPromptClock replaces the prompt cache's clock so cache-freshness tests
// need no sleeps. Call it before the first GetPrompt; now must be safe for
// concurrent use.
func SetPromptClock(c *Client, now func() time.Time) {
	c.prompts.now = now
}

// PromptRefreshCooldown exposes the internal refresh-failure cooldown for
// clock-driven cache tests.
const PromptRefreshCooldown = promptRefreshCooldown

// MaxPromptCacheEntries and MaxPromptForeground expose the internal cache and
// foreground-fetch bounds for cardinality tests.
const (
	MaxPromptCacheEntries = maxPromptCacheEntries
	MaxPromptForeground   = maxPromptForeground
	MaxPromptRefreshes    = maxPromptRefreshes
)

// SetPromptRefreshCommitHook installs a hook fired inside a background
// refresh after its fetch returns and before it commits, so a test can drive
// the evict-then-miss schedule the generation guard defends against.
func SetPromptRefreshCommitHook(c *Client, hook func()) {
	c.prompts.refreshCommitHook = func(promptKey) { hook() }
}

// ReplaceProductionPromptEntry evicts the production-label entry for name and
// installs a fresh entry with a distinct pointer and the given version,
// simulating an eviction and re-population that raced a slow refresh.
func ReplaceProductionPromptEntry(c *Client, name string, version int) {
	pc := c.prompts
	pc.mu.Lock()
	defer pc.mu.Unlock()
	key := promptKey{name: name, label: defaultPromptLabel}
	if entry, ok := pc.entries[key]; ok {
		pc.removeEntryLocked(key, entry)
	}
	pc.storeLocked(key, Prompt{Name: name, Version: version})
}

// ProductionPromptCached reports whether a production-label entry for name is
// currently cached, for eviction tests.
func ProductionPromptCached(c *Client, name string) bool {
	pc := c.prompts
	pc.mu.Lock()
	defer pc.mu.Unlock()
	_, ok := pc.entries[promptKey{name: name, label: defaultPromptLabel}]
	return ok
}

// PromptCacheEntryCount returns the number of cached entries, for LRU bound
// tests.
func PromptCacheEntryCount(c *Client) int {
	pc := c.prompts
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.entries)
}

// ProductionFlightWaiters returns the current waiter count on the in-flight
// fetch for name's production key, or -1 when no flight is running, so tests
// can wait for callers to park in the flight instead of sleeping.
func ProductionFlightWaiters(c *Client, name string) int {
	pc := c.prompts
	pc.mu.Lock()
	defer pc.mu.Unlock()
	flight, ok := pc.flights[promptKey{name: name, label: defaultPromptLabel}]
	if !ok {
		return -1
	}
	return flight.waiters
}

// ProductionPromptCoolingDown reports whether name's production entry is
// within its post-failure refresh cooldown, so tests can wait for a failed
// refresh to record cooldown instead of sleeping.
func ProductionPromptCoolingDown(c *Client, name string) bool {
	pc := c.prompts
	pc.mu.Lock()
	defer pc.mu.Unlock()
	entry, ok := pc.entries[promptKey{name: name, label: defaultPromptLabel}]
	if !ok {
		return false
	}
	return pc.now().Before(entry.cooldownUntil)
}
