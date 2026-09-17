package langfuse

// InvalidatePromptCache removes every cached version and label selector for
// name from this runtime client. It is safe on nil, disabled, and shut-down
// clients. It performs no network I/O and does not change other clients.
//
// Call it after a successful management write, label move, rollback, or deletion.
// The next cached read is a blocking miss rather than a stale-cache hit. Normal
// cancellation, fetch limits, and explicitly supplied fallback rules still apply.
// In-flight reads may finish with their earlier result or a cancellation error,
// but cannot repopulate the invalidated cache. Subsequent reads do not join an
// older miss flight; they wait for it to drain before fetching again.
//
// This does not invalidate prompts that compose name as a dependency. Invalidate
// each affected parent name explicitly, or allow its normal TTL to expire.
func (c *Client) InvalidatePromptCache(name string) {
	if c == nil || c.prompts == nil {
		return
	}
	pc := c.prompts
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for key, entry := range pc.entries {
		if key.name == name {
			pc.removeEntryLocked(key, entry)
		}
	}
	// Keep each reservation until its worker exits so invalidation cannot
	// bypass foreground admission limits or create an unbounded flight set.
	for key, flight := range pc.flights {
		if key.name == name {
			flight.abandoned = true
			flight.cancel()
		}
	}
}
