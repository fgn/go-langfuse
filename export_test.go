package langfuse

import "time"

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
