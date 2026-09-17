package langfuse_test

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

func TestInvalidatePromptCacheNilDisabledAndShutdown(t *testing.T) {
	var nilClient *langfuse.Client
	nilClient.InvalidatePromptCache("greeting")
	new(langfuse.Client).InvalidatePromptCache("greeting")
	disabled, err := langfuse.New(t.Context(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	disabled.InvalidatePromptCache("greeting")
	client, _, _ := newPromptWireClient(t)
	if err := client.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	client.InvalidatePromptCache("greeting")
}

func TestInvalidatePromptCacheAllSelectorsAndExactName(t *testing.T) {
	client, receiver, _ := newPromptWireClient(t)
	var version atomic.Int32
	version.Store(1)
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, _ int) {
		v := int(version.Load())
		if requested := r.URL.Query().Get("version"); requested != "" {
			var err error
			v, err = strconv.Atoi(requested)
			if err != nil {
				t.Error(err)
				return
			}
		}
		writePromptWire(w, strings.TrimPrefix(r.URL.Path, "/api/public/v2/prompts/"), v, "versioned")
	})
	const name = "folder/greeting"
	queries := []langfuse.PromptQuery{{}, {Label: "staging"}, {Version: 1}}
	for _, query := range queries {
		if _, err := client.GetPrompt(t.Context(), name, query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.GetPrompt(t.Context(), name+"-other", langfuse.PromptQuery{}); err != nil {
		t.Fatal(err)
	}
	version.Store(2)
	client.InvalidatePromptCache(name)
	if got := langfuse.PromptCacheEntryCount(client); got != 1 {
		t.Fatalf("cache retained %d entries, want only the unrelated name", got)
	}
	for _, query := range queries {
		prompt, err := client.GetPrompt(t.Context(), name, query)
		wantVersion := 2
		if query.Version != 0 {
			wantVersion = query.Version
		}
		if err != nil || prompt.Source != langfuse.PromptSourceServer || prompt.Version != wantVersion {
			t.Fatalf("fresh read: %+v, %v", prompt, err)
		}
	}
	before := receiver.count()
	other, err := client.GetPrompt(t.Context(), name+"-other", langfuse.PromptQuery{})
	if err != nil || other.Source != langfuse.PromptSourceCache || receiver.count() != before {
		t.Fatalf("unrelated prompt was invalidated: %+v, %v", other, err)
	}
}

func TestInvalidatePromptCacheDiscardsLateMissCommit(t *testing.T) {
	client, receiver, _ := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		writePromptWire(w, "greeting", call, "versioned")
	})
	var once sync.Once
	langfuse.SetPromptFlightCommitHook(client, func() {
		// The response has already been successfully decoded. Cancellation
		// alone cannot prevent that old response from committing.
		once.Do(func() { client.InvalidatePromptCache("greeting") })
	})
	first, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{})
	if err != nil || first.Version != 1 {
		t.Fatalf("existing waiter lost its completed response: %+v, %v", first, err)
	}
	if langfuse.ProductionPromptCached(client, "greeting") {
		t.Fatal("invalidated miss repopulated the cache")
	}
	second, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{})
	if err != nil || second.Version != 2 || second.Source != langfuse.PromptSourceServer {
		t.Fatalf("next read joined or reused the old flight: %+v, %v", second, err)
	}
}

func TestInvalidatePromptCacheDiscardsLateRefreshCommit(t *testing.T) {
	client, receiver, clock := newPromptWireClient(t)
	receiver.setHandler(func(w http.ResponseWriter, _ *http.Request, call int) {
		writePromptWire(w, "greeting", call, "versioned")
	})
	committed := make(chan error, 1)
	langfuse.SetPromptRefreshCommitHook(client, func() {
		// Invalidate after refresh decoded v2, and populate v3 before v2's
		// commit. The old entry's identity must not match the new one.
		client.InvalidatePromptCache("greeting")
		_, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{})
		committed <- err
	})
	if _, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	if _, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-committed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not reach its commit barrier")
	}
	// Observe the completed commit under the cache lock before asserting.
	awaitPromptCondition(t, "old refresh completed", func() bool {
		return langfuse.PromptRefreshCount(client) == 0
	})
	prompt, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{})
	if err != nil || prompt.Version != 3 || prompt.Source != langfuse.PromptSourceCache {
		t.Fatalf("old refresh replaced the fresh value: %+v, %v", prompt, err)
	}
}

func TestInvalidatePromptCacheDoesNotJoinOldMiss(t *testing.T) {
	client, receiver, _ := newPromptWireClient(t)
	started := make(chan struct{})
	receiver.setHandler(func(w http.ResponseWriter, r *http.Request, call int) {
		if call == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		writePromptWire(w, "greeting", 2, "new")
	})
	old := make(chan error, 1)
	go func() {
		_, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{})
		old <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("miss never started")
	}
	client.InvalidatePromptCache("greeting")
	fresh, err := client.GetPrompt(t.Context(), "greeting", langfuse.PromptQuery{})
	if err != nil || fresh.Version != 2 || fresh.Source != langfuse.PromptSourceServer {
		t.Fatalf("new reader joined the canceled miss: %+v, %v", fresh, err)
	}
	select {
	case err := <-old:
		if err == nil {
			t.Fatal("old unfinished request was not canceled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("invalidated miss failed to drain")
	}
}
