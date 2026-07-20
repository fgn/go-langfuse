package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fastScoreRetry() *RetryConfig {
	return &RetryConfig{
		Enabled:         true,
		InitialInterval: time.Millisecond,
		MaxInterval:     4 * time.Millisecond,
		MaxElapsedTime:  2 * time.Second,
	}
}

func newScoresTestClient(t *testing.T, baseURL string, change func(*Config)) *ScoresClient {
	t.Helper()
	cfg := Config{
		BaseURL:   baseURL,
		PublicKey: "pk-lf-scores",
		SecretKey: "sk-lf-scores",
		Retry:     fastScoreRetry(),
	}
	if change != nil {
		change(&cfg)
	}
	client, err := NewScoresClient(cfg)
	if err != nil {
		t.Fatalf("NewScoresClient() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	return client
}

func flushScores(t *testing.T, client *ScoresClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestScoresRetryTransientFailuresUntilSuccess(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want 3 (two 503 retries then success)", got)
	}
}

func TestScoresDoNotRetryPermanentFailures(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 (a 400 must not be retried)", got)
	}
}

func TestScoresRetryTransientIngestionItemErrors(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		if calls.Add(1) < 3 {
			_, _ = w.Write([]byte(`{"successes":[],"errors":[{"id":"e","status":500}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"successes":[{"id":"e","status":201}],"errors":[]}`))
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want 3 (two 500 item errors then success)", got)
	}
}

func TestScoresDoNotRetryPermanentIngestionItemErrors(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write([]byte(`{"successes":[],"errors":[{"id":"e","status":400,"message":"bad"}]}`))
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 (a 400 item error must not be retried)", got)
	}
}

func TestScoresTreatUnparsableSuccessBodiesAsAccepted(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 (a lenient non-207 2xx body must not be retried)", got)
	}
}

func TestScoresRetryUnreadableMultiStatusResponses(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		if calls.Add(1) < 3 {
			// A 207 body is part of the delivery contract; garbage leaves the
			// outcome unknown and must be retried.
			_, _ = w.Write([]byte(`not json`))
			return
		}
		_, _ = w.Write([]byte(`{"successes":[{"id":"e","status":201}],"errors":[]}`))
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want 3 (two unreadable 207 bodies then success)", got)
	}
}

func TestScoresRetryItemErrorsWithoutStatus(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		if calls.Add(1) < 2 {
			_, _ = w.Write([]byte(`{"successes":[],"errors":[{"id":"e"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"successes":[{"id":"e","status":201}],"errors":[]}`))
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2 (a status-free item error must be retried, not dropped)", got)
	}
}

func TestScoresDisabledRetrySendsOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.Retry = &RetryConfig{Enabled: false}
	})

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 with retry disabled", got)
	}
}

func TestScoresRetryStopsAtElapsedBudget(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.Retry = &RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Millisecond,
			MaxInterval:     5 * time.Millisecond,
			MaxElapsedTime:  30 * time.Millisecond,
		}
	})

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	// Flush returning proves the score was dropped once the budget elapsed
	// instead of retrying forever.
	flushScores(t, client)
	if got := calls.Load(); got < 1 {
		t.Fatalf("request count = %d, want at least one attempt", got)
	}
}

func TestScoresHonorRetryAfterAgainstBudget(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.Retry = &RetryConfig{
			Enabled:         true,
			InitialInterval: time.Millisecond,
			MaxInterval:     2 * time.Millisecond,
			MaxElapsedTime:  200 * time.Millisecond,
		}
	})

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	flushScores(t, client)
	// Ignoring Retry-After would fit dozens of 1-2ms backoff attempts into
	// the 200ms budget; honoring the 5s server delay exceeds the budget after
	// the first attempt, so the score is dropped without another request.
	if got := calls.Load(); got != 1 {
		t.Fatalf("request count = %d, want 1 when Retry-After exceeds the retry budget", got)
	}
}

func TestScoresDropWhenQueueIsFull(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	arrived := make(chan struct{}, 8)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseServer)
	client := newScoresTestClient(t, server.URL, nil)
	client.capacity = 1

	// The first score occupies the dispatcher, the second fills the queue,
	// and the third must be dropped without blocking or erroring.
	if err := client.Enqueue(context.Background(), []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Enqueue(a) error = %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first score never reached the server")
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Enqueue(b) error = %v", err)
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"c"}`)); err != nil {
		t.Fatalf("Enqueue(c) on a full queue error = %v, want nil drop", err)
	}
	releaseServer()
	flushScores(t, client)
	if got := calls.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2 (third score dropped)", got)
	}
}

func TestScoresBlockOnQueueFullWaitsForSpace(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	arrived := make(chan struct{}, 8)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseServer)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.BlockOnQueueFull = true
	})
	client.capacity = 1

	if err := client.Enqueue(context.Background(), []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Enqueue(a) error = %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first score never reached the server")
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Enqueue(b) error = %v", err)
	}
	blocked := make(chan error, 1)
	go func() {
		blocked <- client.Enqueue(context.Background(), []byte(`{"name":"c"}`))
	}()
	select {
	case err := <-blocked:
		t.Fatalf("Enqueue(c) returned %v while the queue was full, want it to block", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseServer()
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("blocked Enqueue(c) error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Enqueue(c) never completed after space opened")
	}
	flushScores(t, client)
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want 3", got)
	}
}

func TestScoresBlockedEnqueueHonorsCallerContext(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	arrived := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseServer)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.BlockOnQueueFull = true
	})
	client.capacity = 1

	if err := client.Enqueue(context.Background(), []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Enqueue(a) error = %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first score never reached the server")
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Enqueue(b) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan error, 1)
	go func() {
		blocked <- client.Enqueue(ctx, []byte(`{"name":"c"}`))
	}()
	cancel()
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("blocked Enqueue(c) error = nil after context cancel, want an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Enqueue(c) did not return after context cancel")
	}
	releaseServer()
}

func TestScoresShutdownDrainsAcceptedScores(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, nil)

	for range 3 {
		if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("request count = %d, want all 3 delivered before shutdown returned", got)
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"late"}`)); err == nil {
		t.Fatal("Enqueue() after Shutdown error = nil, want an error")
	}
}

func TestScoresShutdownHonorsContext(t *testing.T) {
	t.Parallel()
	arrived := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.Retry = &RetryConfig{
			Enabled:         true,
			InitialInterval: 50 * time.Millisecond,
			MaxInterval:     50 * time.Millisecond,
			MaxElapsedTime:  10 * time.Second,
		}
	})

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("score never reached the server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := client.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown() error = nil with an undeliverable score, want a context error")
	}
	// The dispatcher must have dropped the retrying score and exited, so a
	// later flush observes an empty queue.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush() after canceled shutdown error = %v", err)
	}
}

func TestScoresFlushHonorsContext(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := newScoresTestClient(t, server.URL, func(cfg *Config) {
		cfg.Retry = &RetryConfig{
			Enabled:         true,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     100 * time.Millisecond,
			MaxElapsedTime:  10 * time.Second,
		}
	})

	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := client.Flush(ctx); err == nil {
		t.Fatal("Flush() error = nil while a score was retrying, want a context error")
	}
	client.mu.Lock()
	leftover := len(client.waiters)
	client.mu.Unlock()
	if leftover != 0 {
		t.Fatalf("abandoned flush waiters = %d, want 0 after a canceled Flush", leftover)
	}
	// Abort the still-retrying delivery now so the deferred cleanup does not
	// spend its full shutdown deadline waiting for the 10-second retry budget.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shutdownCancel()
	_ = client.Shutdown(shutdownCtx)
}

func TestScoresFlushIsACallTimeBarrier(t *testing.T) {
	t.Parallel()
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	var firstOnce, secondOnce sync.Once
	releaseFirst := func() { firstOnce.Do(func() { close(firstRelease) }) }
	releaseSecond := func() { secondOnce.Do(func() { close(secondRelease) }) }
	releaseAll := func() { releaseFirst(); releaseSecond() }
	arrived := make(chan struct{}, 8)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := calls.Add(1)
		arrived <- struct{}{}
		if call == 1 {
			<-firstRelease
		} else {
			<-secondRelease
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseAll)
	client := newScoresTestClient(t, server.URL, nil)

	if err := client.Enqueue(context.Background(), []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Enqueue(a) error = %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first score never reached the server")
	}
	flushReturned := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		flushReturned <- client.Flush(ctx)
	}()
	// The Flush barrier must snapshot only the first score, so wait for its
	// waiter to register before accepting the second score.
	waiterDeadline := time.Now().Add(5 * time.Second)
	for {
		client.mu.Lock()
		registered := len(client.waiters) == 1
		client.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(waiterDeadline) {
			t.Fatal("Flush() never registered its barrier waiter")
		}
		time.Sleep(time.Millisecond)
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Enqueue(b) error = %v", err)
	}
	select {
	case err := <-flushReturned:
		t.Fatalf("Flush() returned %v before its pre-call score was delivered", err)
	case <-time.After(50 * time.Millisecond):
	}
	// Releasing only the first request must satisfy the Flush, even though
	// the score accepted after the Flush call is still in flight.
	releaseFirst()
	select {
	case err := <-flushReturned:
		if err != nil {
			t.Fatalf("Flush() error = %v after its pre-call score was delivered", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush() kept waiting on a score accepted after the call")
	}
	releaseSecond()
	flushScores(t, client)
	if got := calls.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}

func TestScoresFlushEmptyQueueIgnoresEndedContext(t *testing.T) {
	t.Parallel()
	client := newScoresTestClient(t, "https://cloud.langfuse.com", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush() with an empty queue and ended context error = %v, want nil", err)
	}
}

func TestScoresShutdownWithoutUseIsImmediate(t *testing.T) {
	t.Parallel()
	client := newScoresTestClient(t, "https://cloud.langfuse.com", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() of an unused client error = %v, want nil", err)
	}
	if err := client.Enqueue(context.Background(), []byte(`{"name":"n"}`)); err == nil {
		t.Fatal("Enqueue() after Shutdown error = nil, want an error")
	}
}

func TestScoresRejectInvalidRetryConfig(t *testing.T) {
	t.Parallel()
	_, err := NewScoresClient(Config{
		BaseURL:   "https://cloud.langfuse.com",
		PublicKey: "pk-lf-scores",
		SecretKey: "sk-lf-scores",
		Retry: &RetryConfig{
			Enabled:         true,
			InitialInterval: 10 * time.Millisecond,
			MaxInterval:     time.Millisecond,
			MaxElapsedTime:  time.Second,
		},
	})
	if err == nil {
		t.Fatal("NewScoresClient() error = nil with max interval below initial interval, want an error")
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		value string
		min   time.Duration
		max   time.Duration
	}{
		"empty":             {value: "", min: 0, max: 0},
		"seconds":           {value: "3", min: 3 * time.Second, max: 3 * time.Second},
		"padded":            {value: " 2 ", min: 2 * time.Second, max: 2 * time.Second},
		"zero":              {value: "0", min: 0, max: 0},
		"negative":          {value: "-2", min: 0, max: 0},
		"garbage":           {value: "soon", min: 0, max: 0},
		"past http day":     {value: "Mon, 02 Jan 2006 15:04:05 GMT", min: 0, max: 0},
		"clamped seconds":   {value: "10000000000", min: maxRetryAfter, max: maxRetryAfter},
		"overflow seconds":  {value: strings.Repeat("9", 30), min: maxRetryAfter, max: maxRetryAfter},
		"overflow negative": {value: "-" + strings.Repeat("9", 30), min: 0, max: 0},
	}
	for name, testCase := range cases {
		got := parseRetryAfter(testCase.value)
		if got < testCase.min || got > testCase.max {
			t.Errorf("parseRetryAfter(%q) = %v, want between %v and %v",
				name, got, testCase.min, testCase.max)
		}
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(future); got <= 80*time.Second || got > 90*time.Second {
		t.Errorf("parseRetryAfter(future date) = %v, want slightly under 90s", got)
	}
}
