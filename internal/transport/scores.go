package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fgn/go-langfuse/internal/diagnostic"
)

const (
	// maxScoreResponseBytes bounds how much of a 207 ingestion response body
	// is read. The body is inspected for per-item errors, so the bound is
	// generous enough that an error entry echoing a full 128 KiB score
	// payload still parses instead of being truncated into unparsable JSON.
	maxScoreResponseBytes = 256 << 10
	// maxScoreDrainBytes bounds how much of any other response body is read
	// so the underlying connection can be reused.
	maxScoreDrainBytes = 4 << 10
	// defaultScoreQueueSize bounds how many serialized scores wait for the
	// dispatcher. Scores are low-volume evaluation and feedback events, so the
	// queue is intentionally much smaller than the span queue.
	defaultScoreQueueSize = 256
)

// ScoresClient delivers score events to the Langfuse JSON ingestion endpoint
// from a bounded queue serviced by one background dispatcher, retrying
// transient failures with the same policy defaults as the OTLP exporter.
// Delivery failures are reported as payload-free diagnostics, never returned
// to the producer.
type ScoresClient struct {
	endpoint    string
	publicKey   string
	secretKey   string
	client      *http.Client
	retry       RetryConfig
	blockOnFull bool
	capacity    int

	// wake and space are 1-buffered edge signals: wake tells the dispatcher
	// the queue or the stopped flag changed, space tells one blocked producer
	// a slot may have opened. Lost signals are safe because both waiters
	// re-check state in a loop.
	wake  chan struct{}
	space chan struct{}
	done  chan struct{}
	// diagSlots bounds how many diagnostics may be running application-
	// pluggable OpenTelemetry error handlers concurrently.
	diagSlots chan struct{}

	mu  sync.Mutex
	buf []scoreEvent
	// accepted counts scores appended to buf over the client lifetime and
	// completed counts scores delivered or dropped, so completed trails
	// accepted by the queued-plus-in-flight amount. Each Flush call waits on
	// its own waiter targeting the accepted count it observed, giving Flush
	// call-time barrier semantics that concurrent producers cannot starve.
	accepted  uint64
	completed uint64
	waiters   []scoreFlushWaiter
	started   bool
	stopped   bool
	// The dispatcher owns a lifecycle context created when it starts. It
	// governs sends and retry waits instead of any producer context, because
	// a canceled request context must not lose an accepted score. Only its
	// cancellation side is stored: cancel aborts delivery during shutdown and
	// stop lets blocked producers observe that abort.
	cancel context.CancelFunc
	stop   <-chan struct{}
}

// scoreFlushWaiter is one Flush call waiting for every score accepted before
// it to complete; done is closed when completed reaches target.
type scoreFlushWaiter struct {
	target uint64
	done   chan struct{}
}

// scoreEvent is one queued delivery: the serialized single-event ingestion
// request and the envelope event ID it must be accounted under in the
// endpoint's 207 result.
type scoreEvent struct {
	payload []byte
	eventID string
}

// maxConcurrentScoreDiagnostics bounds concurrently running error handlers;
// beyond it further diagnostics are dropped rather than accumulating
// goroutines behind a slow handler.
const maxConcurrentScoreDiagnostics = 4

// NewScoresClient builds a scores client from an already validated transport
// configuration. It performs no network I/O and starts no goroutine; the
// dispatcher starts on the first enqueued score.
func NewScoresClient(cfg Config) (*ScoresClient, error) {
	endpoint, err := NormalizeIngestionEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	retry := defaultRetry
	if cfg.Retry != nil {
		retry = *cfg.Retry
	}
	if err := validateRetry(retry); err != nil {
		return nil, err
	}
	return &ScoresClient{
		endpoint:  endpoint,
		publicKey: cfg.PublicKey,
		secretKey: cfg.SecretKey,
		client: &http.Client{
			Timeout: timeout,
			// Never follow redirects: a Location target is server-controlled
			// and following it would re-send the credentials and the score to
			// another URL and surface that URL in error text. A 3xx response
			// is returned as-is and dropped as a permanent failure.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		retry:       retry,
		blockOnFull: cfg.BlockOnQueueFull,
		capacity:    defaultScoreQueueSize,
		wake:        make(chan struct{}, 1),
		space:       make(chan struct{}, 1),
		done:        make(chan struct{}),
		diagSlots:   make(chan struct{}, maxConcurrentScoreDiagnostics),
	}, nil
}

// Enqueue accepts one JSON-encoded score ingestion request for asynchronous
// delivery; eventID is the envelope event ID the ingestion result must
// account for. It returns an error only when the client is shut down or, in
// blocking mode, when ctx ends while waiting for queue space. In non-blocking
// mode a full queue drops the score with a payload-free diagnostic and
// returns nil, matching the span pipeline's backpressure default.
func (s *ScoresClient) Enqueue(ctx context.Context, payload []byte, eventID string) error {
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return errors.New("langfuse transport: score rejected after client shutdown")
		}
		if len(s.buf) < s.capacity {
			if !s.started {
				s.started = true
				lifecycleCtx, cancel := context.WithCancel(context.Background())
				s.cancel = cancel
				s.stop = lifecycleCtx.Done()
				go s.dispatch(lifecycleCtx)
			}
			s.buf = append(s.buf, scoreEvent{payload: payload, eventID: eventID})
			s.accepted++
			s.mu.Unlock()
			signal(s.wake)
			return nil
		}
		stop := s.stop
		s.mu.Unlock()

		if !s.blockOnFull {
			s.reportAsync("score dropped because the score queue is full")
			return nil
		}
		select {
		case <-s.space:
		case <-ctx.Done():
			return fmt.Errorf("langfuse transport: score enqueue canceled: %w", ctx.Err())
		case <-stop:
			return errors.New("langfuse transport: score rejected after client shutdown")
		}
	}
}

// Flush waits until every score accepted before the call has been delivered
// or dropped. Scores accepted after the call do not extend the wait, and an
// empty queue returns nil even when ctx has already ended.
func (s *ScoresClient) Flush(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.completed >= s.accepted {
		s.mu.Unlock()
		return nil
	}
	waiter := scoreFlushWaiter{target: s.accepted, done: make(chan struct{})}
	s.waiters = append(s.waiters, waiter)
	s.mu.Unlock()
	select {
	case <-waiter.done:
		return nil
	case <-ctx.Done():
		s.abandonWaiter(waiter.done)
		return fmt.Errorf("langfuse transport: score flush: %w", ctx.Err())
	}
}

// abandonWaiter removes a canceled Flush call's waiter so repeated
// short-deadline flushes during a long retry cannot accumulate entries.
func (s *ScoresClient) abandonWaiter(done chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.waiters {
		if s.waiters[i].done == done {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			clear(s.waiters[len(s.waiters):cap(s.waiters)])
			return
		}
	}
}

// Shutdown rejects new scores, drains the queue bounded by ctx, and stops the
// dispatcher. When ctx ends first, in-flight and queued scores are dropped
// with diagnostics and the context error is returned.
func (s *ScoresClient) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	alreadyStopped := s.stopped
	s.stopped = true
	started := s.started
	drained := s.completed >= s.accepted
	s.mu.Unlock()
	if !started {
		return nil
	}
	signal(s.wake)
	if alreadyStopped {
		// Another Shutdown owns the drain; do not cancel its work.
		return nil
	}
	if drained {
		// The dispatcher exits without further network work, so this wait is
		// short even when ctx has already ended.
		<-s.done
		s.cancel()
		return nil
	}
	select {
	case <-s.done:
		s.cancel()
		return nil
	case <-ctx.Done():
		s.cancel()
		<-s.done
		return fmt.Errorf("langfuse transport: score shutdown: %w", ctx.Err())
	}
}

func (s *ScoresClient) dispatch(lifecycleCtx context.Context) {
	defer close(s.done)
	for {
		s.mu.Lock()
		if len(s.buf) == 0 {
			stopped := s.stopped
			s.mu.Unlock()
			if stopped {
				return
			}
			<-s.wake
			continue
		}
		event := s.buf[0]
		s.buf[0] = scoreEvent{}
		s.buf = s.buf[1:]
		s.mu.Unlock()
		signal(s.space)
		s.deliver(lifecycleCtx, event)
		s.finishOne()
	}
}

func (s *ScoresClient) finishOne() {
	s.mu.Lock()
	s.completed++
	kept := s.waiters[:0]
	for _, waiter := range s.waiters {
		if waiter.target <= s.completed {
			close(waiter.done)
		} else {
			kept = append(kept, waiter)
		}
	}
	// Clear compacted-away entries so satisfied waiters' channels do not stay
	// reachable through the backing array for the client's lifetime.
	clear(s.waiters[len(kept):])
	s.waiters = kept
	s.mu.Unlock()
}

// deliver posts one score, retrying transient failures with exponential
// backoff and jitter until the retry budget or the client lifecycle ends.
// Every terminal failure emits one payload-free diagnostic.
func (s *ScoresClient) deliver(ctx context.Context, event scoreEvent) {
	deadline := time.Now().Add(s.retry.MaxElapsedTime)
	interval := s.retry.InitialInterval
	for {
		if ctx.Err() != nil {
			s.reportAsync("score dropped during client shutdown")
			return
		}
		retryable, retryAfter, failure := s.post(ctx, event)
		if failure == "" {
			return
		}
		if ctx.Err() != nil {
			s.reportAsync("score dropped during client shutdown")
			return
		}
		if !s.retry.Enabled || !retryable {
			s.reportAsync("score dropped: " + failure)
			return
		}
		delay := max(interval/2+rand.N(interval), retryAfter)
		if time.Now().Add(delay).After(deadline) {
			s.reportAsync("score dropped after retries: " + failure)
			return
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			s.reportAsync("score dropped during client shutdown")
			return
		}
		interval = min(interval*2, s.retry.MaxInterval)
	}
}

// post sends one attempt. An empty failure means the score was accepted.
// Credentials travel exclusively in the Authorization header, and the failure
// summary is built only from static text and numeric status codes: reason
// phrases, response bodies, and transport error strings can carry
// server-controlled content (an echoed score, redirect targets, certificate
// fields) that must not reach the payload-free diagnostics these summaries
// flow to.
func (s *ScoresClient) post(ctx context.Context, event scoreEvent) (retryable bool, retryAfter time.Duration, failure string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(event.payload))
	if err != nil {
		return false, 0, "the score request could not be built"
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth(s.publicKey, s.secretKey)
	response, err := s.client.Do(request)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return true, 0, "the score request timed out"
		}
		return true, 0, "the score request failed before a response"
	}
	defer func() { _ = response.Body.Close() }()
	retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
	if response.StatusCode == http.StatusMultiStatus {
		// A 207 is the ingestion endpoint's documented response and its body
		// is part of the delivery contract: an unreadable, truncated, or
		// malformed result — or one that does not account for the submitted
		// envelope event ID in successes or errors — leaves the outcome
		// unknown, so it is retried.
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxScoreResponseBytes+1))
		result, parsed := parseIngestionResult(body)
		if readErr != nil || len(body) > maxScoreResponseBytes || !parsed {
			return true, retryAfter, "the score ingestion response could not be read"
		}
		for _, item := range result.Errors {
			if item.ID != event.eventID {
				continue
			}
			failure = "the score ingestion endpoint reported item status " + strconv.Itoa(item.Status)
			// An absent or zero item status leaves the outcome unknown;
			// retry it like a transient failure rather than dropping.
			if item.Status == 0 || retryableStatus(item.Status) {
				return true, retryAfter, failure
			}
			return false, 0, failure
		}
		for _, item := range result.Successes {
			if item.ID == event.eventID {
				return false, 0, ""
			}
		}
		return true, retryAfter, "the score ingestion response did not account for the score"
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxScoreDrainBytes))
	if response.StatusCode < 300 {
		// Other 2xx statuses come from intermediaries or foreign deployments
		// whose bodies carry no contract; the status alone means success.
		return false, 0, ""
	}
	failure = "the score ingestion endpoint returned status " + strconv.Itoa(response.StatusCode)
	if retryableStatus(response.StatusCode) {
		return true, retryAfter, failure
	}
	return false, 0, failure
}

// retryableStatus reports whether an HTTP or ingestion-item status marks a
// transient failure worth retrying.
func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooManyRequests ||
		(status >= 500 && status <= 599)
}

// ingestionResult is the documented shape of an ingestion 207 response. For a
// single-event request the submitted envelope event ID must appear in errors
// (the score was rejected) or successes (it was stored); a response that does
// not account for that ID leaves the outcome unknown.
type ingestionResult struct {
	Successes []ingestionItem `json:"successes"`
	Errors    []ingestionItem `json:"errors"`
}

type ingestionItem struct {
	ID     string `json:"id"`
	Status int    `json:"status"`
}

// parseIngestionResult reports whether body is the documented ingestion
// result object.
func parseIngestionResult(body []byte) (ingestionResult, bool) {
	var result ingestionResult
	if err := json.Unmarshal(body, &result); err != nil {
		return ingestionResult{}, false
	}
	return result, true
}

// maxRetryAfter caps a server-requested delay. It exists to keep hostile or
// broken header values from overflowing duration arithmetic; any capped value
// still far exceeds every retry budget and therefore drops the score.
const maxRetryAfter = 24 * time.Hour

// parseRetryAfter reads a Retry-After header as delay seconds or an HTTP
// date. Unparsable or non-positive values mean no server-requested delay.
func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds > int(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return time.Duration(seconds) * time.Second
	} else if errors.Is(err, strconv.ErrRange) && value[0] != '-' {
		// A positive value too large for int is still a valid delay request;
		// without this it would fall through to zero and permit retries the
		// clamped delay is meant to suppress.
		return maxRetryAfter
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return min(delay, maxRetryAfter)
		}
	}
	return 0
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// reportAsync emits a payload-free diagnostic without running the
// application-pluggable OpenTelemetry error handler on a pipeline goroutine.
// A handler may re-enter Client.Flush, Client.Shutdown, or RecordScore;
// reporting inline from the dispatcher would deadlock a handler that waits on
// the very score pipeline it was invoked from. Concurrent reports are
// bounded: when a slow or blocked handler saturates the bound, further
// diagnostics are dropped instead of accumulating one goroutine each.
func (s *ScoresClient) reportAsync(message string) {
	select {
	case s.diagSlots <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-s.diagSlots }()
		diagnostic.Report(message)
	}()
}
