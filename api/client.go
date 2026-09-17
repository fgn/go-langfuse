// Package api provides synchronous Langfuse project APIs independently of the
// tracing SDK. Clients own no workers, providers, queues, or shutdown lifecycle.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config configures an immutable, concurrency-safe API client. HTTPClient is
// shallow-copied; its Transport must be concurrency-safe and must not replay
// writes or inject unrelated credentials. Redirects and cookies are disabled.
// All operations have a finite total Timeout, including retries and body reads.
type Config struct {
	BaseURL          string
	PublicKey        string
	SecretKey        string
	HTTPClient       *http.Client
	Timeout          time.Duration
	MaxResponseBytes int64
	MaxErrorBytes    int64
	MaxAttempts      int
	MaxRetryDelay    time.Duration
}

// Client accesses one project. Service pointers must not be changed while the
// client is in use. Creating a client performs no network I/O.
type Client struct {
	Prompts          *PromptsService
	Datasets         *DatasetsService
	DatasetItems     *DatasetItemsService
	Observations     *ObservationsService
	Scores           *ScoresService
	Metrics          *MetricsService
	Experiments      *ExperimentsService
	ExperimentItems  *ExperimentItemsService
	ScoreConfigs     *ScoreConfigsService
	Models           *ModelsService
	LLMConnections   *LLMConnectionsService
	Comments         *CommentsService
	AnnotationQueues *AnnotationQueuesService
	Projects         *ProjectsService
	Health           *HealthService
	Media            *MediaService

	baseURL, publicKey, secretKey   string
	http                            *http.Client
	timeout, maxRetryDelay          time.Duration
	maxResponseBytes, maxErrorBytes int64
	maxAttempts                     int
}

type service struct{ client *Client }

// PromptsService manages prompt versions and deployment labels.
type PromptsService service

// DatasetsService manages dataset definitions.
type DatasetsService service

// DatasetItemsService manages current and historical dataset items.
type DatasetItemsService service

// ObservationsService reads the current observations v2 API.
type ObservationsService service

// ScoresService reads scores v3 and performs explicit synchronous score writes.
type ScoresService service

// MetricsService queries the metrics v2 API.
type MetricsService service

// ExperimentsService reads OTel-ingested experiments; it does not create runs.
type ExperimentsService service

// ExperimentItemsService reads OTel-ingested experiment items.
type ExperimentItemsService service

// ScoreConfigsService manages project score configurations.
type ScoreConfigsService service

// ModelsService manages project model definitions and pricing.
type ModelsService service

// LLMConnectionsService manages explicit provider connection settings.
type LLMConnectionsService service

// CommentsService creates and reads project comments.
type CommentsService service

// AnnotationQueuesService manages queues, items, and explicit assignments.
type AnnotationQueuesService service

// ProjectsService reads project information, not organization administration.
type ProjectsService service

// HealthService reads the server health/version endpoint.
type HealthService service

// MediaService manages explicit media records and uploads.
type MediaService service

// NewClient validates configuration without contacting a server. Empty BaseURL
// selects the EU Cloud endpoint. HTTP is allowed for explicitly selected local
// or self-hosted servers; callers must secure credential-bearing connections.
func NewClient(config Config) (*Client, error) {
	if config.BaseURL == "" {
		config.BaseURL = "https://cloud.langfuse.com"
	}
	u, err := url.Parse(config.BaseURL)
	if err != nil || u == nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("langfuse api: invalid base URL")
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return nil, errors.New("langfuse api: base URL contains a dot segment")
		}
	}
	if strings.TrimSpace(config.PublicKey) == "" || strings.TrimSpace(config.SecretKey) == "" || strings.Contains(config.PublicKey, ":") {
		return nil, errors.New("langfuse api: public and secret keys are required")
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = 8 << 20
	}
	if config.MaxErrorBytes == 0 {
		config.MaxErrorBytes = 8 << 10
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 3
	}
	if config.MaxRetryDelay == 0 {
		config.MaxRetryDelay = 10 * time.Second
	}
	if config.Timeout < 0 || config.Timeout > 10*time.Minute || config.MaxResponseBytes < 1 || config.MaxResponseBytes > 64<<20 || config.MaxErrorBytes < 1 || config.MaxErrorBytes > 1<<20 || config.MaxAttempts < 1 || config.MaxAttempts > 5 || config.MaxRetryDelay < 0 || config.MaxRetryDelay > time.Minute {
		return nil, errors.New("langfuse api: invalid timeout, response limit, or retry bound")
	}
	hc := http.Client{}
	if config.HTTPClient != nil {
		hc = *config.HTTPClient
	}
	hc.Jar = nil
	hc.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	c := &Client{
		baseURL: strings.TrimRight(u.String(), "/"), publicKey: config.PublicKey, secretKey: config.SecretKey,
		http: &hc, timeout: config.Timeout, maxRetryDelay: config.MaxRetryDelay,
		maxResponseBytes: config.MaxResponseBytes, maxErrorBytes: config.MaxErrorBytes, maxAttempts: config.MaxAttempts,
	}
	s := &service{client: c}
	c.Prompts, c.Datasets, c.DatasetItems = (*PromptsService)(s), (*DatasetsService)(s), (*DatasetItemsService)(s)
	c.Observations, c.Scores, c.Metrics = (*ObservationsService)(s), (*ScoresService)(s), (*MetricsService)(s)
	c.Experiments, c.ExperimentItems = (*ExperimentsService)(s), (*ExperimentItemsService)(s)
	c.ScoreConfigs, c.Models, c.LLMConnections = (*ScoreConfigsService)(s), (*ModelsService)(s), (*LLMConnectionsService)(s)
	c.Comments, c.AnnotationQueues = (*CommentsService)(s), (*AnnotationQueuesService)(s)
	c.Projects, c.Health, c.Media = (*ProjectsService)(s), (*HealthService)(s), (*MediaService)(s)
	return c, nil
}

var (
	// ErrUninitialized reports use of a nil or zero-value API client/service.
	ErrUninitialized = errors.New("langfuse api: uninitialized client")
	// ErrResponseTooLarge reports a success body exceeding the configured limit.
	ErrResponseTooLarge = errors.New("langfuse api: response exceeds size limit")
	// ErrInvalidResponse reports empty, null, malformed, or inconsistent responses.
	ErrInvalidResponse = errors.New("langfuse api: invalid response")
)

// RequestError deliberately excludes URLs, request/response bodies, headers,
// credentials, and underlying error text from every fmt representation.
// Unwrap preserves errors.Is/errors.As for explicit programmatic inspection.
type RequestError struct {
	Operation string
	Kind      string
	cause     error
}

func (e *RequestError) Error() string                     { return "langfuse api: " + e.Operation + ": " + e.Kind }
func (e *RequestError) Unwrap() error                     { return e.cause }
func (e *RequestError) Format(state fmt.State, verb rune) { formatError(state, verb, e.Error()) }

// ResponseError is a non-success HTTP response. BodyTruncated describes the
// bounded discard; raw server error text is intentionally not retained.
type ResponseError struct {
	Operation     string
	StatusCode    int
	BodyTruncated bool
	RetryAfter    time.Duration
}

func (e *ResponseError) Error() string {
	return "langfuse api: " + e.Operation + ": HTTP " + strconv.Itoa(e.StatusCode)
}
func (e *ResponseError) Format(state fmt.State, verb rune) { formatError(state, verb, e.Error()) }

func formatError(state fmt.State, verb rune, message string) {
	if verb == 'q' {
		message = strconv.Quote(message)
	}
	_, _ = io.WriteString(state, message)
}

func clientFor(s *service) *Client {
	if s == nil {
		return nil
	}
	return s.client
}

func (c *Client) do(ctx context.Context, operation, method, path string, query url.Values, body, result any) error {
	if c == nil || c.http == nil {
		return ErrUninitialized
	}
	if ctx == nil {
		return errors.New("langfuse api: nil context")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return requestError(operation, "canceled", err)
	}
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return requestError(operation, "invalid JSON request", nil)
		}
		if len(payload) > 8<<20 {
			return requestError(operation, "request exceeds size limit", nil)
		}
	}
	endpoint := c.baseURL + "/api/public" + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	attempts := 1
	if method == http.MethodGet {
		attempts = c.maxAttempts
	}
	for attempt := 0; attempt < attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
		if err != nil {
			return requestError(operation, "invalid request", nil)
		}
		// Prevent transport replay of a non-empty write body. A caller-supplied
		// retrying RoundTripper is outside this client's control and unsupported.
		req.GetBody = nil
		req.SetBasicAuth(c.publicKey, c.secretKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "go-langfuse-api")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		response, requestErr := c.http.Do(req)
		if requestErr != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			if err := ctx.Err(); err != nil {
				return requestError(operation, "canceled", err)
			}
			if attempt+1 < attempts {
				if err := sleepRetry(ctx, c.retryDelay(attempt, "")); err != nil {
					return requestError(operation, "canceled", err)
				}
				continue
			}
			return requestError(operation, "transport failed", requestErr)
		}
		if response == nil || response.Body == nil {
			return requestError(operation, "invalid response", ErrInvalidResponse)
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, c.maxErrorBytes+1))
			_ = response.Body.Close()
			if err := ctx.Err(); err != nil {
				return requestError(operation, "canceled", err)
			}
			delay := c.retryDelay(attempt, response.Header.Get("Retry-After"))
			failure := &ResponseError{Operation: operation, StatusCode: response.StatusCode, BodyTruncated: int64(len(data)) > c.maxErrorBytes || readErr != nil, RetryAfter: delay}
			if attempt+1 < attempts && retryableStatus(response.StatusCode) {
				if err := sleepRetry(ctx, delay); err != nil {
					return requestError(operation, "canceled", err)
				}
				continue
			}
			return failure
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
		_ = response.Body.Close()
		if err := ctx.Err(); err != nil {
			return requestError(operation, "canceled", err)
		}
		if readErr != nil {
			return requestError(operation, "response read failed", readErr)
		}
		if int64(len(data)) > c.maxResponseBytes {
			return requestError(operation, "response exceeds size limit", ErrResponseTooLarge)
		}
		if result == nil {
			return nil
		}
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
			return requestError(operation, "invalid response", ErrInvalidResponse)
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(result); err != nil {
			return requestError(operation, "invalid response", ErrInvalidResponse)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return requestError(operation, "invalid response", ErrInvalidResponse)
		}
		return nil
	}
	return requestError(operation, "request attempts exhausted", nil)
}

func requestError(operation, kind string, cause error) error {
	return &RequestError{Operation: operation, Kind: kind, cause: cause}
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (c *Client) retryDelay(attempt int, header string) time.Duration {
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil && seconds >= 0 {
		if seconds > int64(c.maxRetryDelay/time.Second) {
			return c.maxRetryDelay
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(header); err == nil {
		return min(max(time.Until(date), 0), c.maxRetryDelay)
	}
	ceiling := min(100*time.Millisecond<<attempt, c.maxRetryDelay)
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

func sleepRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func escapedSegment(value string) (string, error) {
	if value == "" || len(value) > 4096 || strings.ContainsAny(value, "\x00\r\n") {
		return "", errors.New("langfuse api: invalid resource name or ID")
	}
	if value == "." || value == ".." {
		return strings.Repeat("%2E", len(value)), nil
	}
	return url.PathEscape(value), nil
}
