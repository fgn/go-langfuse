package langfuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/diagnostic"
	lfprocessor "github.com/fgn/go-langfuse/internal/processor"
	"github.com/fgn/go-langfuse/internal/transport"
)

// Go's regexp package has no lookahead, so the prefix rule is checked
// separately from this character allowlist.
var environmentCharacters = regexp.MustCompile(`^[a-z0-9_-]+$`)

const (
	// These values match the standard OpenTelemetry BatchSpanProcessor
	// defaults but are always passed explicitly so hostile or accidentally
	// inherited OTEL_BSP_* variables cannot make New panic, hot-loop, silently
	// drop every span, or reserve an unbounded queue. The queue stores
	// references to caller-owned third-party spans in borrowed mode, so that
	// payload remains a documented trust boundary even though its item count
	// is bounded; the transport's request-size limit splits or rejects
	// oversized requests at export time.
	defaultMaxQueueSize = 2048
	maximumQueueSize    = 1 << 16
	maxExportBatchSize  = 512
	exportBatchTimeout  = 5 * time.Second
	exportTimeout       = 30 * time.Second
)

// ErrTracerProviderInUse reports that another Langfuse client already owns
// the Langfuse processor attachment for a borrowed tracer provider.
var ErrTracerProviderInUse = errors.New("langfuse: tracer provider already has a Langfuse client")

// ErrShutdownInProgress reports that another call is currently shutting down
// the client. It prevents re-entrant shutdown callbacks from waiting on
// themselves. Call Shutdown again after the owning call completes to receive
// its stored result.
var ErrShutdownInProgress = errors.New("langfuse: shutdown is in progress")

// Client owns all Langfuse exporter, processor, and lifecycle state. Its zero
// value is a safe no-op.
type Client struct {
	tracer      oteltrace.Tracer
	provider    *sdktrace.TracerProvider
	processor   *lfprocessor.Processor
	scores      *transport.ScoresClient
	prompts     *promptCache
	environment string
	owned       bool
	reserved    bool
	disabled    bool

	disableContentCapture bool
	mask                  func(string, any) any

	stopped                 atomic.Bool
	stoppedWarning          atomic.Bool
	shutdownStarted         atomic.Bool
	shutdownDone            chan struct{}
	shutdownResultMu        sync.RWMutex
	shutdownResult          error
	borrowedRateWarning     atomic.Bool
	scoreSuppressionWarning atomic.Bool
}

var borrowedProviderRegistry = struct {
	sync.Mutex
	owners map[*sdktrace.TracerProvider]*Client
}{owners: make(map[*sdktrace.TracerProvider]*Client)}

// ConfigFromEnv returns configuration from the eight documented LANGFUSE_*
// variables. It intentionally does not read the deprecated LANGFUSE_HOST.
func ConfigFromEnv() Config {
	cfg := Config{
		BaseURL:     os.Getenv("LANGFUSE_BASE_URL"),
		PublicKey:   os.Getenv("LANGFUSE_PUBLIC_KEY"),
		SecretKey:   os.Getenv("LANGFUSE_SECRET_KEY"),
		Environment: os.Getenv("LANGFUSE_TRACING_ENVIRONMENT"),
		Release:     os.Getenv("LANGFUSE_RELEASE"),
	}
	if raw, ok := os.LookupEnv("LANGFUSE_TRACING_ENABLED"); ok {
		enabled, err := parseEnvironmentBool("LANGFUSE_TRACING_ENABLED", raw)
		if err != nil {
			cfg.envErr = errors.Join(cfg.envErr, err)
		} else {
			cfg.Disabled = !enabled
		}
	}
	if raw, ok := os.LookupEnv("LANGFUSE_CONTENT_CAPTURE_ENABLED"); ok {
		enabled, err := parseEnvironmentBool("LANGFUSE_CONTENT_CAPTURE_ENABLED", raw)
		if err != nil {
			cfg.envErr = errors.Join(cfg.envErr, err)
		} else {
			cfg.DisableContentCapture = !enabled
		}
	}
	if raw, ok := os.LookupEnv("LANGFUSE_SAMPLE_RATE"); ok {
		fraction, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil || !validSampleFraction(fraction) {
			cfg.envErr = errors.Join(cfg.envErr,
				errors.New("langfuse: LANGFUSE_SAMPLE_RATE must be a number within [0, 1]"))
		} else {
			cfg.SampleRate = &fraction
		}
	}
	return cfg
}

func parseEnvironmentBool(name, raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("langfuse: %s must be true or false", name)
	}
}

// New constructs a client and validates configuration without performing
// network I/O. A disabled configuration bypasses all other validation.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Disabled {
		return &Client{disabled: true}, nil
	}
	if ctx == nil {
		return nil, errors.New("langfuse: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("langfuse: context is not usable: %w", err)
	}
	if cfg.envErr != nil {
		return nil, cfg.envErr
	}
	environment := cfg.Environment
	if environment == "" {
		environment = "default"
	}
	if err := validateEnvironment(environment); err != nil {
		return nil, err
	}
	if err := validateConfigString("release", cfg.Release, true); err != nil {
		return nil, err
	}
	if err := validateConfigString("service name", cfg.ServiceName, true); err != nil {
		return nil, err
	}
	if cfg.MaxQueueSize < 0 || cfg.MaxQueueSize > maximumQueueSize {
		return nil, fmt.Errorf("langfuse: max queue size must be within [0, %d]", maximumQueueSize)
	}
	if cfg.SampleRate != nil && !validSampleFraction(*cfg.SampleRate) {
		return nil, errors.New("langfuse: sample rate must be finite and within [0, 1]")
	}
	var mask func(string, any) any
	if cfg.Mask != nil {
		configuredMask := cfg.Mask
		mask = func(field string, value any) any {
			return configuredMask(MaskField(field), value)
		}
	}

	transportConfig := transport.Config{
		BaseURL:          cfg.BaseURL,
		PublicKey:        cfg.PublicKey,
		SecretKey:        cfg.SecretKey,
		SDKVersion:       sdkVersion,
		BlockOnQueueFull: cfg.BlockOnQueueFull,
	}
	if err := transport.ValidateConfig(transportConfig); err != nil {
		return nil, err
	}

	client := &Client{
		disableContentCapture: cfg.DisableContentCapture,
		mask:                  mask,
		environment:           environment,
		shutdownDone:          make(chan struct{}),
	}
	scores, err := transport.NewScoresClient(transportConfig)
	if err != nil {
		return nil, err
	}
	client.scores = scores
	promptFetcher, err := transport.NewPromptsClient(transportConfig)
	if err != nil {
		return nil, err
	}
	client.prompts = newPromptCache(promptFetcher)
	if cfg.TracerProvider != nil {
		if !reserveBorrowedProvider(cfg.TracerProvider, client) {
			return nil, ErrTracerProviderInUse
		}
		client.reserved = true
		client.provider = cfg.TracerProvider
		if cfg.DisableContentCapture {
			diagnostic.Report("content capture is disabled only for SDK-supplied input and output; third-party OpenTelemetry content is unchanged")
		}
		if cfg.SampleRate != nil {
			diagnostic.Report("sample rate is ignored on a borrowed tracer provider; the application's sampler remains authoritative")
		}
	}

	exporter, err := transport.NewExporter(ctx, transportConfig)
	if err != nil {
		client.releaseReservation()
		return nil, err
	}
	queueSize := cfg.MaxQueueSize
	if queueSize == 0 {
		queueSize = defaultMaxQueueSize
	}
	batchOptions := []sdktrace.BatchSpanProcessorOption{
		sdktrace.WithMaxQueueSize(queueSize),
		sdktrace.WithMaxExportBatchSize(min(maxExportBatchSize, queueSize)),
		sdktrace.WithBatchTimeout(exportBatchTimeout),
		sdktrace.WithExportTimeout(exportTimeout),
	}
	if cfg.BlockOnQueueFull {
		batchOptions = append(batchOptions, sdktrace.WithBlocking())
	}
	batch := sdktrace.NewBatchSpanProcessor(exporter, batchOptions...)
	processor, err := lfprocessor.New(lfprocessor.Config{
		Next:              batch,
		PublicKey:         cfg.PublicKey,
		Environment:       environment,
		Release:           cfg.Release,
		ContextAttributes: client.propagatedAttributes,
		HasTraceClaim:     client.hasTraceClaim,
		Admit:             client.admitObservation,
		ShouldExportSpan:  cfg.ShouldExportSpan,
	})
	if err != nil {
		_ = batch.Shutdown(context.Background())
		client.releaseReservation()
		return nil, fmt.Errorf("langfuse: create span processor: %w", err)
	}
	client.processor = processor

	if cfg.TracerProvider == nil {
		res, err := ownedResource(cfg.ServiceName)
		if err != nil {
			_ = processor.Shutdown(context.Background())
			return nil, err
		}
		defaultFraction := 1.0
		if cfg.SampleRate != nil {
			defaultFraction = *cfg.SampleRate
		}
		client.provider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sampler{client: client, defaultFraction: defaultFraction}),
			sdktrace.WithResource(res),
			sdktrace.WithRawSpanLimits(ownedSpanLimits()),
			sdktrace.WithSpanProcessor(processor),
		)
		client.owned = true
	} else {
		client.provider.RegisterSpanProcessor(processor)
	}

	client.tracer = client.provider.Tracer(
		lfattr.TracerName,
		oteltrace.WithInstrumentationVersion(sdkVersion),
		oteltrace.WithInstrumentationAttributes(attribute.String("public_key", cfg.PublicKey)),
	)
	return client, nil
}

func ownedSpanLimits() sdktrace.SpanLimits {
	// Do not inherit OTEL_SPAN_* settings intended for an application's
	// unrelated provider. Counts cover the SDK's bounded schema; value length
	// remains unlimited here because truncating JSON produces invalid payloads,
	// while the SDK enforces its own byte budgets before attributes are set.
	return sdktrace.SpanLimits{
		AttributeValueLengthLimit:   -1,
		AttributeCountLimit:         128,
		EventCountLimit:             maxErrorEvents,
		LinkCountLimit:              128,
		AttributePerEventCountLimit: 16,
		AttributePerLinkCountLimit:  128,
	}
}

func validateEnvironment(value string) error {
	if len(value) > 40 {
		return errors.New("langfuse: environment must be at most 40 characters")
	}
	if strings.HasPrefix(value, "langfuse") || !environmentCharacters.MatchString(value) {
		return errors.New("langfuse: environment must use lowercase letters, numbers, underscores, or hyphens and must not start with langfuse")
	}
	return nil
}

func validateConfigString(field, value string, emptyAllowed bool) error {
	if value == "" && emptyAllowed {
		return nil
	}
	if value == "" || !utf8.ValidString(value) || len(value) > lfattr.MaxDirectStringBytes {
		return fmt.Errorf("langfuse: %s is invalid or exceeds the internal size limit", field)
	}
	return nil
}

func ownedResource(serviceName string) (*resource.Resource, error) {
	if serviceName == "" {
		return resource.Default(), nil
	}
	// Keep this resource schemaless before merging with resource.Default.
	// resource.Default follows the OTel SDK's semconv version, which can be one
	// release ahead of this module's imported helper and would otherwise cause
	// a schema URL conflict during minor OTel upgrades.
	service := resource.NewSchemaless(semconv.ServiceNameKey.String(serviceName))
	result, err := resource.Merge(resource.Default(), service)
	if err != nil {
		return nil, errors.New("langfuse: create OpenTelemetry resource")
	}
	return result, nil
}

func reserveBorrowedProvider(provider *sdktrace.TracerProvider, owner *Client) bool {
	borrowedProviderRegistry.Lock()
	defer borrowedProviderRegistry.Unlock()
	if _, exists := borrowedProviderRegistry.owners[provider]; exists {
		return false
	}
	borrowedProviderRegistry.owners[provider] = owner
	return true
}

func (c *Client) releaseReservation() {
	if c == nil || !c.reserved || c.provider == nil {
		return
	}
	borrowedProviderRegistry.Lock()
	if borrowedProviderRegistry.owners[c.provider] == c {
		delete(borrowedProviderRegistry.owners, c.provider)
	}
	borrowedProviderRegistry.Unlock()
	c.reserved = false
}

func (c *Client) isDisabled() bool {
	return c == nil || c.disabled || c.tracer == nil
}

func (c *Client) reportStoppedOnce() {
	if c != nil && c.stoppedWarning.CompareAndSwap(false, true) {
		diagnostic.Report("observation ignored after client shutdown")
	}
}

// admitObservation accepts a pending admission token. The Langfuse processor
// calls it from OnStart for SDK-scope spans only. expected records whether the
// span passed start-time export classification, so a filtered observation
// remains usable without suppressing a later exported child's application-
// root marker. Admission is decided by the component being torn down: once
// Shutdown publishes stopped or the processor stops, no start can be admitted,
// while an ordinary Flush leaves admission open. See StartObservation for the
// token's evaluation.
func (c *Client) admitObservation(ctx context.Context, expected bool) {
	if c == nil || ctx == nil || c.stopped.Load() {
		return
	}
	if token, ok := ctx.Value(admissionTokenContextKey{client: c}).(*observationAdmission); ok && token != nil {
		token.expected.Store(expected)
		token.admitted.Store(true)
	}
}

// Flush exports all ended observations and delivers every score accepted
// before the call; scores recorded after Flush begins do not extend the wait.
// A score mid-retry holds Flush until it is delivered or dropped, bounded by
// ctx.
func (c *Client) Flush(ctx context.Context) error {
	if c == nil || c.isDisabled() || c.stopped.Load() {
		return nil
	}
	if ctx == nil {
		return errors.New("langfuse: flush context is nil")
	}
	// Start the score barrier immediately so it snapshots the scores accepted
	// at the moment Flush was called; waiting for span flushing first would
	// let scores recorded in the meantime extend the wait.
	scoresDone := make(chan error, 1)
	go func() { scoresDone <- c.scores.Flush(ctx) }()
	var spanErr error
	if c.owned {
		spanErr = c.provider.ForceFlush(ctx)
	} else {
		spanErr = c.processor.ForceFlush(ctx)
	}
	return errors.Join(spanErr, <-scoresDone)
}

// Shutdown permanently stops this Client after draining queued observations
// and scores, bounded by ctx. In borrowed mode it shuts down the Langfuse
// processor using ctx before unregistering it; the provider and all
// unrelated processors remain owned by the application. A concurrent or
// re-entrant call returns [ErrShutdownInProgress]. After shutdown completes,
// later calls return the first call's stored result.
func (c *Client) Shutdown(ctx context.Context) error {
	if c == nil || c.isDisabled() {
		return nil
	}
	if c.shutdownStarted.Load() {
		return c.currentShutdownResult()
	}
	if ctx == nil {
		return errors.New("langfuse: shutdown context is nil")
	}
	// Publish the stopped state before invoking OpenTelemetry. Its processor,
	// exporter, and diagnostic callbacks are application-extensible and may
	// re-enter Shutdown. Re-entrant and concurrent calls return immediately;
	// the call that wins this transition owns teardown and its result.
	if !c.shutdownStarted.CompareAndSwap(false, true) {
		return c.currentShutdownResult()
	}
	c.stopped.Store(true)
	// Stop prompt admission and cancel prompt I/O before the potentially
	// blocking OpenTelemetry teardown below, so no prompt fetch or refresh
	// can start or keep running once shutdown has begun; the drain itself
	// waits at the end alongside the score queue.
	c.prompts.beginShutdown()
	var result error
	if c.owned {
		flushErr := c.provider.ForceFlush(ctx)
		// Stop the processor directly before provider shutdown. The OTel provider
		// marks itself shut down before it checks ctx, so an already-ended context
		// can otherwise leave the batch worker running forever.
		processorErr := c.processor.Shutdown(ctx)
		providerErr := c.provider.Shutdown(ctx)
		result = errors.Join(flushErr, processorErr, providerErr, c.scores.Shutdown(ctx), c.prompts.shutdown(ctx))
	} else {
		flushErr := c.processor.ForceFlush(ctx)
		shutdownErr := c.processor.Shutdown(ctx)
		c.provider.UnregisterSpanProcessor(c.processor)
		c.releaseReservation()
		result = errors.Join(flushErr, shutdownErr, c.scores.Shutdown(ctx), c.prompts.shutdown(ctx))
	}
	c.shutdownResultMu.Lock()
	c.shutdownResult = result
	c.shutdownResultMu.Unlock()
	close(c.shutdownDone)
	return result
}

func (c *Client) currentShutdownResult() error {
	select {
	case <-c.shutdownDone:
		c.shutdownResultMu.RLock()
		defer c.shutdownResultMu.RUnlock()
		return c.shutdownResult
	default:
		return ErrShutdownInProgress
	}
}
