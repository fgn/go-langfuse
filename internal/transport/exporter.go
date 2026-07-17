package transport

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	defaultTimeout      = 10 * time.Second
	sdkName             = "lunte"
	maxHeaderValueBytes = 8 << 10
)

// maxRequestBytes caps one serialized OTLP request. The redacting wrapper
// bisects batches that exceed it, so one oversized span cannot discard
// otherwise-valid spans in the same request. It is a variable only so tests
// can exercise the splitting path with small payloads.
var maxRequestBytes = 4 << 20

var defaultRetry = RetryConfig{
	Enabled:         true,
	InitialInterval: 5 * time.Second,
	MaxInterval:     30 * time.Second,
	MaxElapsedTime:  time.Minute,
}

type RetryConfig struct {
	Enabled         bool
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration
}

// Config contains the internal transport configuration. Timeout and Retry are
// internal test and tuning seams; zero values select the explicit defaults
// above.
type Config struct {
	BaseURL    string
	PublicKey  string
	SecretKey  string
	SDKVersion string

	Timeout time.Duration
	Retry   *RetryConfig
}

// NewExporter constructs an OTLP/HTTP protobuf exporter without performing
// network I/O. It wraps the official otlptracehttp client and pins every
// environment-sensitive option so ambient OTEL_EXPORTER_OTLP_* variables
// intended for an application's separate generic exporter cannot change
// Langfuse's endpoint, headers, compression, TLS behavior, or timeout.
func NewExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	endpoint, err := NormalizeEndpoint(cfg.BaseURL)
	if err != nil { // Validated above; keep the constructor robust if this changes.
		return nil, err
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	if timeout < 0 {
		return nil, errors.New("lunte transport: timeout must not be negative")
	}

	retry := defaultRetry
	if cfg.Retry != nil {
		retry = *cfg.Retry
	}
	if err := validateRetry(retry); err != nil {
		return nil, err
	}

	authToken := base64.StdEncoding.EncodeToString(
		[]byte(cfg.PublicKey + ":" + cfg.SecretKey),
	)
	authorization := "Basic " + authToken
	headers := map[string]string{
		"Authorization":                authorization,
		"x-langfuse-ingestion-version": "4",
		"x-langfuse-sdk-name":          sdkName,
		"x-langfuse-sdk-version":       cfg.SDKVersion,
		"x-langfuse-public-key":        cfg.PublicKey,
	}

	// The client requires an https endpoint before it accepts a TLS
	// configuration. For https, pinning a minimal config overrides ambient
	// OTEL_EXPORTER_OTLP_*_CERTIFICATE/CLIENT_* material; for http, a nil
	// config clears that same ambient material, which would otherwise make the
	// client refuse to start against an insecure endpoint.
	var pinnedTLS *tls.Config
	if strings.HasPrefix(endpoint, "https://") {
		pinnedTLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithTimeout(timeout),
		otlptracehttp.WithCompression(otlptracehttp.NoCompression),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         retry.Enabled,
			InitialInterval: retry.InitialInterval,
			MaxInterval:     retry.MaxInterval,
			MaxElapsedTime:  retry.MaxElapsedTime,
		}),
		otlptracehttp.WithTLSClientConfig(pinnedTLS),
		// Standard proxy variables remain honored, but passing the proxy
		// explicitly also gives this client its own transport instead of the
		// exporter package's shared one.
		otlptracehttp.WithProxy(http.ProxyFromEnvironment),
		otlptracehttp.WithMaxRequestSize(maxRequestBytes),
	)

	redact := newRedactor(cfg.PublicKey, cfg.SecretKey, authorization, authToken)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, safeError(err, redact)
	}

	return &redactingExporter{
		delegate: exporter,
		redact:   redact,
	}, nil
}

// ValidateConfig performs pure configuration-shape validation. It creates no
// exporter, HTTP client, goroutine, or network request.
func ValidateConfig(cfg Config) error {
	if _, err := NormalizeEndpoint(cfg.BaseURL); err != nil {
		return err
	}
	if err := validateCredential("public key", cfg.PublicKey); err != nil {
		return err
	}
	if err := validateCredential("secret key", cfg.SecretKey); err != nil {
		return err
	}
	if err := validateHeaderValue("SDK version", cfg.SDKVersion); err != nil {
		return err
	}
	if cfg.Timeout < 0 {
		return errors.New("lunte transport: timeout must not be negative")
	}
	if cfg.Retry != nil {
		if err := validateRetry(*cfg.Retry); err != nil {
			return err
		}
	}
	return nil
}

func validateCredential(name, value string) error {
	if err := validateHeaderValue(name, value); err != nil {
		return err
	}
	if strings.Contains(value, ":") {
		return fmt.Errorf("lunte transport: %s must not contain a colon", name)
	}
	return nil
}

func validateHeaderValue(name, value string) error {
	if value == "" {
		return fmt.Errorf("lunte transport: %s is required", name)
	}
	if len(value) > maxHeaderValueBytes {
		return fmt.Errorf("lunte transport: %s exceeds the header size limit", name)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("lunte transport: %s must not have surrounding whitespace", name)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("lunte transport: %s must be valid UTF-8", name)
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return fmt.Errorf("lunte transport: %s must contain printable ASCII only", name)
		}
	}
	return nil
}

func validateRetry(retry RetryConfig) error {
	if !retry.Enabled {
		return nil
	}
	if retry.InitialInterval <= 0 || retry.MaxInterval <= 0 || retry.MaxElapsedTime <= 0 {
		return errors.New("lunte transport: enabled retry durations must be positive")
	}
	if retry.MaxInterval < retry.InitialInterval {
		return errors.New("lunte transport: retry max interval must not be less than its initial interval")
	}
	return nil
}

type redactingExporter struct {
	delegate sdktrace.SpanExporter
	redact   func(string) string
}

func (e *redactingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return safeError(e.exportSplitting(ctx, spans), e.redact)
}

// exportSplitting bisects a batch whose serialized request exceeds the
// configured maximum request size. otlptracehttp v1.44 rejects such a request
// before sending it instead of splitting, so without this a single oversized
// span would poison every sibling in its batch. A single span that alone
// exceeds the limit is dropped with the exporter's size error.
func (e *redactingExporter) exportSplitting(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.delegate.ExportSpans(ctx, spans)
	if err == nil || len(spans) <= 1 || !isRequestTooLarge(err) {
		return err
	}
	half := len(spans) / 2
	return errors.Join(
		e.exportSplitting(ctx, spans[:half]),
		e.exportSplitting(ctx, spans[half:]),
	)
}

// isRequestTooLarge matches the pinned otlptracehttp v1.44 client's
// request-size preflight error. The message carries no payload or secret.
func isRequestTooLarge(err error) bool {
	return err != nil && strings.Contains(err.Error(), "request body too large")
}

func (e *redactingExporter) Shutdown(ctx context.Context) error {
	return safeError(e.delegate.Shutdown(ctx), e.redact)
}

type redactedError struct {
	message  string
	canceled bool
	deadline bool
}

func (e *redactedError) Error() string { return e.message }

// Is retains useful context cancellation/deadline classification without
// retaining the original, potentially credential-bearing error.
func (e *redactedError) Is(target error) bool {
	return (target == context.Canceled && e.canceled) ||
		(target == context.DeadlineExceeded && e.deadline)
}

func safeError(err error, redact func(string) string) error {
	if err == nil {
		return nil
	}
	return &redactedError{
		message:  redact(err.Error()),
		canceled: errors.Is(err, context.Canceled),
		deadline: errors.Is(err, context.DeadlineExceeded),
	}
}

func newRedactor(values ...string) func(string) string {
	needles := make(map[string]struct{})
	for _, value := range values {
		if value == "" {
			continue
		}
		needles[value] = struct{}{}
		needles[url.QueryEscape(value)] = struct{}{}
		needles[base64.StdEncoding.EncodeToString([]byte(value))] = struct{}{}
	}

	ordered := make([]string, 0, len(needles))
	for value := range needles {
		if value != "" {
			ordered = append(ordered, value)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })

	replacements := make([]string, 0, len(ordered)*2)
	for _, value := range ordered {
		replacements = append(replacements, value, "[REDACTED]")
	}
	replacer := strings.NewReplacer(replacements...)
	return replacer.Replace
}
