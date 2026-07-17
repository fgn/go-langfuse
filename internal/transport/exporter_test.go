package transport

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	testPublicKey  = "pk-lf-transport-test"
	testSecretKey  = "sk-lf-transport-secret"
	testSDKVersion = "0.1.0"
)

var noRetry = RetryConfig{Enabled: false}

func TestNormalizeEndpoint(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"":                            "https://cloud.langfuse.com/api/public/otel/v1/traces",
		"https://cloud.langfuse.com":  "https://cloud.langfuse.com/api/public/otel/v1/traces",
		"https://cloud.langfuse.com/": "https://cloud.langfuse.com/api/public/otel/v1/traces",
		"https://us.cloud.langfuse.com/api/public/otel":              "https://us.cloud.langfuse.com/api/public/otel/v1/traces",
		"https://jp.cloud.langfuse.com/api/public/otel/":             "https://jp.cloud.langfuse.com/api/public/otel/v1/traces",
		"https://hipaa.cloud.langfuse.com/api/public/otel/v1/traces": "https://hipaa.cloud.langfuse.com/api/public/otel/v1/traces",
		"http://localhost:3000/api/public/otel/v1/traces/":           "http://localhost:3000/api/public/otel/v1/traces",
		"  http://127.0.0.1:3000  ":                                  "http://127.0.0.1:3000/api/public/otel/v1/traces",
	}

	for input, want := range tests {
		input, want := input, want
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeEndpoint(input)
			if err != nil {
				t.Fatalf("NormalizeEndpoint() error = %v", err)
			}
			if got != want {
				t.Fatalf("NormalizeEndpoint() = %q, want %q", got, want)
			}
		})
	}
}

func TestNormalizeEndpointRejectsUnsafeOrAmbiguousURLs(t *testing.T) {
	t.Parallel()

	tests := []string{
		"cloud.langfuse.com",
		"ftp://cloud.langfuse.com",
		"https://",
		"https://user:password@cloud.langfuse.com",
		"https://cloud.langfuse.com?project=one",
		"https://cloud.langfuse.com?",
		"https://cloud.langfuse.com#fragment",
		"https://cloud.langfuse.com#",
		"https://cloud.langfuse.com/%61pi/public/otel",
		"https://cloud.langfuse.com/custom/path",
		"https://cloud.langfuse.com/api/public/otel//",
	}

	for _, input := range tests {
		input := input
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			if got, err := NormalizeEndpoint(input); err == nil {
				t.Fatalf("NormalizeEndpoint() = %q, want error", got)
			}
		})
	}

	const embeddedSecret = "sk-lf-must-not-be-printed"
	_, err := NormalizeEndpoint("https://user:" + embeddedSecret + "@cloud.langfuse.com")
	if err == nil {
		t.Fatal("NormalizeEndpoint() error = nil")
	}
	if strings.Contains(err.Error(), embeddedSecret) {
		t.Fatalf("error disclosed embedded credential: %v", err)
	}
}

func TestExporterWireContract(t *testing.T) {
	evilRequests := new(atomic.Int32)
	evil := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		evilRequests.Add(1)
	}))
	t.Cleanup(evil.Close)
	certificatePath := filepath.Join(t.TempDir(), "ambient-otel-ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: evil.Certificate().Raw})
	if err := os.WriteFile(certificatePath, certificate, 0o600); err != nil {
		t.Fatalf("write ambient certificate: %v", err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", evil.URL+"/wrong")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "authorization=Basic%20wrong,x-langfuse-ingestion-version=1,x-ambient=wrong")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", certificatePath)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", "gzip")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "false")

	var requests atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != tracesPath {
			t.Errorf("path = %q, want %q", r.URL.Path, tracesPath)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Errorf("Content-Type = %q", got)
		}
		if got := r.Header.Get("Content-Encoding"); got != "" {
			t.Errorf("Content-Encoding = %q, want uncompressed protobuf", got)
		}
		if got := r.Header.Get("x-ambient"); got != "" {
			t.Errorf("ambient OTLP header leaked into Langfuse request: %q", got)
		}

		wantAuth := "Basic " + base64.StdEncoding.EncodeToString(
			[]byte(testPublicKey+":"+testSecretKey),
		)
		assertHeader(t, r, "Authorization", wantAuth)
		assertHeader(t, r, "x-langfuse-ingestion-version", "4")
		assertHeader(t, r, "x-langfuse-sdk-name", "go")
		assertHeader(t, r, "x-langfuse-sdk-version", testSDKVersion)
		assertHeader(t, r, "x-langfuse-public-key", testPublicKey)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		if len(body) == 0 {
			t.Error("protobuf body is empty")
		}
		var payload collectortracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode OTLP protobuf: %v", err)
			return
		}
		if got := decodedSpanNames(&payload); len(got) != 1 || got[0] != "wire-span" {
			t.Errorf("decoded span names = %v, want [wire-span]", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.Close)

	inputs := []string{
		receiver.URL,
		receiver.URL + "/",
		receiver.URL + otelBasePath,
		receiver.URL + otelBasePath + "/",
		receiver.URL + tracesPath,
		receiver.URL + tracesPath + "/",
	}
	for _, baseURL := range inputs {
		exporter := newTestExporter(t, Config{BaseURL: baseURL})
		if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{endedSpan(t)}); err != nil {
			t.Fatalf("ExportSpans(%q): %v", baseURL, err)
		}
		if err := exporter.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown(%q): %v", baseURL, err)
		}
	}

	if got, want := requests.Load(), int32(len(inputs)); got != want {
		t.Fatalf("receiver requests = %d, want %d", got, want)
	}
	if got := evilRequests.Load(); got != 0 {
		t.Fatalf("environment-configured endpoint received %d requests", got)
	}
}

func TestNewExporterPerformsNoNetworkIO(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	t.Cleanup(server.Close)

	exporter := newTestExporter(t, Config{BaseURL: server.URL})
	if got := requests.Load(); got != 0 {
		t.Fatalf("construction made %d HTTP requests", got)
	}
	if err := exporter.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("construction/shutdown made %d HTTP requests", got)
	}
}

func TestExporterNeverForwardsCredentialsToAnotherHost(t *testing.T) {
	t.Parallel()

	wantAuth := "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(testPublicKey+":"+testSecretKey),
	)

	var targetRequests atomic.Int32
	var targetAuth atomic.Value
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		targetAuth.Store(r.Header.Get("Authorization"))
		for name, values := range r.Header {
			for _, value := range values {
				for _, secret := range []string{testSecretKey, wantAuth, strings.TrimPrefix(wantAuth, "Basic ")} {
					if strings.Contains(value, secret) {
						t.Errorf("redirect target received secret material in header %s", name)
					}
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)

	targetURL, err := url.Parse(target.URL)
	if err != nil {
		t.Fatalf("parse target URL: %v", err)
	}
	// The redirect crosses hostnames (127.0.0.1 -> localhost), which is how
	// net/http decides to strip the Authorization header. Same-hostname
	// comparisons ignore the port.
	crossHost := "http://localhost:" + targetURL.Port() + tracesPath

	var originRequests atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originRequests.Add(1)
		w.Header().Set("Location", crossHost)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(origin.Close)

	exporter := newTestExporter(t, Config{BaseURL: origin.URL})
	_ = exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{endedSpan(t)})
	if got := originRequests.Load(); got != 1 {
		t.Fatalf("origin requests = %d, want 1", got)
	}
	if got := targetRequests.Load(); got != 1 {
		t.Fatalf("redirect target requests = %d, want 1", got)
	}
	if got := targetAuth.Load(); got != "" {
		t.Fatalf("redirect target received Authorization = %q, want empty", got)
	}
	_ = exporter.Shutdown(context.Background())
}

func TestExporterRedactsRemoteErrorBodies(t *testing.T) {
	t.Parallel()

	wantAuth := "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(testPublicKey+":"+testSecretKey),
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "secret=%s pair=%s:%s authorization=%s", testSecretKey, testPublicKey, testSecretKey, r.Header.Get("Authorization"))
	}))
	t.Cleanup(server.Close)

	exporter := newTestExporter(t, Config{BaseURL: server.URL})
	err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{endedSpan(t)})
	if err == nil {
		t.Fatal("ExportSpans() error = nil")
	}

	renderings := []string{
		err.Error(),
		fmt.Sprintf("%s", err),
		fmt.Sprintf("%q", err),
		fmt.Sprintf("%v", err),
		fmt.Sprintf("%+v", err),
		fmt.Sprintf("%#v", err),
		fmt.Sprintf("%#v", errors.Join(errors.New("outer"), err)),
	}
	for _, secret := range []string{
		testSecretKey,
		testPublicKey,
		testPublicKey + ":" + testSecretKey,
		wantAuth,
		strings.TrimPrefix(wantAuth, "Basic "),
	} {
		for _, rendered := range renderings {
			if strings.Contains(rendered, secret) {
				t.Fatalf("error rendering disclosed %q: %s", secret, rendered)
			}
		}
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("error did not contain redaction marker: %s", err)
	}
	_ = exporter.Shutdown(context.Background())
}

func TestExporterRetriesTransientFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		retryAfter string
	}{
		{name: "429 with Retry-After", status: http.StatusTooManyRequests, retryAfter: "1"},
		{name: "502", status: http.StatusBadGateway},
		{name: "503", status: http.StatusServiceUnavailable},
		{name: "504", status: http.StatusGatewayTimeout},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if requests.Add(1) == 1 {
					if test.retryAfter != "" {
						w.Header().Set("Retry-After", test.retryAfter)
					}
					w.WriteHeader(test.status)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)

			// MaxElapsedTime is generous so an honored one-second Retry-After
			// still completes; the failure mode is a test error, not a hang.
			retry := RetryConfig{
				Enabled:         true,
				InitialInterval: time.Millisecond,
				MaxInterval:     5 * time.Millisecond,
				MaxElapsedTime:  10 * time.Second,
			}
			exporter := newTestExporter(t, Config{BaseURL: server.URL, Retry: &retry})
			if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{endedSpan(t)}); err != nil {
				t.Fatalf("ExportSpans(): %v", err)
			}
			if got := requests.Load(); got != 2 {
				t.Fatalf("requests = %d, want 2", got)
			}
			_ = exporter.Shutdown(context.Background())
		})
	}
}

func TestExporterDoesNotRetryNonTransientStatus(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)

	retry := RetryConfig{
		Enabled:         true,
		InitialInterval: time.Millisecond,
		MaxInterval:     5 * time.Millisecond,
		MaxElapsedTime:  200 * time.Millisecond,
	}
	exporter := newTestExporter(t, Config{BaseURL: server.URL, Retry: &retry})
	if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{endedSpan(t)}); err == nil {
		t.Fatal("ExportSpans() error = nil, want permanent status failure")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1 without retries", got)
	}
	_ = exporter.Shutdown(context.Background())
}

func TestExporterUsesExplicitTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	exporter := newTestExporter(t, Config{
		BaseURL: server.URL,
		Timeout: 20 * time.Millisecond,
	})
	started := time.Now()
	err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{endedSpan(t)})
	if err == nil {
		t.Fatal("ExportSpans() error = nil, want timeout")
	}
	if elapsed := time.Since(started); elapsed >= 90*time.Millisecond {
		t.Fatalf("timeout was not enforced; export took %s", elapsed)
	}
	_ = exporter.Shutdown(context.Background())
}

func TestExporterSplitsOversizedBatchesAcrossRequests(t *testing.T) {
	// Not parallel: overrides the package-level request-size limit so the
	// splitting path is reachable with small payloads.
	restoreMaxRequestBytes(t, 4096)

	var requests atomic.Int32
	spansPerRequest := make(chan int, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		spansPerRequest <- len(decodedRequestSpanNames(t, r))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	exporter := newTestExporter(t, Config{BaseURL: server.URL})
	spans := []sdktrace.ReadOnlySpan{
		endedSpanWithPayload(t, "split-0", 1200),
		endedSpanWithPayload(t, "split-1", 1200),
		endedSpanWithPayload(t, "split-2", 1200),
		endedSpanWithPayload(t, "split-3", 1200),
	}
	if err := exporter.ExportSpans(context.Background(), spans); err != nil {
		t.Fatalf("ExportSpans(): %v", err)
	}
	if got := requests.Load(); got < 2 {
		t.Fatalf("requests = %d, want the oversized batch split across at least 2", got)
	}
	total := 0
	for range requests.Load() {
		count := <-spansPerRequest
		if count == 0 || count == len(spans) {
			t.Fatalf("request span count = %d, want a proper subset of %d", count, len(spans))
		}
		total += count
	}
	if total != len(spans) {
		t.Fatalf("delivered spans = %d, want all %d", total, len(spans))
	}
	_ = exporter.Shutdown(context.Background())
}

func TestExporterDropsOnlyTheSpanExceedingTheRequestLimit(t *testing.T) {
	// Not parallel: overrides the package-level request-size limit.
	restoreMaxRequestBytes(t, 4096)

	var requests atomic.Int32
	var delivered sync.Map
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		for _, name := range decodedRequestSpanNames(t, r) {
			delivered.Store(name, true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	exporter := newTestExporter(t, Config{BaseURL: server.URL})
	spans := []sdktrace.ReadOnlySpan{
		endedSpanWithPayload(t, "poison", 8192),
		endedSpanWithPayload(t, "kept-0", 64),
		endedSpanWithPayload(t, "kept-1", 64),
	}
	err := exporter.ExportSpans(context.Background(), spans)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("ExportSpans() error = %v, want a request-size error for the oversized span", err)
	}
	if _, found := delivered.Load("poison"); found {
		t.Fatal("oversized span was sent over HTTP, want a preflight rejection")
	}
	for _, name := range []string{"kept-0", "kept-1"} {
		if _, found := delivered.Load(name); !found {
			t.Fatalf("sibling span %q was not delivered", name)
		}
	}
	_ = exporter.Shutdown(context.Background())
}

func restoreMaxRequestBytes(t *testing.T, limit int) {
	t.Helper()
	previous := maxRequestBytes
	maxRequestBytes = limit
	t.Cleanup(func() { maxRequestBytes = previous })
}

func decodedRequestSpanNames(t *testing.T, r *http.Request) []string {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request body: %v", err)
		return nil
	}
	var payload collectortracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &payload); err != nil {
		t.Errorf("decode OTLP protobuf: %v", err)
		return nil
	}
	return decodedSpanNames(&payload)
}

func TestNewExporterValidatesWithoutDisclosingValues(t *testing.T) {
	t.Parallel()

	retry := RetryConfig{
		Enabled:         true,
		InitialInterval: 2 * time.Second,
		MaxInterval:     time.Second,
		MaxElapsedTime:  time.Minute,
	}
	tests := []Config{
		{BaseURL: "https://cloud.langfuse.com", SecretKey: testSecretKey, SDKVersion: testSDKVersion},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SDKVersion: testSDKVersion},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: "pk:invalid", SecretKey: testSecretKey, SDKVersion: testSDKVersion},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: "sk:invalid", SDKVersion: testSDKVersion},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: testSecretKey},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: testSecretKey, SDKVersion: "bad\nversion"},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: testSecretKey, SDKVersion: testSDKVersion, Timeout: -time.Second},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: testSecretKey, SDKVersion: testSDKVersion, Retry: &retry},
		{BaseURL: "https://user:embedded-secret@cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: testSecretKey, SDKVersion: testSDKVersion},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: strings.Repeat("p", maxHeaderValueBytes+1), SecretKey: testSecretKey, SDKVersion: testSDKVersion},
		{BaseURL: "https://cloud.langfuse.com", PublicKey: testPublicKey, SecretKey: strings.Repeat("s", maxHeaderValueBytes+1), SDKVersion: testSDKVersion},
	}

	for i, cfg := range tests {
		_, err := NewExporter(context.Background(), cfg)
		if err == nil {
			t.Fatalf("case %d: NewExporter() error = nil", i)
		}
		for _, forbidden := range []string{testSecretKey, "embedded-secret"} {
			if strings.Contains(err.Error(), forbidden) {
				t.Fatalf("case %d disclosed secret: %v", i, err)
			}
		}
	}
}

func TestRedactedErrorRetainsContextClassification(t *testing.T) {
	t.Parallel()

	err := safeError(context.DeadlineExceeded, newRedactor(testSecretKey))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.Is(%v, DeadlineExceeded) = false", err)
	}
}

func newTestExporter(t *testing.T, cfg Config) sdktrace.SpanExporter {
	t.Helper()
	if cfg.PublicKey == "" {
		cfg.PublicKey = testPublicKey
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = testSecretKey
	}
	if cfg.SDKVersion == "" {
		cfg.SDKVersion = testSDKVersion
	}
	if cfg.Retry == nil {
		cfg.Retry = &noRetry
	}

	exporter, err := NewExporter(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewExporter(): %v", err)
	}
	return exporter
}

func endedSpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()
	return endedSpanWithPayload(t, "wire-span", 0)
}

func endedSpanWithPayload(t *testing.T, name string, payloadBytes int) sdktrace.ReadOnlySpan {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	_, span := provider.Tracer("transport-test").Start(context.Background(), name)
	if payloadBytes > 0 {
		span.SetAttributes(attribute.String("payload", strings.Repeat("x", payloadBytes)))
	}
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	return ended[0]
}

func assertHeader(t *testing.T, r *http.Request, name, want string) {
	t.Helper()
	if got := r.Header.Get(name); got != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}

func decodedSpanNames(payload *collectortracepb.ExportTraceServiceRequest) []string {
	var names []string
	for _, resourceSpans := range payload.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				names = append(names, span.Name)
			}
		}
	}
	return names
}
