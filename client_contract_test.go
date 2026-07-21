package langfuse

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	resourcepkg "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	testClientPublicKey = "test-public-key"
	testClientSecretKey = "test-secret-key"
)

var langfuseEnvironmentVariables = []string{
	"LANGFUSE_PUBLIC_KEY",
	"LANGFUSE_SECRET_KEY",
	"LANGFUSE_BASE_URL",
	"LANGFUSE_TRACING_ENVIRONMENT",
	"LANGFUSE_RELEASE",
	"LANGFUSE_TRACING_ENABLED",
	"LANGFUSE_CONTENT_CAPTURE_ENABLED",
	"LANGFUSE_HOST",
}

func TestConfigFromEnvReadsOnlyDocumentedVariables(t *testing.T) {
	clearLangfuseEnvironment(t)

	t.Setenv("LANGFUSE_PUBLIC_KEY", testClientPublicKey)
	t.Setenv("LANGFUSE_SECRET_KEY", testClientSecretKey)
	t.Setenv("LANGFUSE_BASE_URL", "https://us.cloud.langfuse.com")
	t.Setenv("LANGFUSE_TRACING_ENVIRONMENT", "test_env")
	t.Setenv("LANGFUSE_RELEASE", "release-test")
	t.Setenv("LANGFUSE_TRACING_ENABLED", "false")
	t.Setenv("LANGFUSE_CONTENT_CAPTURE_ENABLED", "false")
	t.Setenv("LANGFUSE_HOST", "https://must-be-ignored.example")

	got := ConfigFromEnv()
	want := Config{
		BaseURL:               "https://us.cloud.langfuse.com",
		PublicKey:             testClientPublicKey,
		SecretKey:             testClientSecretKey,
		Environment:           "test_env",
		Release:               "release-test",
		Disabled:              true,
		DisableContentCapture: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ConfigFromEnv() = %#v, want %#v", printableConfig(got), printableConfig(want))
	}
}

func TestConfigFromEnvDoesNotReadLangfuseHostAlias(t *testing.T) {
	clearLangfuseEnvironment(t)
	t.Setenv("LANGFUSE_HOST", "https://legacy-host-must-be-ignored.example")

	got := ConfigFromEnv()
	if got.BaseURL != "" {
		t.Fatalf("ConfigFromEnv().BaseURL = %q, want empty when only LANGFUSE_HOST is set", got.BaseURL)
	}
}

func TestConfigFromEnvDefaultsToTracingAndContentEnabled(t *testing.T) {
	clearLangfuseEnvironment(t)

	got := ConfigFromEnv()
	if got.Disabled {
		t.Fatal("ConfigFromEnv().Disabled = true, want tracing enabled by default")
	}
	if got.DisableContentCapture {
		t.Fatal("ConfigFromEnv().DisableContentCapture = true, want content capture enabled by default")
	}
	if got.envErr != nil {
		t.Fatalf("ConfigFromEnv().envErr = %v, want nil", got.envErr)
	}
	if got.BaseURL != "" || got.Environment != "" || got.ServiceName != "" {
		t.Fatalf("ConfigFromEnv() unexpectedly materialized constructor defaults: %#v", printableConfig(got))
	}
}

func TestConfigFromEnvStrictBooleanValues(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		enabled bool
	}{
		{name: "true", raw: "true", enabled: true},
		{name: "uppercase true", raw: "TRUE", enabled: true},
		{name: "trimmed true", raw: "  true\t", enabled: true},
		{name: "false", raw: "false", enabled: false},
		{name: "mixed-case false", raw: "FaLsE", enabled: false},
		{name: "trimmed false", raw: "\tfalse  ", enabled: false},
	}

	for _, variable := range []string{"LANGFUSE_TRACING_ENABLED", "LANGFUSE_CONTENT_CAPTURE_ENABLED"} {
		t.Run(variable, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					clearLangfuseEnvironment(t)
					t.Setenv(variable, test.raw)

					got := ConfigFromEnv()
					if got.envErr != nil {
						t.Fatalf("ConfigFromEnv().envErr = %v, want nil", got.envErr)
					}
					if variable == "LANGFUSE_TRACING_ENABLED" && got.Disabled == test.enabled {
						t.Fatalf("Disabled = %v for %q, want %v", got.Disabled, test.raw, !test.enabled)
					}
					if variable == "LANGFUSE_CONTENT_CAPTURE_ENABLED" && got.DisableContentCapture == test.enabled {
						t.Fatalf("DisableContentCapture = %v for %q, want %v", got.DisableContentCapture, test.raw, !test.enabled)
					}
				})
			}
		})
	}
}

func TestConfigFromEnvRejectsInvalidBooleanValues(t *testing.T) {
	invalid := []string{"", "0", "1", "yes", "no", "enabled", "disabled", "truthy"}
	for _, variable := range []string{"LANGFUSE_TRACING_ENABLED", "LANGFUSE_CONTENT_CAPTURE_ENABLED"} {
		t.Run(variable, func(t *testing.T) {
			for _, raw := range invalid {
				t.Run(raw, func(t *testing.T) {
					clearLangfuseEnvironment(t)
					t.Setenv("LANGFUSE_PUBLIC_KEY", testClientPublicKey)
					t.Setenv("LANGFUSE_SECRET_KEY", testClientSecretKey)
					t.Setenv(variable, raw)

					cfg := ConfigFromEnv()
					if cfg.envErr == nil {
						t.Fatal("ConfigFromEnv().envErr = nil, want strict boolean error")
					}
					if !strings.Contains(cfg.envErr.Error(), variable) {
						t.Fatalf("environment error = %q, want variable name %q", cfg.envErr, variable)
					}
					client, err := New(context.Background(), cfg)
					if err == nil || client != nil {
						t.Fatalf("New() = (%v, %v), want (nil, error)", client, err)
					}
					if strings.Contains(err.Error(), testClientSecretKey) {
						t.Fatalf("New() error exposed printable test credential: %v", err)
					}
				})
			}
		})
	}
}

func TestDisabledEnvironmentBypassesOtherInvalidConfiguration(t *testing.T) {
	clearLangfuseEnvironment(t)
	t.Setenv("LANGFUSE_TRACING_ENABLED", "false")
	t.Setenv("LANGFUSE_CONTENT_CAPTURE_ENABLED", "not-a-boolean")
	t.Setenv("LANGFUSE_BASE_URL", "not a URL")

	cfg := ConfigFromEnv()
	if !cfg.Disabled || cfg.envErr == nil {
		t.Fatalf("ConfigFromEnv() = %#v, want disabled config retaining unrelated env error", printableConfig(cfg))
	}
	client, err := New(nil, cfg)
	if err != nil {
		t.Fatalf("New(disabled config) error = %v, want nil", err)
	}
	assertTrueNoopClient(t, client)
}

func TestNewAppliesOwnedProviderDefaults(t *testing.T) {
	clearLangfuseEnvironment(t)
	receiver := newRootOTLPReceiver(t)

	client, err := New(context.Background(), Config{
		BaseURL:   receiver.server.URL,
		PublicKey: testClientPublicKey,
		SecretKey: testClientSecretKey,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { shutdownClient(t, client) })

	if client.disabled || !client.owned || client.reserved || client.disableContentCapture {
		t.Fatalf("owned client flags = disabled:%v owned:%v reserved:%v disableContent:%v", client.disabled, client.owned, client.reserved, client.disableContentCapture)
	}

	_, observation := client.StartObservation(context.Background(), "default-contract", TypeSpan, ObservationAttributes{})
	observation.End()
	flushClient(t, client)

	payload := receiver.nextPayload(t)
	span, resource := firstSpanAndResource(t, payload)
	var wantServiceName string
	for _, item := range resourcepkg.Default().Attributes() {
		if string(item.Key) == "service.name" {
			wantServiceName = item.Value.AsString()
		}
	}
	if got := stringAttribute(resource.Attributes, "service.name"); got != wantServiceName {
		t.Fatalf("resource service.name = %q, want resource.Default value %q", got, wantServiceName)
	}
	if got := stringAttribute(span.Attributes, "langfuse.environment"); got != "default" {
		t.Fatalf("span langfuse.environment = %q, want default", got)
	}
}

func TestOwnedProviderHonorsOTELServiceName(t *testing.T) {
	const helper = "LANGFUSE_SERVICE_NAME_HELPER"
	if os.Getenv(helper) != "1" {
		command := exec.Command(os.Args[0], "-test.run=^TestOwnedProviderHonorsOTELServiceName$")
		command.Env = append(os.Environ(), helper+"=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("service-name helper failed: %v\n%s", err, output)
		}
		return
	}

	t.Setenv("OTEL_SERVICE_NAME", "otel-environment-service")
	receiver := newRootOTLPReceiver(t)
	client, err := New(context.Background(), Config{
		BaseURL:   receiver.server.URL,
		PublicKey: testClientPublicKey,
		SecretKey: testClientSecretKey,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { shutdownClient(t, client) })
	_, observation := client.StartObservation(context.Background(), "otel-service-name", TypeSpan, ObservationAttributes{})
	observation.End()
	flushClient(t, client)

	_, resource := firstSpanAndResource(t, receiver.nextPayload(t))
	if got := stringAttribute(resource.Attributes, "service.name"); got != "otel-environment-service" {
		t.Fatalf("resource service.name = %q, want OTEL_SERVICE_NAME value", got)
	}
}

func TestShutdownFlushesAndReturnsRedactedExportFailure(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, testClientPublicKey+":"+testClientSecretKey+" "+request.Header.Get("Authorization"))
	}))
	t.Cleanup(server.Close)

	client, err := New(context.Background(), Config{
		BaseURL:   server.URL,
		PublicKey: testClientPublicKey,
		SecretKey: testClientSecretKey,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, observation := client.StartObservation(context.Background(), "shutdown-export-failure", TypeSpan, ObservationAttributes{})
	observation.End()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Shutdown(ctx)
	if err == nil {
		t.Fatal("Shutdown() error = nil, want final export failure")
	}
	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte(testClientPublicKey+":"+testClientSecretKey))
	for _, rendered := range []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%#v", err)} {
		for _, forbidden := range []string{testClientPublicKey, testClientSecretKey, wantAuthorization} {
			if strings.Contains(rendered, forbidden) {
				t.Fatalf("Shutdown error disclosed credential %q: %s", forbidden, rendered)
			}
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("shutdown export requests = %d, want 1", got)
	}
	if !client.stopped.Load() {
		t.Fatal("client was not stopped after failed final export")
	}
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated Shutdown() error = %v, want nil", err)
	}
}

func TestDisabledClientIsTrueNoop(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { shutdownProvider(t, provider) })
	client, err := New(nil, Config{
		Disabled:       true,
		BaseURL:        "not a URL",
		PublicKey:      "printable-but-ignored-public-key",
		SecretKey:      "printable-but-ignored-secret-key",
		Environment:    "INVALID ENVIRONMENT",
		TracerProvider: provider,
		envErr:         errors.New("printable ignored environment error"),
	})
	if err != nil {
		t.Fatalf("New(disabled config) error = %v", err)
	}
	assertTrueNoopClient(t, client)
	assertProviderUnreserved(t, provider)

	ctx := context.WithValue(context.Background(), testContextKey{}, "kept")
	if got := client.WithTraceAttributes(ctx, TraceAttributes{Name: "ignored"}); got != ctx {
		t.Fatal("disabled WithTraceAttributes returned a different context")
	}
	gotCtx, observation := client.StartObservation(ctx, "ignored", TypeGeneration, ObservationAttributes{Input: "ignored"})
	if gotCtx != ctx {
		t.Fatal("disabled StartObservation returned a different context")
	}
	assertTrueNoopObservation(t, observation)
	client.Event(ctx, "ignored", ObservationAttributes{Input: "ignored"})
	if err := client.Flush(nil); err != nil {
		t.Fatalf("disabled Flush(nil) error = %v", err)
	}
	if err := client.Shutdown(nil); err != nil {
		t.Fatalf("disabled Shutdown(nil) error = %v", err)
	}
	assertTrueNoopClient(t, client)
}

func TestZeroAndNilClientsAreSafeNoops(t *testing.T) {
	clients := []*Client{new(Client), nil}
	ctx := context.WithValue(context.Background(), testContextKey{}, "kept")
	for index, client := range clients {
		if got := client.WithTraceAttributes(ctx, TraceAttributes{Name: "ignored"}); got != ctx {
			t.Errorf("client %d WithTraceAttributes returned a different context", index)
		}
		gotCtx, observation := client.StartObservation(ctx, "ignored", TypeSpan, ObservationAttributes{})
		if gotCtx != ctx {
			t.Errorf("client %d StartObservation returned a different context", index)
		}
		assertTrueNoopObservation(t, observation)
		client.Event(ctx, "ignored", ObservationAttributes{})
		if err := client.Flush(nil); err != nil {
			t.Errorf("client %d Flush(nil) error = %v", index, err)
		}
		if err := client.Shutdown(nil); err != nil {
			t.Errorf("client %d Shutdown(nil) error = %v", index, err)
		}
	}
}

func TestZeroAndNilObservationsAreSafeNoops(t *testing.T) {
	observations := []*Observation{new(Observation), nil}
	for index, observation := range observations {
		observation.Update(ObservationAttributes{Input: false, Output: 0})
		observation.RecordError(errors.New("printable test error"))
		observation.RecordError(nil)
		observation.End()
		observation.End()
		observation.EndAt(time.Now())
		observation.EndAt(time.Time{})
		if got := observation.TraceID(); got != "" {
			t.Errorf("observation %d TraceID() = %q, want empty", index, got)
		}
		if got := observation.ID(); got != "" {
			t.Errorf("observation %d ID() = %q, want empty", index, got)
		}
	}
}

func TestDisabledAndZeroValuesAreConcurrencySafe(t *testing.T) {
	disabled, err := New(nil, Config{Disabled: true})
	if err != nil {
		t.Fatalf("New(disabled) error = %v", err)
	}
	zeroClient := new(Client)
	zeroObservation := new(Observation)
	ctx := context.Background()

	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			for j := range 100 {
				for _, client := range []*Client{disabled, zeroClient, nil} {
					childCtx, observation := client.StartObservation(ctx, "ignored", TypeSpan, ObservationAttributes{})
					if childCtx != ctx {
						t.Errorf("no-op client returned a different context")
					}
					observation.Update(ObservationAttributes{Output: j})
					observation.End()
					_ = client.Flush(nil)
					_ = client.Shutdown(nil)
				}
				zeroObservation.Update(ObservationAttributes{Output: j})
				zeroObservation.RecordError(errors.New("printable concurrent error"))
				zeroObservation.End()
			}
		})
	}
	workers.Wait()

	assertTrueNoopClient(t, disabled)
	assertTrueNoopClient(t, zeroClient)
	assertTrueNoopObservation(t, zeroObservation)
}

func TestDuplicateBorrowedProviderReturnsTrueNoopAndOwnerShutdownUnlocks(t *testing.T) {
	diagnostics := captureRootDiagnostics(t)
	ownerReceiver := newRootOTLPReceiver(t)
	successorReceiver := newRootOTLPReceiver(t)
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { shutdownProvider(t, provider) })

	owner, err := New(context.Background(), borrowedTestConfig(provider, ownerReceiver.server.URL, "owner"))
	if err != nil {
		t.Fatalf("New(owner) error = %v", err)
	}
	t.Cleanup(func() { shutdownClient(t, owner) })
	if !owner.reserved || owner.disabled {
		t.Fatalf("owner flags = reserved:%v disabled:%v", owner.reserved, owner.disabled)
	}

	duplicate, err := New(context.Background(), borrowedTestConfig(provider, successorReceiver.server.URL, "duplicate"))
	if err != nil {
		t.Fatalf("New(duplicate) error = %v", err)
	}
	assertTrueNoopClient(t, duplicate)
	assertProviderOwner(t, provider, owner)
	assertDiagnosticCount(t, diagnostics, "duplicate client is disabled", 1)

	ctx := context.Background()
	duplicateCtx, duplicateObservation := duplicate.StartObservation(ctx, "must-not-export", TypeGeneration, ObservationAttributes{
		Input: "printable duplicate payload",
	})
	if duplicateCtx != ctx {
		t.Fatal("duplicate no-op client returned a different context")
	}
	duplicateObservation.End()
	if err := duplicate.Flush(context.Background()); err != nil {
		t.Fatalf("duplicate Flush() error = %v", err)
	}
	if err := duplicate.Shutdown(nil); err != nil {
		t.Fatalf("duplicate Shutdown(nil) error = %v", err)
	}
	assertProviderOwner(t, provider, owner)
	if got := successorReceiver.requests.Load(); got != 0 {
		t.Fatalf("duplicate project received %d requests, want zero", got)
	}

	_, ownerObservation := owner.StartObservation(ctx, "owner-export", TypeSpan, ObservationAttributes{})
	ownerObservation.End()
	flushClient(t, owner)
	if got := ownerReceiver.requests.Load(); got != 1 {
		t.Fatalf("owner endpoint requests = %d, want 1", got)
	}
	if got := successorReceiver.requests.Load(); got != 0 {
		t.Fatalf("duplicate endpoint requests = %d, want 0", got)
	}

	shutdownClient(t, owner)
	assertProviderUnreserved(t, provider)

	successor, err := New(context.Background(), borrowedTestConfig(provider, successorReceiver.server.URL, "successor"))
	if err != nil {
		t.Fatalf("New(successor) error = %v", err)
	}
	t.Cleanup(func() { shutdownClient(t, successor) })
	if successor.disabled || !successor.reserved {
		t.Fatalf("successor flags = disabled:%v reserved:%v", successor.disabled, successor.reserved)
	}
	assertProviderOwner(t, provider, successor)

	// Repeated shutdown of the former owner must not release its successor's
	// reservation.
	shutdownClient(t, owner)
	assertProviderOwner(t, provider, successor)

	_, successorObservation := successor.StartObservation(ctx, "successor-export", TypeSpan, ObservationAttributes{})
	successorObservation.End()
	flushClient(t, successor)
	if got := successorReceiver.requests.Load(); got != 1 {
		t.Fatalf("successor endpoint requests = %d, want 1", got)
	}

	shutdownClient(t, successor)
	assertProviderUnreserved(t, provider)
}

func TestConcurrentNewOnBorrowedProviderCreatesExactlyOneOwner(t *testing.T) {
	diagnostics := captureRootDiagnostics(t)
	receiver := newRootOTLPReceiver(t)
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { shutdownProvider(t, provider) })

	const clientCount = 32
	clients := make([]*Client, clientCount)
	errorsByIndex := make([]error, clientCount)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range clients {
		workers.Go(func() {
			<-start
			clients[i], errorsByIndex[i] = New(
				context.Background(),
				borrowedTestConfig(provider, receiver.server.URL, "concurrent"),
			)
		})
	}
	close(start)
	workers.Wait()
	t.Cleanup(func() {
		for _, client := range clients {
			shutdownClient(t, client)
		}
	})

	var owner *Client
	disabledCount := 0
	for index, client := range clients {
		if errorsByIndex[index] != nil {
			t.Errorf("New() call %d error = %v", index, errorsByIndex[index])
			continue
		}
		if client == nil {
			t.Errorf("New() call %d returned nil client", index)
			continue
		}
		if client.disabled {
			disabledCount++
			assertTrueNoopClient(t, client)
			continue
		}
		if owner != nil {
			t.Errorf("multiple active clients returned: %p and %p", owner, client)
		}
		owner = client
	}
	if owner == nil {
		t.Fatal("concurrent New() calls returned no active owner")
	}
	if disabledCount != clientCount-1 {
		t.Fatalf("disabled clients = %d, want %d", disabledCount, clientCount-1)
	}
	assertProviderOwner(t, provider, owner)
	assertDiagnosticCount(t, diagnostics, "duplicate client is disabled", clientCount-1)

	// Shutting down every duplicate concurrently must not release the owner.
	workers = sync.WaitGroup{}
	for _, client := range clients {
		if client == owner {
			continue
		}
		workers.Add(1)
		go func(client *Client) {
			defer workers.Done()
			if err := client.Shutdown(nil); err != nil {
				t.Errorf("duplicate Shutdown(nil) error = %v", err)
			}
		}(client)
	}
	workers.Wait()
	assertProviderOwner(t, provider, owner)

	_, observation := owner.StartObservation(context.Background(), "concurrent-owner-export", TypeSpan, ObservationAttributes{})
	observation.End()
	flushClient(t, owner)
	if got := receiver.requests.Load(); got != 1 {
		t.Fatalf("receiver requests = %d, want exactly one owner export", got)
	}

	shutdownClient(t, owner)
	assertProviderUnreserved(t, provider)
}

type testContextKey struct{}

func assertTrueNoopClient(t *testing.T, client *Client) {
	t.Helper()
	if client == nil {
		t.Fatal("no-op client is nil")
	}
	if !client.isDisabled() {
		t.Fatal("client is not disabled/no-op")
	}
	if client.provider != nil || client.processor != nil || client.tracer != nil || client.owned || client.reserved {
		t.Fatalf("no-op client owns resources: provider=%p processor=%p tracer=%v owned=%v reserved=%v", client.provider, client.processor, client.tracer, client.owned, client.reserved)
	}
}

func assertTrueNoopObservation(t *testing.T, observation *Observation) {
	t.Helper()
	if observation == nil {
		t.Fatal("no-op observation is nil")
	}
	observation.Update(ObservationAttributes{Input: false, Output: 0})
	observation.RecordError(errors.New("printable ignored error"))
	observation.End()
	observation.End()
	if got := observation.TraceID(); got != "" {
		t.Fatalf("no-op TraceID() = %q, want empty", got)
	}
	if got := observation.ID(); got != "" {
		t.Fatalf("no-op ID() = %q, want empty", got)
	}
}

func borrowedTestConfig(provider *sdktrace.TracerProvider, baseURL, project string) Config {
	return Config{
		BaseURL:        baseURL,
		PublicKey:      testClientPublicKey + "-" + project,
		SecretKey:      testClientSecretKey + "-" + project,
		TracerProvider: provider,
	}
}

func assertProviderOwner(t *testing.T, provider *sdktrace.TracerProvider, want *Client) {
	t.Helper()
	borrowedProviderRegistry.Lock()
	got := borrowedProviderRegistry.owners[provider]
	borrowedProviderRegistry.Unlock()
	if got != want {
		t.Fatalf("provider owner = %p, want %p", got, want)
	}
}

func assertProviderUnreserved(t *testing.T, provider *sdktrace.TracerProvider) {
	t.Helper()
	borrowedProviderRegistry.Lock()
	_, found := borrowedProviderRegistry.owners[provider]
	borrowedProviderRegistry.Unlock()
	if found {
		t.Fatal("provider remains reserved after owner shutdown")
	}
}

func flushClient(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func shutdownClient(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

func shutdownProvider(t *testing.T, provider *sdktrace.TracerProvider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Shutdown(ctx); err != nil {
		t.Errorf("TracerProvider.Shutdown() error = %v", err)
	}
}

type rootOTLPReceiver struct {
	server   *httptest.Server
	requests atomic.Int32
	payloads chan *collectortracepb.ExportTraceServiceRequest
}

func newRootOTLPReceiver(t *testing.T) *rootOTLPReceiver {
	t.Helper()
	receiver := &rootOTLPReceiver{
		payloads: make(chan *collectortracepb.ExportTraceServiceRequest, 16),
	}
	receiver.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read OTLP body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload collectortracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode OTLP body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		receiver.requests.Add(1)
		receiver.payloads <- &payload
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.server.Close)
	return receiver
}

func (r *rootOTLPReceiver) nextPayload(t *testing.T) *collectortracepb.ExportTraceServiceRequest {
	t.Helper()
	select {
	case payload := <-r.payloads:
		return payload
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OTLP payload")
		return nil
	}
}

func firstSpanAndResource(
	t *testing.T,
	payload *collectortracepb.ExportTraceServiceRequest,
) (*tracepb.Span, *resourcepb.Resource) {
	t.Helper()
	for _, resourceSpans := range payload.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			if len(scopeSpans.Spans) != 0 {
				return scopeSpans.Spans[0], resourceSpans.Resource
			}
		}
	}
	t.Fatal("OTLP payload contains no spans")
	return nil, nil
}

func stringAttribute(attributes []*commonpb.KeyValue, key string) string {
	for _, item := range attributes {
		if item.Key == key && item.Value != nil {
			return item.Value.GetStringValue()
		}
	}
	return ""
}

type rootDiagnosticRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (r *rootDiagnosticRecorder) Handle(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, err.Error())
}

func (r *rootDiagnosticRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.messages...)
}

func captureRootDiagnostics(t *testing.T) *rootDiagnosticRecorder {
	t.Helper()
	previous := otel.GetErrorHandler()
	recorder := &rootDiagnosticRecorder{}
	otel.SetErrorHandler(recorder)
	t.Cleanup(func() { otel.SetErrorHandler(previous) })
	return recorder
}

func assertDiagnosticCount(t *testing.T, recorder *rootDiagnosticRecorder, text string, want int) {
	t.Helper()
	count := 0
	for _, message := range recorder.snapshot() {
		if strings.Contains(message, text) {
			count++
		}
	}
	if count != want {
		t.Fatalf("diagnostics containing %q = %d, want %d; all diagnostics: %v", text, count, want, recorder.snapshot())
	}
}

func clearLangfuseEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range langfuseEnvironmentVariables {
		value, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, value)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
}

func printableConfig(cfg Config) Config {
	// The test credentials are already printable and non-sensitive. Removing
	// the callback makes failure formatting stable and useful.
	cfg.Mask = nil
	return cfg
}
