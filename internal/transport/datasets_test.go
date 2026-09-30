package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newDatasetsTestClient(t *testing.T, baseURL string) *DatasetsClient {
	t.Helper()
	client, err := NewDatasetsClient(Config{
		BaseURL:    baseURL,
		PublicKey:  "pk-lf-datasets",
		SecretKey:  "sk-lf-datasets",
		SDKVersion: "test",
	})
	if err != nil {
		t.Fatalf("NewDatasetsClient() error = %v", err)
	}
	client.retryInterval = time.Millisecond
	return client
}

const wireTimes = `"createdAt":"2026-09-30T10:00:00.000Z","updatedAt":"2026-09-30T11:00:00.000Z"`

func wireDataset(name string) string {
	return fmt.Sprintf(`{"id":"ds-1","projectId":"p","name":%q,"description":null,"metadata":{"a":1},`+
		`"inputSchema":null,"expectedOutputSchema":{"type":"object"},%s}`, name, wireTimes)
}

func wireItem(id, datasetName string) string {
	return fmt.Sprintf(`{"id":%q,"datasetId":"ds-1","datasetName":%q,"status":"ACTIVE",`+
		`"input":{"q":"x"},"expectedOutput":"Paris","metadata":null,"sourceTraceId":null,`+
		`"sourceObservationId":null,%s,"mediaReferences":[]}`, id, datasetName, wireTimes)
}

func wirePage(page, limit, totalItems, totalPages int, items ...string) string {
	return fmt.Sprintf(`{"data":[%s],"meta":{"page":%d,"limit":%d,"totalItems":%d,"totalPages":%d}}`,
		strings.Join(items, ","), page, limit, totalItems, totalPages)
}

func asDatasetError(t *testing.T, err error) *DatasetError {
	t.Helper()
	var target *DatasetError
	if !errors.As(err, &target) {
		t.Fatalf("error = %v (%T), want *DatasetError", err, err)
	}
	return target
}

func TestDatasetsRequestShape(t *testing.T) {
	t.Parallel()
	type seen struct {
		method, requestURI, contentType, accept, user, pass, sdkName, sdkVersion, publicKey string
		body                                                                                []byte
	}
	var last atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		user, pass, _ := r.BasicAuth()
		last.Store(seen{
			method: r.Method, requestURI: r.RequestURI, contentType: r.Header.Get("Content-Type"),
			accept: r.Header.Get("Accept"), user: user, pass: pass,
			sdkName: r.Header.Get("x-langfuse-sdk-name"), sdkVersion: r.Header.Get("x-langfuse-sdk-version"),
			publicKey: r.Header.Get("x-langfuse-public-key"), body: body,
		})
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/public/v2/datasets":
			_, _ = io.WriteString(w, wireDataset("team/set"))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/public/v2/datasets/"):
			_, _ = io.WriteString(w, wireDataset(strings.TrimPrefix(r.URL.Path, "/api/public/v2/datasets/")))
		case r.Method == http.MethodPost && r.URL.Path == "/api/public/dataset-items":
			_, _ = io.WriteString(w, wireItem("item-1", "team/set"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/public/dataset-items":
			_, _ = io.WriteString(w, wirePage(2, 8, 9, 2, wireItem("item-9", "team/set")))
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, wireItem(strings.TrimPrefix(r.URL.Path, "/api/public/dataset-items/"), "team/set"))
		case r.Method == http.MethodDelete:
			_, _ = io.WriteString(w, `{"message":"deleted"}`)
		}
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL+"/api/public/otel")
	ctx := context.Background()

	check := func(name, method, requestURI string, body string) {
		t.Helper()
		got := last.Load().(seen)
		if got.method != method || got.requestURI != requestURI {
			t.Fatalf("%s request = %s %s, want %s %s", name, got.method, got.requestURI, method, requestURI)
		}
		if got.user != "pk-lf-datasets" || got.pass != "sk-lf-datasets" || got.sdkName != sdkName ||
			got.sdkVersion != "test" || got.publicKey != "pk-lf-datasets" || got.accept != "application/json" {
			t.Fatalf("%s identity headers = %+v", name, got)
		}
		wantType := ""
		if body != "" {
			wantType = "application/json"
		}
		if got.contentType != wantType || string(got.body) != body {
			t.Fatalf("%s body = %q (%q), want %q (%q)", name, got.body, got.contentType, body, wantType)
		}
	}

	dataset, err := client.UpsertDataset(ctx, []byte(`{"name":"team/set"}`), "team/set")
	if err != nil {
		t.Fatalf("UpsertDataset() error = %v", err)
	}
	check("upsert dataset", http.MethodPost, "/api/public/v2/datasets", `{"name":"team/set"}`)
	if dataset.ID != "ds-1" || dataset.Name != "team/set" || dataset.Description != "" ||
		string(dataset.Metadata) != `{"a":1}` || dataset.InputSchema != nil ||
		string(dataset.ExpectedOutputSchema) != `{"type":"object"}` ||
		!dataset.UpdatedAt.Equal(time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("UpsertDataset() = %+v", dataset)
	}

	for _, name := range []string{"team/set", "a%b", "what?", "hash#tag", "Ünïcode set", ".", "..", "a/../b"} {
		if _, err := client.GetDataset(ctx, name); err != nil {
			t.Fatalf("GetDataset(%q) error = %v", name, err)
		}
		escaped := strings.NewReplacer("/", "%2F", "%", "%25", "?", "%3F", "#", "%23", " ", "%20",
			"Ü", "%C3%9C", "ï", "%C3%AF").Replace(name)
		check("get dataset "+name, http.MethodGet, "/api/public/v2/datasets/"+escaped, "")
	}

	item, err := client.UpsertItem(ctx, []byte(`{"datasetName":"team/set","id":"item-1"}`), "team/set", "item-1")
	if err != nil {
		t.Fatalf("UpsertItem() error = %v", err)
	}
	check("upsert item", http.MethodPost, "/api/public/dataset-items", `{"datasetName":"team/set","id":"item-1"}`)
	if item.ID != "item-1" || item.DatasetID != "ds-1" || item.Status != "ACTIVE" ||
		string(item.Input) != `{"q":"x"}` || string(item.ExpectedOutput) != `"Paris"` || item.Metadata != nil {
		t.Fatalf("UpsertItem() = %+v", item)
	}

	if _, err := client.GetItem(ctx, "a/b?c#d"); err != nil {
		t.Fatalf("GetItem() error = %v", err)
	}
	check("get item", http.MethodGet, "/api/public/dataset-items/a%2Fb%3Fc%23d", "")

	if err := client.DeleteItem(ctx, "x y"); err != nil {
		t.Fatalf("DeleteItem() error = %v", err)
	}
	check("delete item", http.MethodDelete, "/api/public/dataset-items/x%20y", "")

	page, err := client.ListItems(ctx, DatasetItemListQuery{
		DatasetName: "team/set&x=1", SourceTraceID: "t", SourceObservationID: "o",
		Version: "2026-09-30T10:00:00.000Z", Page: 2, Limit: 8,
	})
	if err != nil {
		t.Fatalf("ListItems() error = %v", err)
	}
	check("list items", http.MethodGet, "/api/public/dataset-items?datasetName=team%2Fset%26x%3D1&limit=8&page=2"+
		"&sourceObservationId=o&sourceTraceId=t&version=2026-09-30T10%3A00%3A00.000Z", "")
	if page.Page != 2 || page.Limit != 8 || page.TotalItems != 9 || page.TotalPages != 2 ||
		len(page.Items) != 1 || page.Items[0].ID != "item-9" {
		t.Fatalf("ListItems() = %+v", page)
	}
}

// roundTripFunc lets a test fail a request before or after the request
// headers were written, which the client cannot otherwise distinguish.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestDatasetsWriteRetriesOnlyProvablyUnsentFailures(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	client := newDatasetsTestClient(t, "http://127.0.0.1:1")
	client.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, errors.New("dial refused")
	})
	_, err := client.UpsertItem(context.Background(), []byte(`{}`), "set", "")
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want the initial attempt plus two retries", got)
	}
	if failure := asDatasetError(t, err); failure.OutcomeUnknown {
		t.Fatalf("a failure before any header was written is not ambiguous: %v", err)
	}

	attempts.Store(0)
	client.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		attempts.Add(1)
		if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.WroteHeaders != nil {
			trace.WroteHeaders()
		}
		return nil, errors.New("connection reset")
	})
	_, err = client.UpsertItem(context.Background(), []byte(`{}`), "set", "item-1")
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want one: the write may have been applied", got)
	}
	if failure := asDatasetError(t, err); !failure.OutcomeUnknown {
		t.Fatalf("error = %v, want outcome unknown", err)
	}
	if strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("error %q leaks transport text", err)
	}
}

func TestDatasetsCommitThenDisconnectIsOutcomeUnknown(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		_, _ = io.ReadAll(r.Body)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL)
	for name, write := range map[string]func() error{
		"dataset": func() error {
			_, err := client.UpsertDataset(context.Background(), []byte(`{"name":"set"}`), "set")
			return err
		},
		"item": func() error {
			_, err := client.UpsertItem(context.Background(), []byte(`{}`), "set", "item-1")
			return err
		},
		"delete": func() error { return client.DeleteItem(context.Background(), "item-1") },
	} {
		attempts.Store(0)
		err := write()
		if got := attempts.Load(); got != 1 {
			t.Fatalf("%s attempts = %d, want 1", name, got)
		}
		if failure := asDatasetError(t, err); !failure.OutcomeUnknown {
			t.Fatalf("%s error = %v, want outcome unknown", name, err)
		}
	}
}

func TestDatasetsStatusClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status       int
		write        bool
		wantAttempts int32
		wantUnknown  bool
		wantNotFound bool
	}{
		{status: 500, write: false, wantAttempts: 3},
		{status: 429, write: false, wantAttempts: 3},
		{status: 408, write: false, wantAttempts: 3},
		{status: 404, write: false, wantAttempts: 1, wantNotFound: true},
		{status: 400, write: false, wantAttempts: 1},
		{status: 401, write: false, wantAttempts: 1},
		{status: 503, write: true, wantAttempts: 1, wantUnknown: true},
		{status: 429, write: true, wantAttempts: 1, wantUnknown: true},
		{status: 408, write: true, wantAttempts: 1, wantUnknown: true},
		{status: 422, write: true, wantAttempts: 1, wantUnknown: true},
		{status: 302, write: true, wantAttempts: 1, wantUnknown: true},
		{status: 404, write: true, wantAttempts: 1, wantNotFound: true},
		{status: 409, write: true, wantAttempts: 1},
		{status: 400, write: true, wantAttempts: 1},
		{status: 403, write: true, wantAttempts: 1},
		{status: 413, write: true, wantAttempts: 1},
	} {
		t.Run(fmt.Sprintf("%d-write-%t", test.status, test.write), func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.Header().Set("Location", "http://127.0.0.1:1/elsewhere")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, `{"message":"secret dataset name"}`)
			}))
			t.Cleanup(server.Close)
			client := newDatasetsTestClient(t, server.URL)
			var err error
			if test.write {
				_, err = client.UpsertItem(context.Background(), []byte(`{}`), "set", "item-1")
			} else {
				_, err = client.GetItem(context.Background(), "item-1")
			}
			failure := asDatasetError(t, err)
			if got := attempts.Load(); got != test.wantAttempts {
				t.Fatalf("attempts = %d, want %d", got, test.wantAttempts)
			}
			if failure.OutcomeUnknown != test.wantUnknown || failure.NotFound != test.wantNotFound ||
				failure.Status != test.status {
				t.Fatalf("error = %+v", failure)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "item-1") {
				t.Fatalf("error %q leaks response or request content", err)
			}
		})
	}
}

func TestDatasetsRedirectTargetReceivesNothing(t *testing.T) {
	t.Parallel()
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetHits.Add(1)
	}))
	t.Cleanup(target.Close)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/public/dataset-items", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(origin.Close)
	client := newDatasetsTestClient(t, origin.URL)
	if _, err := client.UpsertItem(context.Background(), []byte(`{}`), "set", "i"); err == nil {
		t.Fatal("UpsertItem() followed a redirect")
	}
	if _, err := client.GetDataset(context.Background(), "set"); err == nil {
		t.Fatal("GetDataset() followed a redirect")
	}
	if got := targetHits.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests", got)
	}
}

func TestDatasetsRejectsInvalidSuccessBodies(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"malformed":         `{"id":`,
		"trailing":          wireItem("item-1", "set") + ` {}`,
		"invalid UTF-8":     strings.Replace(wireItem("item-1", "set"), "Paris", "Par\xffis", 1),
		"wrong ID":          wireItem("item-2", "set"),
		"wrong dataset":     wireItem("item-1", "other"),
		"missing dates":     `{"id":"item-1","datasetId":"d","datasetName":"set","status":"ACTIVE"}`,
		"unknown status":    strings.Replace(wireItem("item-1", "set"), "ACTIVE", "DRAFT", 1),
		"array":             `[]`,
		"null":              `null`,
		"non-string ID":     strings.Replace(wireItem("item-1", "set"), `"id":"item-1"`, `"id":1`, 1),
		"bad nullable type": strings.Replace(wireItem("item-1", "set"), `"sourceTraceId":null`, `"sourceTraceId":7`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				_, _ = io.WriteString(w, body)
			}))
			t.Cleanup(server.Close)
			client := newDatasetsTestClient(t, server.URL)
			_, err := client.UpsertItem(context.Background(), []byte(`{}`), "set", "item-1")
			if failure := asDatasetError(t, err); !failure.OutcomeUnknown {
				t.Fatalf("write error = %v, want outcome unknown", err)
			}
			if got := attempts.Load(); got != 1 {
				t.Fatalf("attempts = %d; a processed write must never be retried", got)
			}
			if name == "wrong dataset" {
				return // a read by ID has no expected dataset
			}
			_, err = client.GetItem(context.Background(), "item-1")
			if failure := asDatasetError(t, err); failure.OutcomeUnknown || failure.NotFound {
				t.Fatalf("read error = %+v, want a plain decoding failure", failure)
			}
		})
	}
}

func TestDatasetsRejectsOversizedBodies(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, wireItem("item-1", "set"))
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL)
	client.responseLimit = 64
	client.pageResponseLimit = 64
	_, err := client.UpsertItem(context.Background(), []byte(`{}`), "set", "item-1")
	if failure := asDatasetError(t, err); !failure.OutcomeUnknown || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("oversized write response error = %v", err)
	}
	if _, err := client.ListItems(context.Background(), DatasetItemListQuery{DatasetName: "set", Page: 1, Limit: 1}); err == nil {
		t.Fatal("ListItems() accepted an oversized page")
	}
}

func TestDatasetsDeclinesRetryBeyondDeadline(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.Header().Set("Retry-After", "86400")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	_, err := client.GetDataset(ctx, "set")
	if err == nil || attempts.Load() != 1 || time.Since(started) > 2*time.Second {
		t.Fatalf("GetDataset() = %v after %d attempts in %v; want an immediate declined retry",
			err, attempts.Load(), time.Since(started))
	}
}

func TestDatasetsCancellationWrapsContextErrors(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	client := newDatasetsTestClient(t, server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := client.UpsertItem(ctx, []byte(`{}`), "set", "item-1")
	if !errors.Is(err, context.Canceled) || !asDatasetError(t, err).OutcomeUnknown {
		t.Fatalf("cancel during a write's body read = %v, want context.Canceled and outcome unknown", err)
	}

	pre, preCancel := context.WithCancel(context.Background())
	preCancel()
	_, err = client.UpsertItem(pre, []byte(`{}`), "set", "item-1")
	if !errors.Is(err, context.Canceled) || asDatasetError(t, err).OutcomeUnknown {
		t.Fatalf("pre-canceled write = %v, want context.Canceled without outcome unknown", err)
	}

	deadline, deadlineCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer deadlineCancel()
	_, err = client.GetItem(deadline, "item-1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline during a read = %v, want context.DeadlineExceeded", err)
	}
}

func TestDatasetsCancellationDuringBackoff(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := client.GetItem(ctx, "item-1")
	if !errors.Is(err, context.Canceled) || attempts.Load() != 1 {
		t.Fatalf("cancel during backoff = %v after %d attempts; want the cancellation", err, attempts.Load())
	}
}

func TestDatasetsWriteRetriesReplayIdenticalBytes(t *testing.T) {
	t.Parallel()
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		_, _ = io.WriteString(w, wireItem("item-1", "set"))
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL)
	var failures atomic.Int32
	base := http.DefaultTransport
	client.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		// Two connection failures before anything was written, then the
		// real transport.
		if failures.Add(1) <= 2 {
			return nil, errors.New("dial refused")
		}
		return base.RoundTrip(request)
	})
	payload := []byte(`{"datasetName":"set","id":"item-1","input":"x"}`)
	if _, err := client.UpsertItem(context.Background(), payload, "set", "item-1"); err != nil {
		t.Fatalf("UpsertItem() error = %v", err)
	}
	if len(bodies) != 1 || bodies[0] != string(payload) || failures.Load() != 3 {
		t.Fatalf("bodies = %q after %d attempts, want the exact payload once after two unsent attempts",
			bodies, failures.Load())
	}
}

func TestDatasetsValidatedSuccessSurvivesLateCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, wireItem("item-1", "set"))
	}))
	t.Cleanup(server.Close)
	client := newDatasetsTestClient(t, server.URL)
	base := http.DefaultTransport
	client.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(request)
		if err == nil {
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(string(body)))
		}
		cancel() // canceled after the complete response arrived
		return response, err
	})
	if _, err := client.UpsertItem(ctx, []byte(`{}`), "set", "item-1"); err != nil {
		t.Fatalf("a validated success reported %v after a late cancellation", err)
	}
}

func TestNormalizeAPIBase(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"":                               "https://cloud.langfuse.com/api/public",
		"http://localhost:3000/":         "http://localhost:3000/api/public",
		"https://x.test/api/public/otel": "https://x.test/api/public",
		"https://x.test/api/public/otel/v1/traces": "https://x.test/api/public",
	} {
		got, err := NormalizeAPIBase(input)
		if err != nil || got != want {
			t.Fatalf("NormalizeAPIBase(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	if _, err := NormalizeAPIBase("ftp://x.test"); err == nil {
		t.Fatal("NormalizeAPIBase() accepted an unsupported scheme")
	}
}
