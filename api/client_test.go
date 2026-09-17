package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testPublicKey = "pk-lf-fixture"
	testSecretKey = "sk-lf-fixture-sensitive"
)

func testClient(t *testing.T, server *httptest.Server, modify func(*Config)) *Client {
	t.Helper()
	config := Config{BaseURL: server.URL, PublicKey: testPublicKey, SecretKey: testSecretKey, MaxRetryDelay: time.Millisecond}
	if modify != nil {
		modify(&config)
	}
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func promptFixture(name string, version int) string {
	data, err := json.Marshal(map[string]any{"name": name, "version": version, "type": "text", "prompt": "Hi {{name}}", "config": map[string]any{}, "labels": []string{"staging"}, "tags": []string{}})
	if err != nil {
		panic(err) // All fixture values are JSON-compatible.
	}
	return string(data)
}

func TestPinnedContractHash(t *testing.T) {
	data, err := os.ReadFile("testdata/langfuse-openapi-2026-09-17.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != "b235737d48b117621f9d607b2beb48b135851fac7bff2aa72b50c6cc8796e9df" {
		t.Fatalf("contract changed: %s", got)
	}
}

func TestClientEscapingAuthenticationAndOwnership(t *testing.T) {
	const name = "folder/sub a?#&"
	var redirectPolicyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/prefix/api/public/v2/prompts/"+url.PathEscape(name) {
			t.Errorf("escaped path = %q", r.URL.EscapedPath())
		}
		if r.URL.Query().Get("version") != "2" || r.URL.Query().Get("resolve") != "false" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		public, secret, ok := r.BasicAuth()
		if !ok || public != testPublicKey || secret != testSecretKey {
			t.Error("missing expected Basic auth")
		}
		if r.Header.Get("Cookie") != "" {
			t.Error("management client inherited caller cookies")
		}
		_, _ = io.WriteString(w, promptFixture(name, 2))
	}))
	defer server.Close()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	jar.SetCookies(serverURL, []*http.Cookie{{Name: "private", Value: "cookie"}})
	original := &http.Client{Jar: jar, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { redirectPolicyCalls.Add(1); return nil }}
	client := testClient(t, server, func(c *Config) { c.BaseURL += "/prefix/"; c.HTTPClient = original })
	resolve := false
	prompt, err := client.Prompts.Get(t.Context(), name, GetPromptOptions{Version: 2, Resolve: &resolve})
	if err != nil || prompt.Version != 2 {
		t.Fatalf("get: %+v, %v", prompt, err)
	}
	if original.Jar != jar || original.CheckRedirect == nil || redirectPolicyCalls.Load() != 0 {
		t.Fatal("caller HTTP client was modified")
	}
}

func TestClientRetriesOnlyReadsAndBoundsRetryAfter(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				n := calls.Add(1)
				if n < 3 {
					w.Header().Set("Retry-After", "3600")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, testSecretKey)
					return
				}
				_, _ = io.WriteString(w, `{}`)
			}))
			defer server.Close()
			client := testClient(t, server, nil)
			var output map[string]any
			err := client.do(t.Context(), "fixture", method, "/fixture", nil, map[string]any{"value": 1}, &output)
			if method == http.MethodGet {
				if err != nil || calls.Load() != 3 {
					t.Fatalf("read calls=%d err=%v", calls.Load(), err)
				}
			} else {
				var responseErr *ResponseError
				if !errors.As(err, &responseErr) || responseErr.StatusCode != http.StatusServiceUnavailable || calls.Load() != 1 {
					t.Fatalf("write calls=%d err=%v", calls.Load(), err)
				}
				if responseErr.RetryAfter > time.Millisecond {
					t.Fatal("Retry-After was not capped")
				}
			}
		})
	}
}

func TestClientDoesNotRetryPermanentErrorsOrInvalidSuccess(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusNotImplemented, http.StatusOK} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `<html>not JSON</html>`)
			}))
			defer server.Close()
			client := testClient(t, server, nil)
			var output map[string]any
			if err := client.do(t.Context(), "fixture", http.MethodGet, "/fixture", nil, nil, &output); err == nil {
				t.Fatal("accepted failed or invalid response")
			}
			if calls.Load() != 1 {
				t.Fatalf("calls=%d", calls.Load())
			}
		})
	}
}

func TestClientRedirectsNeverReachDestination(t *testing.T) {
	var destinationCalls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { destinationCalls.Add(1); _, _ = io.WriteString(w, `{}`) }))
	defer destination.Close()
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(fmt.Sprintf("%s-%d", method, status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL+"/private", status) }))
				defer server.Close()
				client := testClient(t, server, nil)
				err := client.do(t.Context(), "fixture", method, "/fixture", nil, map[string]string{"secret": "private-body"}, nil)
				var responseErr *ResponseError
				if !errors.As(err, &responseErr) || responseErr.StatusCode != status {
					t.Fatalf("redirect result=%v", err)
				}
			})
		}
	}
	if destinationCalls.Load() != 0 {
		t.Fatalf("followed %d redirects", destinationCalls.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countedBody struct {
	remaining, read int
	closed          bool
}

func (b *countedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), b.remaining)
	for i := range n {
		p[i] = 'x'
	}
	b.remaining -= n
	b.read += n
	return n, nil
}
func (b *countedBody) Close() error { b.closed = true; return nil }

func TestClientBodyBoundsClosureAndNoReplayBody(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			body := &countedBody{remaining: 1 << 20}
			client, err := NewClient(Config{PublicKey: testPublicKey, SecretKey: testSecretKey, MaxResponseBytes: 32, MaxErrorBytes: 16, HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.GetBody != nil {
					t.Error("write body is replayable")
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: body, Request: r}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			var output map[string]any
			err = client.do(t.Context(), "fixture", http.MethodPost, "/fixture", nil, map[string]any{"v": 1}, &output)
			if !body.closed {
				t.Fatal("response body not closed")
			}
			if status == http.StatusOK {
				if !errors.Is(err, ErrResponseTooLarge) || body.read != 33 {
					t.Fatalf("read=%d err=%v", body.read, err)
				}
			} else {
				var responseErr *ResponseError
				if !errors.As(err, &responseErr) || !responseErr.BodyTruncated || body.read != 17 {
					t.Fatalf("read=%d err=%v", body.read, err)
				}
			}
		})
	}
}

func TestClientRejectsEmptyNullAndTrailingSuccess(t *testing.T) {
	for _, body := range []string{"", "null", "{} {}", "{} trailing", "[]", "{\"unterminated\":"} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
			defer server.Close()
			client := testClient(t, server, nil)
			var output map[string]any
			err := client.do(t.Context(), "fixture", http.MethodGet, "/fixture", nil, nil, &output)
			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("result=%v", err)
			}
		})
	}
}

func TestClientTotalTimeoutIncludesResponseBodyAndBackoff(t *testing.T) {
	for _, stall := range []string{"body", "retry"} {
		t.Run(stall, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if stall == "retry" {
					w.Header().Set("Retry-After", "120")
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer server.Close()
			client := testClient(t, server, func(c *Config) { c.Timeout = 30 * time.Millisecond; c.MaxRetryDelay = time.Second })
			var output map[string]any
			start := time.Now()
			err := client.do(t.Context(), "fixture", http.MethodGet, "/fixture", nil, nil, &output)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("timeout=%v", err)
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("timeout exceeded its bounded allowance")
			}
		})
	}
}

func TestClientErrorsDoNotFormatSensitiveContext(t *testing.T) {
	client, err := NewClient(Config{PublicKey: testPublicKey, SecretKey: testSecretKey, MaxAttempts: 1, HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("%s %s %s", testSecretKey, r.URL, r.Header.Get("Authorization"))
	})}})
	if err != nil {
		t.Fatal(err)
	}
	err = client.do(t.Context(), "fixture", http.MethodGet, "/sensitive-name", url.Values{"private": {"filter-value"}}, nil, nil)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || requestErr.Unwrap() == nil {
		t.Fatalf("missing typed wrapped error: %v", err)
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%q", "%s"} {
		formatted := fmt.Sprintf(format, err)
		for _, sensitive := range []string{testSecretKey, testPublicKey, "sensitive-name", "filter-value", "Basic", "https://"} {
			if strings.Contains(formatted, sensitive) {
				t.Fatalf("format %s leaked sensitive context", format)
			}
		}
	}
}

func TestClientValidationAndCanceledNoRequest(t *testing.T) {
	for _, base := range []string{"/relative", "ftp://host", "https://user:pass@host", "https://host?token=x", "https://host/#fragment", "https://host/a/../b"} {
		if _, err := NewClient(Config{BaseURL: base, PublicKey: testPublicKey, SecretKey: testSecretKey}); err == nil {
			t.Errorf("accepted base URL %q", base)
		}
	}
	for _, config := range []Config{{}, {PublicKey: "a:b", SecretKey: "secret"}, {PublicKey: testPublicKey, SecretKey: testSecretKey, Timeout: -time.Second}, {PublicKey: testPublicKey, SecretKey: testSecretKey, MaxResponseBytes: -1}, {PublicKey: testPublicKey, SecretKey: testSecretKey, MaxAttempts: 99}} {
		if _, err := NewClient(config); err == nil {
			t.Error("accepted invalid config")
		}
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); _, _ = io.WriteString(w, `{}`) }))
	defer server.Close()
	client := testClient(t, server, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.do(ctx, "fixture", http.MethodGet, "/fixture", nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("canceled request reached the server")
	}
	var uninitialized *PromptsService
	if _, err := uninitialized.Get(t.Context(), "p", GetPromptOptions{}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("nil service=%v", err)
	}
	var zero Client
	if err := zero.do(t.Context(), "fixture", http.MethodGet, "/fixture", nil, nil, nil); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("zero client=%v", err)
	}
}

func TestClientDynamicJSONNumbersAreLossless(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"n":9007199254740993,"nested":{"n":0.1234567890123456789}}`)
	}))
	defer server.Close()
	client := testClient(t, server, nil)
	var output map[string]any
	if err := client.do(t.Context(), "fixture", http.MethodGet, "/fixture", nil, nil, &output); err != nil {
		t.Fatal(err)
	}
	if output["n"] != json.Number("9007199254740993") {
		t.Fatalf("rounded number=%v", output["n"])
	}
	if output["nested"].(map[string]any)["n"] != json.Number("0.1234567890123456789") {
		t.Fatal("rounded nested number")
	}
}
