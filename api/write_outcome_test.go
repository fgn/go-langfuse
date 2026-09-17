package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestPromptWriteOutcomeUnknownAndNoReplay(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("X-Request-Id", "fixture-123")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"malformed":`)
			}))
			defer server.Close()
			_, err := testClient(t, server, nil).Prompts.Create(t.Context(), CreatePromptRequest{Name: "p", Prompt: TextContent("x")})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("write was accepted or replayed: calls=%d err=%v", calls.Load(), err)
			}
			if status == http.StatusOK {
				var failure *RequestError
				if !errors.As(err, &failure) || !failure.OutcomeUnknown || !errors.Is(err, ErrInvalidResponse) {
					t.Fatalf("lost ambiguous successful-write response: %v", err)
				}
				return
			}
			var failure *ResponseError
			if !errors.As(err, &failure) || failure.OutcomeUnknown != (status >= 500) || failure.Retryable || failure.RequestID != "fixture-123" {
				t.Fatalf("incorrect write failure classification: %v", err)
			}
		})
	}
}

func TestPromptPreCanceledWriteHasKnownOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("pre-canceled write reached the server")
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := testClient(t, server, nil).Prompts.Create(ctx, CreatePromptRequest{Name: "p", Prompt: TextContent("x")})
	var failure *RequestError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &failure) || failure.OutcomeUnknown {
		t.Fatalf("incorrect pre-send cancellation: %v", err)
	}
}

func TestRequestIDValidation(t *testing.T) {
	for _, value := range []string{"line\nbreak", "contains spaces", "non-ascii-é"} {
		if safeRequestID(value) != "" {
			t.Fatal("retained unsafe request identifier")
		}
	}
}
