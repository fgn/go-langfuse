// Package otlpreceiver provides a local OTLP/HTTP protobuf receiver for tests.
package otlpreceiver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Request is one decoded OTLP export request and its HTTP envelope.
type Request struct {
	Method string
	Path   string
	Header http.Header
	Export *collectortrace.ExportTraceServiceRequest
}

// Receiver is an in-memory HTTP test server.
type Receiver struct {
	Server *httptest.Server

	mu       sync.Mutex
	requests []Request
	notify   chan struct{}
	stall    chan struct{}
}

// New starts a receiver. Call Close when finished.
func New() *Receiver {
	r := &Receiver{notify: make(chan struct{}, 1024)}
	r.Server = httptest.NewServer(http.HandlerFunc(r.handle))
	return r
}

// Close stops the HTTP server.
func (r *Receiver) Close() { r.Server.Close() }

// URL returns the server host root.
func (r *Receiver) URL() string { return r.Server.URL }

// Requests returns a defensive snapshot of received requests.
func (r *Receiver) Requests() []Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Request, len(r.requests))
	copy(result, r.requests)
	return result
}

// Notify is signaled after each successfully decoded request.
func (r *Receiver) Notify() <-chan struct{} { return r.notify }

// Stall holds every subsequent request open until Release is called. Callers
// must call Release before Close or Close blocks on the held requests.
func (r *Receiver) Stall() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stall == nil {
		r.stall = make(chan struct{})
	}
}

// Release unblocks all currently held and future requests. It is safe to call
// without a prior Stall and safe to call more than once.
func (r *Receiver) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stall != nil {
		close(r.stall)
		r.stall = nil
	}
}

func (r *Receiver) handle(w http.ResponseWriter, request *http.Request) {
	r.mu.Lock()
	stall := r.stall
	r.mu.Unlock()
	if stall != nil {
		select {
		case <-stall:
		case <-request.Context().Done():
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 16<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	decoded := new(collectortrace.ExportTraceServiceRequest)
	if err := proto.Unmarshal(body, decoded); err != nil {
		http.Error(w, "decode protobuf", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.requests = append(r.requests, Request{
		Method: request.Method,
		Path:   request.URL.Path,
		Header: request.Header.Clone(),
		Export: decoded,
	})
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}

	response, _ := proto.Marshal(new(collectortrace.ExportTraceServiceResponse))
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

// Spans flattens all resource and instrumentation-scope groups in request.
func Spans(request Request) []*tracepb.Span {
	var result []*tracepb.Span
	for _, resourceSpans := range request.Export.GetResourceSpans() {
		for _, scopeSpans := range resourceSpans.GetScopeSpans() {
			result = append(result, scopeSpans.GetSpans()...)
		}
	}
	return result
}
