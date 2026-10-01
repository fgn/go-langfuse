package langfuse_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

const datasetTimes = `"createdAt":"2026-09-30T10:00:00.000Z","updatedAt":"2026-09-30T11:00:00.000Z"`

func datasetJSON(name string) string {
	return fmt.Sprintf(`{"id":"ds-1","name":%q,"description":null,"metadata":{"a":1},`+
		`"inputSchema":null,"expectedOutputSchema":{"type":"object"},%s}`, name, datasetTimes)
}

func datasetItemJSON(id, datasetName string) string {
	return fmt.Sprintf(`{"id":%q,"datasetId":"ds-1","datasetName":%q,"status":"ACTIVE","input":{"q":1},`+
		`"expectedOutput":"Paris","metadata":{"m":12345678901234567890},"sourceTraceId":null,`+
		`"sourceObservationId":null,%s,"mediaReferences":[]}`, id, datasetName, datasetTimes)
}

func itemsPage(totalPages int, ids ...string) string {
	items := make([]string, len(ids))
	for index, id := range ids {
		items[index] = datasetItemJSON(id, "set")
	}
	return fmt.Sprintf(`{"data":[%s],"meta":{"totalPages":%d}}`, strings.Join(items, ","), totalPages)
}

type datasetServer struct {
	*httptest.Server
	requests atomic.Int32
}

func newDatasetServer(t *testing.T, handle func(w http.ResponseWriter, r *http.Request, body []byte)) *datasetServer {
	t.Helper()
	server := &datasetServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		handle(w, r, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func serveDataset(w http.ResponseWriter, r *http.Request, _ []byte) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/public/v2/datasets"):
		_, _ = io.WriteString(w, datasetJSON("set"))
	case r.Method == http.MethodGet && r.URL.Path == "/api/public/dataset-items":
		_, _ = io.WriteString(w, itemsPage(1, "item-1"))
	default:
		_, _ = io.WriteString(w, datasetItemJSON("item-1", "set"))
	}
}

func newDatasetClient(t *testing.T, baseURL string, change func(*langfuse.Config)) *langfuse.Client {
	t.Helper()
	config := langfuse.Config{BaseURL: baseURL, PublicKey: "pk-lf-dataset", SecretKey: "sk-lf-dataset"}
	if change != nil {
		change(&config)
	}
	client, err := langfuse.New(context.Background(), config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	return client
}

func collectDatasetItems(ctx context.Context, client *langfuse.Client, query langfuse.DatasetItemQuery) ([]string, error) {
	var ids []string
	var final error
	for item, err := range client.DatasetItems(ctx, query) {
		if final != nil {
			return ids, errors.New("DatasetItems yielded after its terminal error")
		}
		if err != nil {
			final = err
			continue
		}
		ids = append(ids, item.ID)
	}
	return ids, final
}

func errOf[T any](_ T, err error) error { return err }

var datasetCalls = map[string]func(context.Context, *langfuse.Client) error{
	"UpsertDataset": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.UpsertDataset(ctx, langfuse.DatasetSpec{Name: "set", Metadata: map[string]any{"a": 1}}))
	},
	"GetDataset": func(ctx context.Context, c *langfuse.Client) error { return errOf(c.GetDataset(ctx, "set")) },
	"UpsertDatasetItem": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(c.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set", ID: "item-1", Input: "x"}))
	},
	"GetDatasetItem":    func(ctx context.Context, c *langfuse.Client) error { return errOf(c.GetDatasetItem(ctx, "item-1")) },
	"DeleteDatasetItem": func(ctx context.Context, c *langfuse.Client) error { return c.DeleteDatasetItem(ctx, "item-1") },
	"DatasetItems": func(ctx context.Context, c *langfuse.Client) error {
		return errOf(collectDatasetItems(ctx, c, langfuse.DatasetItemQuery{DatasetName: "set"}))
	},
}

func sameJSON(t *testing.T, got, want string) bool {
	t.Helper()
	decode := func(text string) any {
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decode %q: %v", text, err)
		}
		return value
	}
	return reflect.DeepEqual(decode(got), decode(want))
}

func TestDatasetRequestShape(t *testing.T) {
	t.Parallel()
	type seen struct {
		method, uri, body string
		header            http.Header
	}
	var last atomic.Pointer[seen]
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		last.Store(&seen{method: r.Method, uri: r.RequestURI, body: string(body), header: r.Header.Clone()})
		if name, ok := strings.CutPrefix(r.URL.Path, "/api/public/v2/datasets/"); ok {
			_, _ = io.WriteString(w, datasetJSON(name))
		} else if id, ok := strings.CutPrefix(r.URL.Path, "/api/public/dataset-items/"); ok {
			_, _ = io.WriteString(w, datasetItemJSON(id, "set"))
		} else {
			serveDataset(w, r, body)
		}
	})
	client := newDatasetClient(t, server.URL+"/api/public/otel/v1/traces", nil)
	ctx := context.Background()
	check := func(method, uri, body string) {
		t.Helper()
		got := last.Load()
		if got.method != method || got.uri != uri {
			t.Fatalf("request = %s %s, want %s %s", got.method, got.uri, method, uri)
		}
		if (body == "") != (got.body == "") || body != "" && !sameJSON(t, got.body, body) {
			t.Fatalf("%s body = %s, want %s", uri, got.body, body)
		}
		contentType := ""
		if body != "" {
			contentType = "application/json"
		}
		for header, want := range map[string]string{
			"Authorization":          "Basic " + base64.StdEncoding.EncodeToString([]byte("pk-lf-dataset:sk-lf-dataset")),
			"Content-Type":           contentType,
			"Accept":                 "application/json",
			"x-langfuse-sdk-name":    "go",
			"x-langfuse-sdk-version": langfuse.SDKVersion,
			"x-langfuse-public-key":  "pk-lf-dataset",
		} {
			if value := got.header.Get(header); value != want {
				t.Errorf("%s %s = %q, want %q", uri, header, value, want)
			}
		}
	}

	dataset, err := client.UpsertDataset(ctx, langfuse.DatasetSpec{Name: "set"})
	if err != nil {
		t.Fatalf("UpsertDataset() error = %v", err)
	}
	check(http.MethodPost, "/api/public/v2/datasets", `{"name":"set"}`)
	if dataset.ID != "ds-1" || dataset.Name != "set" || dataset.Description != "" ||
		string(dataset.Metadata) != `{"a":1}` || dataset.InputSchema != nil ||
		string(dataset.ExpectedOutputSchema) != `{"type":"object"}` ||
		!dataset.UpdatedAt.Equal(time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("UpsertDataset() = %+v", dataset)
	}

	for name, escaped := range map[string]string{
		"team/set": "team%2Fset", "a%b": "a%25b", "what?": "what%3F", "hash#tag": "hash%23tag",
		"Ünïcode set": "%C3%9Cn%C3%AFcode%20set", ".": ".", "..": "..", "a/../b": "a%2F..%2Fb",
	} {
		if _, err := client.GetDataset(ctx, name); err != nil {
			t.Fatalf("GetDataset(%q) error = %v", name, err)
		}
		check(http.MethodGet, "/api/public/v2/datasets/"+escaped, "")
	}

	item, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set", ID: "item-1"})
	if err != nil {
		t.Fatalf("UpsertDatasetItem() error = %v", err)
	}
	check(http.MethodPost, "/api/public/dataset-items", `{"datasetName":"set","id":"item-1"}`)
	if item.ID != "item-1" || item.DatasetID != "ds-1" || item.Status != langfuse.DatasetItemActive ||
		string(item.Input) != `{"q":1}` || string(item.ExpectedOutput) != `"Paris"` ||
		string(item.Metadata) != `{"m":12345678901234567890}` || item.SourceTraceID != "" {
		t.Fatalf("UpsertDatasetItem() = %+v", item)
	}

	if _, err := client.GetDatasetItem(ctx, "a/b?c#d"); err != nil {
		t.Fatalf("GetDatasetItem() error = %v", err)
	}
	check(http.MethodGet, "/api/public/dataset-items/a%2Fb%3Fc%23d", "")

	if err := client.DeleteDatasetItem(ctx, "x y"); err != nil {
		t.Fatalf("DeleteDatasetItem() error = %v", err)
	}
	check(http.MethodDelete, "/api/public/dataset-items/x%20y", "")
}

func TestDatasetWritesSendMaskedFields(t *testing.T) {
	t.Parallel()
	var body atomic.Value
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, request []byte) {
		body.Store(string(request))
		serveDataset(w, r, request)
	})
	var calls sync.Map
	client := newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.DisableContentCapture = true // does not apply to dataset writes
		config.Mask = func(field langfuse.MaskField, value any) any {
			count, _ := calls.LoadOrStore(field, new(atomic.Int32))
			count.(*atomic.Int32).Add(1)
			if field == langfuse.MaskDatasetMetadata || field == langfuse.MaskDatasetItemInput {
				return map[string]any{"k": "[redacted]"}
			}
			return value
		}
	})
	ctx := context.Background()
	dataset := func(spec langfuse.DatasetSpec) func() error {
		return func() error { return errOf(client.UpsertDataset(ctx, spec)) }
	}
	description, empty := "d", ""
	for _, test := range []struct {
		write func() error
		want  string
	}{
		{
			write: dataset(langfuse.DatasetSpec{
				Name: "set", Description: &description, Metadata: map[string]any{"k": "secret"},
				InputSchema: json.RawMessage(`{"type": "object"}`), ExpectedOutputSchema: json.RawMessage(`null`),
			}),
			want: `{"name":"set","description":"d","metadata":{"k":"[redacted]"},` +
				`"inputSchema":{"type":"object"},"expectedOutputSchema":null}`,
		},
		{write: dataset(langfuse.DatasetSpec{Name: "set", Description: &empty}), want: `{"name":"set","description":""}`},
		{
			write: func() error {
				return errOf(client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{
					DatasetName: "set", ID: "item-1", Input: map[string]any{"k": "secret"},
					ExpectedOutput: json.RawMessage(`12345678901234567890`), Metadata: map[string]any{},
					SourceTraceID: "trace", SourceObservationID: "span", Status: langfuse.DatasetItemArchived,
				}))
			},
			want: `{"datasetName":"set","id":"item-1","input":{"k":"[redacted]"},` +
				`"expectedOutput":12345678901234567890,"metadata":{},"sourceTraceId":"trace",` +
				`"sourceObservationId":"span","status":"ARCHIVED"}`,
		},
	} {
		if err := test.write(); err != nil {
			t.Fatalf("write error = %v", err)
		}
		if got := body.Load().(string); !sameJSON(t, got, test.want) {
			t.Fatalf("request body = %s\nwant %s", got, test.want)
		}
	}
	for _, field := range []langfuse.MaskField{
		langfuse.MaskDatasetMetadata, langfuse.MaskDatasetItemInput,
		langfuse.MaskDatasetItemExpectedOutput, langfuse.MaskDatasetItemMetadata,
	} {
		if count, ok := calls.Load(field); !ok || count.(*atomic.Int32).Load() != 1 {
			t.Errorf("Mask(%q) was not called exactly once", field)
		}
	}
}

type nullMarshaler struct{}

func (nullMarshaler) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

func TestDatasetInvalidInputSendsNothing(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, serveDataset)
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	ctx := context.Background()
	item := func(spec langfuse.DatasetItemSpec) func(*langfuse.Client) error {
		return func(c *langfuse.Client) error { return errOf(c.UpsertDatasetItem(ctx, spec)) }
	}
	dataset := func(spec langfuse.DatasetSpec) func(*langfuse.Client) error {
		return func(c *langfuse.Client) error { return errOf(c.UpsertDataset(ctx, spec)) }
	}
	for name, test := range map[string]struct {
		mask func(langfuse.MaskField, any) any
		call func(*langfuse.Client) error
	}{
		"mask returns nil": {
			mask: func(langfuse.MaskField, any) any { return nil },
			call: item(langfuse.DatasetItemSpec{DatasetName: "set", ExpectedOutput: "secret"}),
		},
		"mask panics": {
			mask: func(langfuse.MaskField, any) any { panic("PANIC-PAYLOAD") },
			call: item(langfuse.DatasetItemSpec{DatasetName: "set", Input: "secret"}),
		},
		"mask changes metadata type": {
			mask: func(_ langfuse.MaskField, value any) any { return fmt.Sprint(value) },
			call: dataset(langfuse.DatasetSpec{Name: "set", Metadata: map[string]any{"a": "secret"}}),
		},
		"raw null":         {call: item(langfuse.DatasetItemSpec{DatasetName: "set", Input: json.RawMessage(`null`)})},
		"null marshaler":   {call: item(langfuse.DatasetItemSpec{DatasetName: "set", ExpectedOutput: nullMarshaler{}})},
		"invalid raw":      {call: item(langfuse.DatasetItemSpec{DatasetName: "set", Input: json.RawMessage(`{`)})},
		"cycle":            {call: item(langfuse.DatasetItemSpec{DatasetName: "set", Metadata: cyclic})},
		"oversized field":  {call: item(langfuse.DatasetItemSpec{DatasetName: "set", Input: strings.Repeat("x", 1<<20)})},
		"oversized body":   {call: item(langfuse.DatasetItemSpec{DatasetName: strings.Repeat("n", 4<<20)})},
		"unsupported":      {call: item(langfuse.DatasetItemSpec{DatasetName: "set", Input: func() {}})},
		"invalid UTF-8 ID": {call: item(langfuse.DatasetItemSpec{DatasetName: "set", ID: "\xff"})},
		"invalid UTF-8 schema": {
			call: dataset(langfuse.DatasetSpec{Name: "set", InputSchema: json.RawMessage("{\"a\":\"\xff\"}")}),
		},
		"invalid schema":       {call: dataset(langfuse.DatasetSpec{Name: "set", ExpectedOutputSchema: json.RawMessage(`{`)})},
		"missing dataset name": {call: dataset(langfuse.DatasetSpec{Metadata: map[string]any{"a": "secret"}})},
		"missing item dataset": {call: item(langfuse.DatasetItemSpec{Input: "secret"})},
		"missing read name":    {call: func(c *langfuse.Client) error { return errOf(c.GetDataset(ctx, "")) }},
		"missing read ID":      {call: func(c *langfuse.Client) error { return errOf(c.GetDatasetItem(ctx, "")) }},
		"missing delete ID":    {call: func(c *langfuse.Client) error { return c.DeleteDatasetItem(ctx, "") }},
		"missing query dataset": {call: func(c *langfuse.Client) error {
			return errOf(collectDatasetItems(ctx, c, langfuse.DatasetItemQuery{}))
		}},
		"nil context": {call: func(c *langfuse.Client) error {
			return errOf(c.GetDataset(nil, "set")) //nolint:staticcheck // A nil context is the input under test.
		}},
	} {
		t.Run(name, func(t *testing.T) {
			client := newDatasetClient(t, server.URL, func(config *langfuse.Config) { config.Mask = test.mask })
			before := server.requests.Load()
			err := test.call(client)
			if err == nil {
				t.Fatal("invalid input was accepted")
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "PANIC-PAYLOAD") {
				t.Fatalf("error %q leaks content", err)
			}
			if got := server.requests.Load() - before; got != 0 {
				t.Fatalf("invalid input sent %d requests", got)
			}
		})
	}
}

func TestDatasetResponseOutcomes(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		call       string
		status     int // of the first response; 0 drops the connection
		retryAfter string
		requests   int32
		fails      bool
		sentinel   error
	}{
		"read retries 500":         {call: "GetDatasetItem", status: 500, requests: 2},
		"read retries 429":         {call: "GetDataset", status: 429, requests: 2},
		"read retries 408":         {call: "DatasetItems", status: 408, requests: 2},
		"read retries a drop":      {call: "GetDatasetItem", status: 0, requests: 2},
		"read declines long waits": {call: "GetDataset", status: 503, retryAfter: "86400", requests: 1, fails: true},
		"read 400":                 {call: "GetDatasetItem", status: 400, requests: 1, fails: true},
		"read 401":                 {call: "GetDataset", status: 401, requests: 1, fails: true},
		"read redirect":            {call: "GetDataset", status: 307, requests: 1, fails: true},
		"dataset read 404":         {call: "GetDataset", status: 404, requests: 1, fails: true, sentinel: langfuse.ErrDatasetNotFound},
		"item upsert 404":          {call: "UpsertDatasetItem", status: 404, requests: 1, fails: true, sentinel: langfuse.ErrDatasetNotFound},
		"item read 404":            {call: "GetDatasetItem", status: 404, requests: 1, fails: true, sentinel: langfuse.ErrDatasetItemNotFound},
		"item delete 404":          {call: "DeleteDatasetItem", status: 404, requests: 1, fails: true, sentinel: langfuse.ErrDatasetItemNotFound},
		"listing 404":              {call: "DatasetItems", status: 404, requests: 1, fails: true, sentinel: langfuse.ErrDatasetNotFound},
		"dataset upsert 404":       {call: "UpsertDataset", status: 404, requests: 1, fails: true},
		"write 400":                {call: "UpsertDatasetItem", status: 400, requests: 1, fails: true},
		"write 403":                {call: "UpsertDatasetItem", status: 403, requests: 1, fails: true},
		"write 409":                {call: "UpsertDatasetItem", status: 409, requests: 1, fails: true},
		"write 413":                {call: "UpsertDataset", status: 413, requests: 1, fails: true},
		"write 503":                {call: "UpsertDatasetItem", status: 503, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"write 429":                {call: "UpsertDatasetItem", status: 429, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"write 408":                {call: "UpsertDatasetItem", status: 408, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"write 422":                {call: "UpsertDatasetItem", status: 422, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"write 502":                {call: "DeleteDatasetItem", status: 502, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"write redirect":           {call: "UpsertDataset", status: 302, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"dataset upsert drop":      {call: "UpsertDataset", status: 0, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"item upsert drop":         {call: "UpsertDatasetItem", status: 0, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
		"item delete drop":         {call: "DeleteDatasetItem", status: 0, requests: 1, fails: true, sentinel: langfuse.ErrWriteOutcomeUnknown},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var redirected atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
			t.Cleanup(target.Close)
			var answered atomic.Bool
			server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				if answered.Swap(true) {
					serveDataset(w, r, body)
					return
				}
				if test.status == 0 {
					if connection, _, err := w.(http.Hijacker).Hijack(); err == nil {
						_ = connection.Close()
					}
					return
				}
				w.Header().Set("Location", target.URL+r.URL.Path)
				if test.retryAfter != "" {
					w.Header().Set("Retry-After", test.retryAfter)
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, `{"message":"secret"}`)
			})
			client := newDatasetClient(t, server.URL, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := datasetCalls[test.call](ctx, client)
			if (err != nil) != test.fails || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want failure %t without waiting out the deadline", err, test.fails)
			}
			for _, sentinel := range []error{
				langfuse.ErrDatasetNotFound, langfuse.ErrDatasetItemNotFound, langfuse.ErrWriteOutcomeUnknown,
			} {
				if errors.Is(err, sentinel) != (sentinel == test.sentinel) {
					t.Fatalf("error = %v, want sentinel %v", err, test.sentinel)
				}
			}
			if err != nil && (strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "item-1")) {
				t.Fatalf("error %q leaks response or request content", err)
			}
			if got := server.requests.Load(); got != test.requests {
				t.Fatalf("requests = %d, want %d", got, test.requests)
			}
			if got := redirected.Load(); got != 0 {
				t.Fatalf("redirect target received %d requests", got)
			}
		})
	}
}

func TestDatasetRejectsInvalidSuccessBodies(t *testing.T) {
	t.Parallel()
	valid := datasetItemJSON("item-1", "set")
	for name, body := range map[string]string{
		"malformed":         `{"id":`,
		"trailing":          valid + ` {}`,
		"invalid UTF-8":     strings.Replace(valid, "Paris", "Par\xffis", 1),
		"wrong ID":          datasetItemJSON("item-2", "set"),
		"wrong dataset":     datasetItemJSON("item-1", "other"),
		"missing dates":     `{"id":"item-1","datasetId":"d","datasetName":"set","status":"ACTIVE"}`,
		"unknown status":    strings.Replace(valid, "ACTIVE", "DRAFT", 1),
		"array":             `[]`,
		"null":              `null`,
		"non-string ID":     strings.Replace(valid, `"id":"item-1"`, `"id":1`, 1),
		"bad nullable type": strings.Replace(valid, `"sourceTraceId":null`, `"sourceTraceId":7`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				_, _ = io.WriteString(w, body)
			})
			client := newDatasetClient(t, server.URL, nil)
			ctx := context.Background()
			if err := datasetCalls["UpsertDatasetItem"](ctx, client); !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
				t.Fatalf("write error = %v, want ErrWriteOutcomeUnknown", err)
			}
			if got := server.requests.Load(); got != 1 {
				t.Fatalf("write requests = %d; a processed write must not be repeated", got)
			}
			if name == "wrong dataset" {
				return // a read by ID has no expected dataset
			}
			if err := datasetCalls["GetDatasetItem"](ctx, client); err == nil || errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
				t.Fatalf("read error = %v, want a plain failure", err)
			}
		})
	}
}

func TestDatasetResponsesAreBounded(t *testing.T) {
	t.Parallel()
	padding := strings.Repeat(" ", 16<<20)
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		serveDataset(w, r, body)
		_, _ = io.WriteString(w, padding)
	})
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	if err := datasetCalls["UpsertDatasetItem"](ctx, client); !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
		t.Fatalf("oversized write response error = %v, want ErrWriteOutcomeUnknown", err)
	}
	for _, call := range []string{"GetDatasetItem", "DatasetItems"} {
		if err := datasetCalls[call](ctx, client); err == nil {
			t.Fatalf("%s accepted an oversized response", call)
		}
	}
}

func TestDatasetCancellationIsVisibleToErrorsIs(t *testing.T) {
	t.Parallel()
	hang := func(_ http.ResponseWriter, r *http.Request, reached func()) {
		reached()
		<-r.Context().Done()
	}
	for name, test := range map[string]struct {
		respond  func(w http.ResponseWriter, r *http.Request, reached func())
		cancel   string // "before" the call, "shutdown" or cancel once the server "reached" its point, or "deadline"
		call     string
		cause    error
		unknown  bool
		requests int32
	}{
		"pre-canceled read":  {cancel: "before", call: "GetDataset", cause: context.Canceled},
		"pre-canceled write": {cancel: "before", call: "UpsertDatasetItem", cause: context.Canceled},
		"read deadline":      {respond: hang, cancel: "deadline", call: "GetDatasetItem", cause: context.DeadlineExceeded, requests: 1},
		"write deadline": {
			respond: hang, cancel: "deadline", call: "UpsertDatasetItem",
			cause: context.DeadlineExceeded, unknown: true, requests: 1,
		},
		"write canceled while reading the response": {
			respond: func(w http.ResponseWriter, r *http.Request, reached func()) {
				_, _ = io.WriteString(w, `{"id":`)
				w.(http.Flusher).Flush()
				hang(w, r, reached)
			},
			cancel: "reached", call: "UpsertDatasetItem", cause: context.Canceled, unknown: true, requests: 1,
		},
		"read canceled before a retry": {
			respond: func(w http.ResponseWriter, _ *http.Request, reached func()) {
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(http.StatusTooManyRequests)
				reached()
			},
			cancel: "reached", call: "GetDatasetItem", cause: context.Canceled, requests: 1,
		},
		"write in flight at Shutdown": {respond: hang, cancel: "shutdown", call: "UpsertDatasetItem", unknown: true, requests: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			reached := make(chan struct{})
			var once sync.Once
			server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
				test.respond(w, r, func() { once.Do(func() { close(reached) }) })
			})
			client := newDatasetClient(t, server.URL, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch test.cancel {
			case "before":
				cancel()
			case "reached":
				go func() { <-reached; cancel() }()
			case "shutdown":
				go func() { <-reached; _ = client.Shutdown(context.Background()) }()
			case "deadline":
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}
			err := datasetCalls[test.call](ctx, client)
			if err == nil || errors.Is(err, langfuse.ErrWriteOutcomeUnknown) != test.unknown {
				t.Fatalf("error = %v, want outcome unknown %t", err, test.unknown)
			}
			for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
				if errors.Is(err, cause) != (cause == test.cause) {
					t.Fatalf("error = %v, want cause %v", err, test.cause)
				}
			}
			if got := server.requests.Load(); got != test.requests {
				t.Fatalf("requests = %d, want %d", got, test.requests)
			}
		})
	}
}

func TestDatasetItemsPages(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		pages     []string
		query     langfuse.DatasetItemQuery
		want      []string
		fails     bool
		wantQuery url.Values
	}{
		"pinned and filtered": {
			pages: []string{itemsPage(3, "a", "b"), itemsPage(3, "c", "d"), itemsPage(3, "e")},
			query: langfuse.DatasetItemQuery{
				DatasetName: "set", PageSize: 2, SourceTraceID: "trace", SourceObservationID: "span",
				AsOf: time.Date(2026, 9, 30, 12, 0, 0, 999999999, time.FixedZone("CEST", 2*3600)),
			},
			want: []string{"a", "b", "c", "d", "e"},
			wantQuery: url.Values{
				"datasetName": {"set"}, "page": {"1"}, "limit": {"2"}, "sourceTraceId": {"trace"},
				"sourceObservationId": {"span"}, "version": {"2026-09-30T10:00:00.999Z"},
			},
		},
		"empty dataset": {
			pages:     []string{itemsPage(0)},
			wantQuery: url.Values{"datasetName": {"set"}, "page": {"1"}, "limit": {"20"}},
		},
		"overstated page count": {pages: []string{itemsPage(5, "a"), itemsPage(5)}, want: []string{"a"}},
		"invalid later page": {
			pages: []string{itemsPage(2, "a", "b"), `{"data":[{"id":"c"}],"meta":{"totalPages":2}}`},
			want:  []string{"a", "b"}, fails: true,
		},
		"page without meta": {pages: []string{`{"data":[]}`}, fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var queries []url.Values
			server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
				mu.Lock()
				queries = append(queries, r.URL.Query())
				mu.Unlock()
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				_, _ = io.WriteString(w, test.pages[page-1])
			})
			client := newDatasetClient(t, server.URL, nil)
			query := test.query
			query.DatasetName = "set"
			ids, err := collectDatasetItems(context.Background(), client, query)
			if (err != nil) != test.fails || !reflect.DeepEqual(ids, test.want) {
				t.Fatalf("DatasetItems() = %v, %v; want %v, failure %t", ids, err, test.want, test.fails)
			}
			if len(queries) != len(test.pages) {
				t.Fatalf("requests = %d, want %d", len(queries), len(test.pages))
			}
			if test.wantQuery != nil && !reflect.DeepEqual(queries[0], test.wantQuery) {
				t.Fatalf("first query = %v, want %v", queries[0], test.wantQuery)
			}
		})
	}
}

func TestDatasetItemsConvertToPinnedExperimentItems(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, itemsPage(1, "a"))
	})
	client := newDatasetClient(t, server.URL, nil)
	asOf := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	for item, err := range client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set", AsOf: asOf}) {
		if err != nil {
			t.Fatal(err)
		}
		got, err := item.ExperimentItem()
		want := langfuse.ExperimentItem{
			ID:             "a",
			Version:        asOf,
			ExpectedOutput: json.RawMessage(`"Paris"`),
			Metadata:       map[string]any{"m": json.Number("12345678901234567890")},
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("ExperimentItem() = %#v, %v; want %#v", got, err, want)
		}
	}
	_, err := langfuse.DatasetItem{ID: "a", Metadata: json.RawMessage(`"text"`)}.ExperimentItem()
	if !errors.Is(err, langfuse.ErrInvalidExperiment) {
		t.Fatalf("non-object metadata error = %v, want ErrInvalidExperiment", err)
	}
}

func TestDatasetItemsIsLazyAndStopsOnBreak(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, itemsPage(3, "a", "b"))
	})
	client := newDatasetClient(t, server.URL, nil)
	items := client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set", PageSize: 2})
	if server.requests.Load() != 0 {
		t.Fatal("DatasetItems sent a request before iteration")
	}
	for item, err := range items {
		if err != nil || item.ID != "a" {
			t.Fatalf("first item = %q, %v", item.ID, err)
		}
		break
	}
	if got := server.requests.Load(); got != 1 {
		t.Fatalf("requests after an early break = %d, want 1", got)
	}
}

func TestDatasetUnavailableClientsSendNothing(t *testing.T) {
	t.Parallel()
	var masked atomic.Bool
	mask := func(langfuse.MaskField, any) any { masked.Store(true); return nil }
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true, Mask: mask})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server := newDatasetServer(t, serveDataset)
	stopped := newDatasetClient(t, server.URL, func(config *langfuse.Config) { config.Mask = mask })
	if err := stopped.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	for state, client := range map[string]*langfuse.Client{"nil": nil, "disabled": disabled, "stopped": stopped} {
		for name, call := range datasetCalls {
			if err := call(context.Background(), client); err == nil {
				t.Errorf("%s client: %s succeeded", state, name)
			}
		}
	}
	if masked.Load() || server.requests.Load() != 0 {
		t.Fatalf("unavailable clients masked %t and sent %d requests", masked.Load(), server.requests.Load())
	}
}

func TestDatasetItemsShutdownFromLoopBody(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, itemsPage(2, "a", "b"))
	})
	client := newDatasetClient(t, server.URL, nil)
	items := client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set", PageSize: 2})
	var ids []string
	var final error
	for item, err := range items {
		if err != nil {
			final = err
			continue
		}
		ids = append(ids, item.ID)
		if len(ids) == 1 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := client.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() from the loop body = %v", err)
			}
			cancel()
		}
	}
	if final == nil || len(ids) != 2 || server.requests.Load() != 1 {
		t.Fatalf("after Shutdown: ids %v, error %v, requests %d", ids, final, server.requests.Load())
	}
}

type shutdownMarshaler struct{ client **langfuse.Client }

func (m shutdownMarshaler) MarshalJSON() ([]byte, error) {
	_ = (*m.client).Shutdown(context.Background())
	return []byte(`"x"`), nil
}

func TestDatasetCallbacksCanShutDownTheClient(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, serveDataset)
	for name, viaMask := range map[string]bool{"mask": true, "marshaler": false} {
		var client *langfuse.Client
		client = newDatasetClient(t, server.URL, func(config *langfuse.Config) {
			if viaMask {
				config.Mask = func(_ langfuse.MaskField, value any) any {
					_ = client.Shutdown(context.Background())
					return value
				}
			}
		})
		spec := langfuse.DatasetItemSpec{DatasetName: "set", Input: "x"}
		if !viaMask {
			spec.Input = shutdownMarshaler{client: &client}
		}
		done := make(chan error, 1)
		go func() {
			_, err := client.UpsertDatasetItem(context.Background(), spec)
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("%s: the write was sent after its callback shut the client down", name)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: Shutdown from a callback deadlocked the write", name)
		}
	}
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}
