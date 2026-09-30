package langfuse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

const datasetTimes = `"createdAt":"2026-09-30T10:00:00.000Z","updatedAt":"2026-09-30T11:00:00.000Z"`

func datasetItemJSON(id, datasetName string) string {
	return fmt.Sprintf(`{"id":%q,"datasetId":"ds-1","datasetName":%q,"status":"ACTIVE","input":{"q":1},`+
		`"expectedOutput":"Paris","metadata":{"m":12345678901234567890},"sourceTraceId":null,`+
		`"sourceObservationId":null,%s,"mediaReferences":[]}`, id, datasetName, datasetTimes)
}

// datasetServer is a scripted Langfuse dataset API. handle receives each
// request with its body and writes the response.
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

func TestDatasetUpsertItemSendsMaskedBodyOnce(t *testing.T) {
	t.Parallel()
	var body atomic.Value
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, request []byte) {
		body.Store(string(request))
		_, _ = io.WriteString(w, datasetItemJSON("item-1", "team/set"))
	})
	var maskCalls sync.Map
	client := newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.DisableContentCapture = true // does not apply to dataset writes
		config.Mask = func(field langfuse.MaskField, value any) any {
			count, _ := maskCalls.LoadOrStore(field, new(atomic.Int32))
			count.(*atomic.Int32).Add(1)
			if field == langfuse.MaskDatasetItemInput {
				return map[string]any{"question": "[redacted]"}
			}
			return value
		}
	})
	item, err := client.UpsertDatasetItem(context.Background(), langfuse.DatasetItemSpec{
		DatasetName:    "team/set",
		ID:             "item-1",
		Input:          map[string]any{"question": "secret"},
		ExpectedOutput: json.RawMessage(`"Paris"`),
		Metadata:       map[string]any{},
		SourceTraceID:  "0123456789abcdef0123456789abcdef",
		Status:         langfuse.DatasetItemArchived,
	})
	if err != nil {
		t.Fatalf("UpsertDatasetItem() error = %v", err)
	}
	want := `{"datasetName":"team/set","expectedOutput":"Paris","id":"item-1","input":{"question":"[redacted]"},` +
		`"metadata":{},"sourceTraceId":"0123456789abcdef0123456789abcdef","status":"ARCHIVED"}`
	if got := body.Load(); got != want {
		t.Fatalf("request body = %s\nwant %s", got, want)
	}
	for _, field := range []langfuse.MaskField{
		langfuse.MaskDatasetItemInput, langfuse.MaskDatasetItemExpectedOutput, langfuse.MaskDatasetItemMetadata,
	} {
		count, ok := maskCalls.Load(field)
		if !ok || count.(*atomic.Int32).Load() != 1 {
			t.Fatalf("Mask(%q) was not called exactly once", field)
		}
	}
	if item.ID != "item-1" || item.DatasetName != "team/set" || item.Status != langfuse.DatasetItemActive ||
		string(item.ExpectedOutput) != `"Paris"` || string(item.Metadata) != `{"m":12345678901234567890}` {
		t.Fatalf("UpsertDatasetItem() = %+v", item)
	}
}

func TestDatasetWritesFailClosedBeforeIO(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, datasetItemJSON("item-1", "set"))
	})
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	for name, test := range map[string]struct {
		mask func(langfuse.MaskField, any) any
		spec langfuse.DatasetItemSpec
	}{
		"mask returns nil": {
			mask: func(langfuse.MaskField, any) any { return nil },
			spec: langfuse.DatasetItemSpec{DatasetName: "set", ExpectedOutput: "secret"},
		},
		"mask panics": {
			mask: func(langfuse.MaskField, any) any { panic("PANIC-PAYLOAD") },
			spec: langfuse.DatasetItemSpec{DatasetName: "set", Input: "secret"},
		},
		"mask changes metadata type": {
			mask: func(_ langfuse.MaskField, value any) any { return fmt.Sprint(value) },
			spec: langfuse.DatasetItemSpec{DatasetName: "set", Metadata: map[string]any{"a": 1}},
		},
		"raw null":        {spec: langfuse.DatasetItemSpec{DatasetName: "set", Input: json.RawMessage(`null`)}},
		"invalid raw":     {spec: langfuse.DatasetItemSpec{DatasetName: "set", Input: json.RawMessage(`{`)}},
		"cycle":           {spec: langfuse.DatasetItemSpec{DatasetName: "set", Metadata: cyclic}},
		"oversized field": {spec: langfuse.DatasetItemSpec{DatasetName: "set", Input: strings.Repeat("x", 1<<20)}},
		"unsupported":     {spec: langfuse.DatasetItemSpec{DatasetName: "set", Input: func() {}}},
		"missing dataset": {spec: langfuse.DatasetItemSpec{Input: "x"}},
		"bad status":      {spec: langfuse.DatasetItemSpec{DatasetName: "set", Status: "DRAFT"}},
		"observation without trace": {
			spec: langfuse.DatasetItemSpec{DatasetName: "set", SourceObservationID: "0123456789abcdef"},
		},
		"padded ID":   {spec: langfuse.DatasetItemSpec{DatasetName: "set", ID: " item"}},
		"control":     {spec: langfuse.DatasetItemSpec{DatasetName: "se\tt"}},
		"long name":   {spec: langfuse.DatasetItemSpec{DatasetName: strings.Repeat("n", 256)}},
		"invalid ID":  {spec: langfuse.DatasetItemSpec{DatasetName: "set", ID: "\xff"}},
		"long source": {spec: langfuse.DatasetItemSpec{DatasetName: "set", SourceTraceID: strings.Repeat("t", 256)}},
	} {
		t.Run(name, func(t *testing.T) {
			client := newDatasetClient(t, server.URL, func(config *langfuse.Config) { config.Mask = test.mask })
			before := server.requests.Load()
			_, err := client.UpsertDatasetItem(context.Background(), test.spec)
			if err == nil {
				t.Fatal("UpsertDatasetItem() accepted invalid or unmaskable content")
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "PANIC-PAYLOAD") {
				t.Fatalf("error %q leaks content", err)
			}
			if got := server.requests.Load() - before; got != 0 {
				t.Fatalf("a rejected write sent %d requests", got)
			}
		})
	}
}

func TestDatasetUpsertDatasetValidatesAndMasks(t *testing.T) {
	t.Parallel()
	var body atomic.Value
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, request []byte) {
		body.Store(string(request))
		_, _ = fmt.Fprintf(w, `{"id":"ds-1","name":"set","description":"d","metadata":{"k":"[m]"},`+
			`"inputSchema":{"type":"object"},"expectedOutputSchema":null,%s}`, datasetTimes)
	})
	client := newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.Mask = func(field langfuse.MaskField, value any) any {
			if field == langfuse.MaskDatasetMetadata {
				return map[string]any{"k": "[m]"}
			}
			return value
		}
	})
	description := "d"
	dataset, err := client.UpsertDataset(context.Background(), langfuse.DatasetSpec{
		Name: "set", Description: &description, Metadata: map[string]any{"k": "secret"},
		InputSchema: json.RawMessage(` {"type": "object"} `), ExpectedOutputSchema: json.RawMessage(`null`),
	})
	if err != nil {
		t.Fatalf("UpsertDataset() error = %v", err)
	}
	if got := body.Load(); got != `{"description":"d","expectedOutputSchema":null,"inputSchema":{"type":"object"},"metadata":{"k":"[m]"},"name":"set"}` {
		t.Fatalf("request body = %s", got)
	}
	// Nil fields are omitted, so an update can change one field alone, and an
	// explicit empty description clears it.
	empty := ""
	if _, err := client.UpsertDataset(context.Background(), langfuse.DatasetSpec{Name: "set", Description: &empty}); err != nil {
		t.Fatalf("UpsertDataset() error = %v", err)
	}
	if got := body.Load(); got != `{"description":"","name":"set"}` {
		t.Fatalf("partial update body = %s", got)
	}
	if dataset.ID != "ds-1" || string(dataset.Metadata) != `{"k":"[m]"}` || dataset.ExpectedOutputSchema != nil {
		t.Fatalf("UpsertDataset() = %+v", dataset)
	}
	for name, spec := range map[string]langfuse.DatasetSpec{
		"array schema":     {Name: "set", InputSchema: json.RawMessage(`[]`)},
		"invalid schema":   {Name: "set", ExpectedOutputSchema: json.RawMessage(`{`)},
		"long description": {Name: "set", Description: func() *string { d := strings.Repeat("d", 16<<10+1); return &d }()},
		"empty name":       {},
	} {
		before := server.requests.Load()
		if _, err := client.UpsertDataset(context.Background(), spec); err == nil {
			t.Fatalf("%s: UpsertDataset() accepted invalid input", name)
		}
		if server.requests.Load() != before {
			t.Fatalf("%s: invalid input sent a request", name)
		}
	}
}

func TestDatasetErrorsMapPerOperation(t *testing.T) {
	t.Parallel()
	status := atomic.Int32{}
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(int(status.Load()))
		_, _ = io.WriteString(w, `{"message":"secret-body"}`)
	})
	client := newDatasetClient(t, server.URL, nil)
	ctx := context.Background()
	status.Store(http.StatusNotFound)
	if _, err := client.GetDataset(ctx, "set"); !errors.Is(err, langfuse.ErrDatasetNotFound) {
		t.Fatalf("GetDataset 404 = %v", err)
	}
	if _, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set"}); !errors.Is(err, langfuse.ErrDatasetNotFound) {
		t.Fatalf("UpsertDatasetItem 404 = %v", err)
	}
	if _, err := client.GetDatasetItem(ctx, "item"); !errors.Is(err, langfuse.ErrDatasetItemNotFound) {
		t.Fatalf("GetDatasetItem 404 = %v", err)
	}
	if err := client.DeleteDatasetItem(ctx, "item"); !errors.Is(err, langfuse.ErrDatasetItemNotFound) {
		t.Fatalf("DeleteDatasetItem 404 = %v", err)
	}
	for _, err := range client.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: "set"}) {
		if !errors.Is(err, langfuse.ErrDatasetNotFound) {
			t.Fatalf("DatasetItems 404 = %v", err)
		}
	}
	if _, err := client.UpsertDataset(ctx, langfuse.DatasetSpec{Name: "set"}); err == nil ||
		errors.Is(err, langfuse.ErrDatasetNotFound) {
		t.Fatalf("UpsertDataset 404 = %v, want a plain status error", err)
	}

	status.Store(http.StatusBadGateway)
	_, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set", ID: "item-1"})
	if !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
		t.Fatalf("item write 502 = %v, want ErrWriteOutcomeUnknown", err)
	}
	if strings.Contains(err.Error(), "secret-body") || strings.Contains(err.Error(), "item-1") {
		t.Fatalf("error %q leaks the response body or the item ID", err)
	}
	status.Store(http.StatusConflict)
	_, err = client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set", ID: "item-1"})
	if err == nil || errors.Is(err, langfuse.ErrWriteOutcomeUnknown) || !strings.Contains(err.Error(), "409") {
		t.Fatalf("item write 409 = %v, want a known rejection naming the status", err)
	}
}

func TestDatasetCancellationIsVisibleToErrorsIs(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	client := newDatasetClient(t, server.URL, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set", ID: "item"})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
		t.Fatalf("expired write = %v, want DeadlineExceeded and ErrWriteOutcomeUnknown", err)
	}
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	before := server.requests.Load()
	if _, err := client.GetDataset(canceled, "set"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled read = %v, want context.Canceled", err)
	}
	if server.requests.Load() != before {
		t.Fatal("a pre-canceled call sent a request")
	}
}

func TestDatasetUnavailableClientsSendNothing(t *testing.T) {
	t.Parallel()
	var masked atomic.Bool
	disabled, err := langfuse.New(context.Background(), langfuse.Config{
		Disabled: true,
		Mask:     func(langfuse.MaskField, any) any { masked.Store(true); return nil },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server := newDatasetServer(t, func(http.ResponseWriter, *http.Request, []byte) {})
	stopped := newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.Mask = func(langfuse.MaskField, any) any { masked.Store(true); return nil }
	})
	if err := stopped.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	ctx := context.Background()
	for name, client := range map[string]*langfuse.Client{"nil": nil, "disabled": disabled, "stopped": stopped} {
		if _, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{DatasetName: "set", Input: "x"}); err == nil {
			t.Fatalf("%s: UpsertDatasetItem() succeeded", name)
		}
		if _, err := client.UpsertDataset(ctx, langfuse.DatasetSpec{Name: "set", Metadata: map[string]any{"a": 1}}); err == nil {
			t.Fatalf("%s: UpsertDataset() succeeded", name)
		}
		if _, err := client.GetDataset(ctx, "set"); err == nil {
			t.Fatalf("%s: GetDataset() succeeded", name)
		}
		if _, err := client.GetDatasetItem(ctx, "item"); err == nil {
			t.Fatalf("%s: GetDatasetItem() succeeded", name)
		}
		if err := client.DeleteDatasetItem(ctx, "item"); err == nil {
			t.Fatalf("%s: DeleteDatasetItem() succeeded", name)
		}
		for _, err := range client.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: "set"}) {
			if err == nil {
				t.Fatalf("%s: DatasetItems() yielded an item", name)
			}
		}
		// Invalid input is still an error, before availability.
		if _, err := client.GetDataset(ctx, ""); err == nil || !strings.Contains(err.Error(), "required") {
			t.Fatalf("%s: GetDataset(\"\") = %v, want a validation error", name, err)
		}
	}
	if masked.Load() {
		t.Fatal("an unavailable client called Mask")
	}
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("unavailable clients sent %d requests", got)
	}
}

// pageScript serves dataset item pages from a function of the page number.
func pageScript(t *testing.T, pages func(page, limit int) string) (*datasetServer, *[]string) {
	t.Helper()
	var mu sync.Mutex
	var queries []string
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		mu.Unlock()
		var page, limit int
		_, _ = fmt.Sscan(r.URL.Query().Get("page"), &page)
		_, _ = fmt.Sscan(r.URL.Query().Get("limit"), &limit)
		_, _ = io.WriteString(w, pages(page, limit))
	})
	return server, &queries
}

func itemsPage(page, limit, total int, ids ...string) string {
	items := make([]string, len(ids))
	for index, id := range ids {
		items[index] = datasetItemJSON(id, "set")
	}
	totalPages := (total + limit - 1) / limit
	return fmt.Sprintf(`{"data":[%s],"meta":{"page":%d,"limit":%d,"totalItems":%d,"totalPages":%d}}`,
		strings.Join(items, ","), page, limit, total, totalPages)
}

func collectDatasetItems(client *langfuse.Client, query langfuse.DatasetItemQuery) ([]string, error) {
	var ids []string
	for item, err := range client.DatasetItems(context.Background(), query) {
		if err != nil {
			return ids, err
		}
		ids = append(ids, item.ID)
	}
	return ids, nil
}

func TestDatasetItemsPinsAndPagesExactly(t *testing.T) {
	t.Parallel()
	server, queries := pageScript(t, func(page, limit int) string {
		switch page {
		case 1:
			return itemsPage(1, limit, 5, "a", "b")
		case 2:
			return itemsPage(2, limit, 5, "c", "d")
		default:
			return itemsPage(3, limit, 5, "e")
		}
	})
	client := newDatasetClient(t, server.URL, nil)
	asOf := time.Date(2026, 9, 30, 12, 0, 0, 999999999, time.FixedZone("CEST", 2*3600))
	ids, err := collectDatasetItems(client, langfuse.DatasetItemQuery{DatasetName: "set", AsOf: asOf, PageSize: 2})
	if err != nil || strings.Join(ids, "") != "abcde" {
		t.Fatalf("DatasetItems() = %v, %v", ids, err)
	}
	if len(*queries) != 3 ||
		(*queries)[0] != "datasetName=set&limit=2&page=1&version=2026-09-30T10%3A00%3A00.999Z" ||
		!strings.Contains((*queries)[2], "page=3") {
		t.Fatalf("queries = %q", *queries)
	}
}

func TestDatasetItemsEmptyDataset(t *testing.T) {
	t.Parallel()
	server, queries := pageScript(t, func(page, limit int) string { return itemsPage(page, limit, 0) })
	client := newDatasetClient(t, server.URL, nil)
	ids, err := collectDatasetItems(client, langfuse.DatasetItemQuery{DatasetName: "set"})
	if err != nil || len(ids) != 0 || len(*queries) != 1 || !strings.Contains((*queries)[0], "limit=20") {
		t.Fatalf("empty dataset = %v, %v after %q", ids, err, *queries)
	}
}

func TestDatasetItemsRejectsInconsistentPages(t *testing.T) {
	t.Parallel()
	for name, pages := range map[string]func(page, limit int) string{
		"duplicate across pages": func(page, limit int) string {
			if page == 1 {
				return itemsPage(1, limit, 4, "a", "b")
			}
			return itemsPage(2, limit, 4, "b", "c")
		},
		"A/B/A cycle": func(page, limit int) string {
			if page == 2 {
				return itemsPage(2, limit, 6, "c", "d")
			}
			return itemsPage(page, limit, 6, "a", "b")
		},
		"total changes": func(page, limit int) string {
			if page == 1 {
				return itemsPage(1, limit, 4, "a", "b")
			}
			return itemsPage(2, limit, 5, "c", "d")
		},
		"short non-final page": func(page, limit int) string {
			if page == 1 {
				return itemsPage(1, limit, 4, "a")
			}
			return itemsPage(2, limit, 4, "c", "d")
		},
		"final page too long": func(page, limit int) string {
			if page == 1 {
				return itemsPage(1, limit, 3, "a", "b")
			}
			return itemsPage(2, limit, 3, "c", "d")
		},
		"wrong page number": func(_, limit int) string { return itemsPage(1, limit, 4, "a", "b") },
		"wrong limit":       func(page, _ int) string { return itemsPage(page, 3, 4, "a", "b", "c") },
		"inconsistent total pages": func(_, limit int) string {
			return fmt.Sprintf(`{"data":[],"meta":{"page":1,"limit":%d,"totalItems":0,"totalPages":1}}`, limit)
		},
		"page cap": func(_, limit int) string {
			return fmt.Sprintf(`{"data":[],"meta":{"page":1,"limit":%d,"totalItems":%d,"totalPages":10001}}`,
				limit, 10001*limit)
		},
		"other dataset": func(_, limit int) string {
			return fmt.Sprintf(`{"data":[%s],"meta":{"page":1,"limit":%d,"totalItems":1,"totalPages":1}}`,
				datasetItemJSON("a", "other"), limit)
		},
		"archived item": func(_, limit int) string {
			return fmt.Sprintf(`{"data":[%s],"meta":{"page":1,"limit":%d,"totalItems":1,"totalPages":1}}`,
				strings.Replace(datasetItemJSON("a", "set"), "ACTIVE", "ARCHIVED", 1), limit)
		},
		"missing meta": func(int, int) string { return `{"data":[]}` },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, _ := pageScript(t, pages)
			client := newDatasetClient(t, server.URL, nil)
			var yielded []string
			errorsSeen := 0
			for item, err := range client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{
				DatasetName: "set", PageSize: 2,
			}) {
				if err != nil {
					errorsSeen++
					continue
				}
				yielded = append(yielded, item.ID)
			}
			if errorsSeen != 1 {
				t.Fatalf("errors yielded = %d (items %v), want exactly one terminal error", errorsSeen, yielded)
			}
			// Only pages before the invalid one may be yielded: the A/B/A
			// cycle fails on its third page, every other case on its second.
			valid := 2
			if name == "A/B/A cycle" {
				valid = 4
			}
			if len(yielded) > valid {
				t.Fatalf("items from an invalid page were yielded: %v", yielded)
			}
		})
	}
}

func TestDatasetItemsIsLazyAndStopsOnBreak(t *testing.T) {
	t.Parallel()
	server, queries := pageScript(t, func(page, limit int) string {
		return itemsPage(page, limit, 6, fmt.Sprint("p", page, "a"), fmt.Sprint("p", page, "b"))
	})
	client := newDatasetClient(t, server.URL, nil)
	sequence := client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set", PageSize: 2})
	if len(*queries) != 0 {
		t.Fatal("DatasetItems sent a request before iteration")
	}
	for item, err := range sequence {
		if err != nil || item.ID != "p1a" {
			t.Fatalf("first item = %q, %v", item.ID, err)
		}
		break
	}
	if len(*queries) != 1 {
		t.Fatalf("requests after an early break = %d, want 1", len(*queries))
	}
}

func TestDatasetItemsValidatesQueryBeforeIO(t *testing.T) {
	t.Parallel()
	server, queries := pageScript(t, func(page, limit int) string { return itemsPage(page, limit, 0) })
	client := newDatasetClient(t, server.URL, nil)
	for name, query := range map[string]langfuse.DatasetItemQuery{
		"no dataset":        {},
		"negative size":     {DatasetName: "set", PageSize: -1},
		"oversized page":    {DatasetName: "set", PageSize: 101},
		"year out of range": {DatasetName: "set", AsOf: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		"future as-of":      {DatasetName: "set", AsOf: time.Now().Add(time.Hour)},
		"bad source":        {DatasetName: "set", SourceTraceID: "a\x00b"},
	} {
		if _, err := collectDatasetItems(client, query); err == nil {
			t.Fatalf("%s: DatasetItems() accepted an invalid query", name)
		}
	}
	if len(*queries) != 0 {
		t.Fatalf("invalid queries sent %d requests", len(*queries))
	}
}

func TestDatasetShutdownCancelsAndRefusesDatasetIO(t *testing.T) {
	t.Parallel()
	started := make(chan struct{}, 1)
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
	client := newDatasetClient(t, server.URL, nil)
	result := make(chan error, 1)
	go func() {
		_, err := client.UpsertDatasetItem(context.Background(), langfuse.DatasetItemSpec{DatasetName: "set", ID: "i"})
		result <- err
	}()
	<-started
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, langfuse.ErrWriteOutcomeUnknown) || !strings.Contains(err.Error(), "shut down") {
			t.Fatalf("in-flight write after Shutdown = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not cancel in-flight dataset I/O")
	}
	before := server.requests.Load()
	if _, err := client.GetDataset(context.Background(), "set"); err == nil {
		t.Fatal("GetDataset() succeeded after Shutdown")
	}
	if server.requests.Load() != before {
		t.Fatal("a dataset call after Shutdown sent a request")
	}
}

func TestDatasetItemsReentrantShutdownFromLoopBody(t *testing.T) {
	t.Parallel()
	server, queries := pageScript(t, func(page, limit int) string {
		return itemsPage(page, limit, 4, fmt.Sprint("p", page, "a"), fmt.Sprint("p", page, "b"))
	})
	client := newDatasetClient(t, server.URL, nil)
	sequence := client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set", PageSize: 2})
	var ids []string
	var final error
	for item, err := range sequence {
		if err != nil {
			final = err
			break
		}
		ids = append(ids, item.ID)
		if len(ids) == 1 {
			// Shutdown from inside the loop body must not deadlock on the
			// iterator, and the next page must not be requested.
			done := make(chan error, 1)
			go func() { done <- client.Shutdown(context.Background()) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Shutdown() error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Shutdown waited for the iterator's loop body")
			}
		}
	}
	if final == nil || len(ids) != 2 || len(*queries) != 1 {
		t.Fatalf("after shutdown: ids %v, error %v, requests %d", ids, final, len(*queries))
	}
	// A stored iterator invoked after shutdown sends nothing either.
	for _, err := range sequence {
		if err == nil {
			t.Fatal("stored iterator yielded an item after shutdown")
		}
	}
	if len(*queries) != 1 {
		t.Fatalf("stored iterator sent %d requests after shutdown", len(*queries)-1)
	}
}

func TestDatasetMaskReentersShutdown(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, datasetItemJSON("i", "set"))
	})
	var client *langfuse.Client
	client = newDatasetClient(t, server.URL, func(config *langfuse.Config) {
		config.Mask = func(_ langfuse.MaskField, value any) any {
			_ = client.Shutdown(context.Background())
			return value
		}
	})
	_, err := client.UpsertDatasetItem(context.Background(), langfuse.DatasetItemSpec{DatasetName: "set", ID: "i", Input: "x"})
	if err == nil {
		t.Fatal("a write whose masker shut the client down was still sent")
	}
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}

// shutdownMarshaler shuts its client down from inside serialization, the
// way a trusted callback can re-enter the SDK.
type shutdownMarshaler struct{ client **langfuse.Client }

func (m shutdownMarshaler) MarshalJSON() ([]byte, error) {
	_ = (*m.client).Shutdown(context.Background())
	return []byte(`"x"`), nil
}

type nullMarshaler struct{}

func (nullMarshaler) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

func TestDatasetSerializerCallbacksRunBeforeAdmission(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, datasetItemJSON("i", "set"))
	})
	client := newDatasetClient(t, server.URL, nil)
	done := make(chan error, 1)
	go func() {
		_, err := client.UpsertDatasetItem(context.Background(), langfuse.DatasetItemSpec{
			DatasetName: "set", ID: "i", Input: shutdownMarshaler{client: &client},
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a write whose serializer shut the client down was still sent")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown from a serializer deadlocked the write")
	}
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}

	other := newDatasetClient(t, server.URL, nil)
	if _, err := other.UpsertDatasetItem(context.Background(), langfuse.DatasetItemSpec{
		DatasetName: "set", ExpectedOutput: nullMarshaler{},
	}); err == nil {
		t.Fatal("a value that serializes to JSON null was sent, which would keep the stored value")
	}
}

func TestDatasetCallsRaceShutdown(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		_, _ = io.WriteString(w, datasetItemJSON("i", "set"))
	})
	client := newDatasetClient(t, server.URL, nil)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 20 {
				_, _ = client.GetDatasetItem(context.Background(), "i")
				for _, err := range client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set"}) {
					_ = err
				}
			}
		})
	}
	time.Sleep(5 * time.Millisecond)
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dataset calls did not return after Shutdown")
	}
	// Requests canceled by Shutdown can still reach the handler late; let
	// them settle, then prove no call is admitted any more.
	time.Sleep(100 * time.Millisecond)
	after := server.requests.Load()
	_, _ = client.GetDatasetItem(context.Background(), "i")
	for _, err := range client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set"}) {
		_ = err
	}
	time.Sleep(20 * time.Millisecond)
	if server.requests.Load() != after {
		t.Fatal("dataset requests continued after Shutdown returned")
	}
}
