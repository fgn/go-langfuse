package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var left, right any
	decode := func(data string, out *any) error { decoder := json.NewDecoder(strings.NewReader(data)); decoder.UseNumber(); return decoder.Decode(out) }
	if err := decode(string(got), &left); err != nil { t.Fatalf("invalid actual JSON: %v", err) }
	if err := decode(want, &right); err != nil { t.Fatalf("invalid expected JSON: %v", err) }
	if !reflect.DeepEqual(left, right) { t.Fatalf("JSON mismatch\n got: %s\nwant: %s", got, want) }
}

func TestPromptCreateChatUnionAndEmptyPresence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/public/v2/prompts" { t.Errorf("route=%s %s", r.Method, r.URL.Path) }
		body, err := io.ReadAll(r.Body)
		if err != nil { t.Error(err); return }
		assertJSONEqual(t, body, `{"name":"chat","type":"chat","prompt":[{"type":"chatmessage","role":"system","content":""},{"type":"placeholder","name":"history"},{"type":"chatmessage","role":"developer","content":"{{question}}"}],"config":{"large":9007199254740993},"labels":[],"tags":[],"commitMessage":""}`)
		_, _ = io.WriteString(w, `{"name":"chat","version":2,"type":"chat","prompt":[{"role":"system","content":""},{"type":"placeholder","name":"history"}],"config":{"large":9007199254740993},"labels":[],"tags":[],"commitMessage":null,"resolutionGraph":null}`)
	}))
	defer server.Close()
	client := testClient(t, server, nil)
	prompt, err := client.Prompts.Create(t.Context(), CreatePromptRequest{Name: "chat", Prompt: ChatContent(MessageEntry("system", ""), PlaceholderEntry("history"), MessageEntry("developer", "{{question}}")), Config: json.RawMessage(`{"large":9007199254740993}`), Labels: Set([]string{}), Tags: Set([]string{}), CommitMessage: Set("")})
	if err != nil { t.Fatal(err) }
	if prompt.Type != PromptChat || len(prompt.Prompt.Chat) != 2 || prompt.Prompt.Chat[1].Placeholder.Name != "history" { t.Fatalf("decoded prompt=%+v", prompt) }
	if !prompt.CommitMessage.Present || !prompt.CommitMessage.Null || !prompt.ResolutionGraph.Present || !prompt.ResolutionGraph.Null { t.Fatal("lost explicit nulls") }
	if !strings.Contains(string(prompt.Config), "9007199254740993") { t.Fatal("rounded arbitrary config") }
}

func TestPromptCreateTextAndUnionValidation(t *testing.T) {
	request := CreatePromptRequest{Name: "text", Prompt: TextContent("")}
	data, err := json.Marshal(request)
	if err != nil { t.Fatal(err) }
	assertJSONEqual(t, data, `{"name":"text","type":"text","prompt":""}`)
	for _, invalid := range []CreatePromptRequest{
		{Name: "", Prompt: TextContent("x")},
		{Name: "p", Prompt: PromptContent{}},
		{Name: "p", Prompt: PromptContent{Text: new(string), Chat: []ChatEntry{}}},
		{Name: "p", Type: PromptChat, Prompt: TextContent("x")},
		{Name: "p", Prompt: ChatContent(ChatEntry{})},
		{Name: "p", Prompt: TextContent("x"), Config: json.RawMessage(`{"bad":`)},
		{Name: "p", Prompt: TextContent("x"), Labels: Set([]string{"latest"})},
	} {
		if _, err := json.Marshal(invalid); err == nil { t.Errorf("accepted invalid prompt request") }
	}
	for _, invalid := range []string{`null`, `{}`, `{"role":"user"}`, `{"type":"message","role":"user","content":"x"}`, `{"type":"future","name":"x"}`, `{"name":"x","role":"user","content":"x"}`} {
		var entry ChatEntry
		if err := json.Unmarshal([]byte(invalid), &entry); err == nil { t.Errorf("accepted invalid entry %s", invalid) }
	}
}

func TestPromptSelectionMismatchAndDeletionSafety(t *testing.T) {
	var calls atomic.Int32
	var lastQuery atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		lastQuery.Store(r.URL.RawQuery)
		if r.Method == http.MethodDelete { w.WriteHeader(http.StatusNoContent); return }
		_, _ = io.WriteString(w, promptFixture("p", 1))
	}))
	defer server.Close()
	client := testClient(t, server, nil)
	if _, err := client.Prompts.Get(t.Context(), "p", GetPromptOptions{Version: 2}); !errors.Is(err, ErrInvalidResponse) { t.Fatalf("wrong version accepted: %v", err) }
	if _, err := client.Prompts.Get(t.Context(), "other", GetPromptOptions{}); !errors.Is(err, ErrInvalidResponse) { t.Fatalf("wrong name accepted: %v", err) }
	before := calls.Load()
	for _, options := range []DeletePromptOptions{{}, {Version: -1}, {Version: 2, Label: "staging"}, {Version: 2, AllVersions: true}, {Label: "staging", AllVersions: true}} {
		if err := client.Prompts.Delete(t.Context(), "p", options); err == nil { t.Fatal("accepted ambiguous deletion") }
	}
	if _, err := client.Prompts.Get(t.Context(), "p", GetPromptOptions{Version: 2, Label: "staging"}); err == nil { t.Fatal("accepted ambiguous retrieval") }
	if calls.Load() != before { t.Fatal("invalid selection performed I/O") }
	for _, test := range []struct { options DeletePromptOptions; query string }{
		{DeletePromptOptions{Version: 2}, "version=2"},
		{DeletePromptOptions{Label: "staging & test"}, "label=staging+%26+test"},
		{DeletePromptOptions{AllVersions: true}, ""},
	} {
		if err := client.Prompts.Delete(t.Context(), "p", test.options); err != nil { t.Fatal(err) }
		if got := lastQuery.Load(); got != test.query { t.Fatalf("query=%s want=%s", got, test.query) }
	}
}

func TestPromptLabelUpdateWireAndSingleAttempt(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPatch || r.URL.EscapedPath() != "/api/public/v2/prompts/folder%2Fp/versions/2" { t.Errorf("route=%s %s", r.Method, r.URL.EscapedPath()) }
		body, err := io.ReadAll(r.Body)
		if err != nil { t.Error(err); return }
		assertJSONEqual(t, body, `{"newLabels":[]}`)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := testClient(t, server, nil)
	if _, err := client.Prompts.UpdateLabels(t.Context(), "folder/p", 2, nil); err == nil { t.Fatal("accepted failed write") }
	if calls.Load() != 1 { t.Fatalf("replayed label mutation %d times", calls.Load()) }
	if _, err := client.Prompts.UpdateLabels(t.Context(), "folder/p", 2, []string{"latest"}); err == nil { t.Fatal("accepted reserved latest label") }
	if calls.Load() != 1 { t.Fatal("invalid label caused I/O") }
}

func TestPromptListFilterDatesAndPage(t *testing.T) {
	from := time.Date(2026, time.September, 1, 8, 30, 0, 123, time.FixedZone("test", 2*60*60))
	to := from.Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		want := url.Values{"page": {"2"}, "limit": {"3"}, "name": {"a/b"}, "label": {"staging"}, "tag": {"test"}, "fromUpdatedAt": {from.UTC().Format(time.RFC3339Nano)}, "toUpdatedAt": {to.UTC().Format(time.RFC3339Nano)}}
		if !reflect.DeepEqual(query, want) { t.Errorf("query=%v want=%v", query, want) }
		_, _ = io.WriteString(w, `{"data":[],"meta":{"page":2,"limit":3,"totalItems":5,"totalPages":3}}`)
	}))
	defer server.Close()
	client := testClient(t, server, nil)
	page, err := client.Prompts.List(t.Context(), ListPromptsOptions{PageOptions: PageOptions{Page: 2, Limit: 3}, Name: "a/b", Label: "staging", Tag: "test", FromUpdatedAt: from, ToUpdatedAt: to})
	if err != nil || page.Meta.Page != 2 || len(page.Data) != 0 { t.Fatalf("page=%+v err=%v", page, err) }
}

func TestOptionalAndSparseFieldPreserveNullZeroAndAbsence(t *testing.T) {
	request := struct {
		Omitted *Optional[string] `json:"omitted,omitempty"`
		Null *Optional[string] `json:"null,omitempty"`
		Flag *Optional[bool] `json:"flag,omitempty"`
		Count *Optional[int] `json:"count,omitempty"`
		Items *Optional[[]string] `json:"items,omitempty"`
	}{Null: Null[string](), Flag: Set(false), Count: Set(0), Items: Set([]string{})}
	data, err := json.Marshal(request)
	if err != nil { t.Fatal(err) }
	assertJSONEqual(t, data, `{"null":null,"flag":false,"count":0,"items":[]}`)
	var response struct {
		Absent Field[int] `json:"absent,omitzero"`
		Null Field[int] `json:"null,omitzero"`
		Zero Field[int] `json:"zero,omitzero"`
		Flag Field[bool] `json:"flag,omitzero"`
		Large Field[json.Number] `json:"large,omitzero"`
	}
	const original = `{"null":null,"zero":0,"flag":false,"large":9007199254740993}`
	if err := json.Unmarshal([]byte(original), &response); err != nil { t.Fatal(err) }
	if response.Absent.Present || !response.Null.Null || !response.Zero.Present || response.Zero.Null || response.Zero.Value != 0 || response.Flag.Value { t.Fatal("presence semantics changed") }
	data, err = json.Marshal(response)
	if err != nil { t.Fatal(err) }
	assertJSONEqual(t, data, original)
	if !validateNumber(json.Number("0")) || !validateNumber(json.Number("1e1000")) || validateNumber(json.Number("NaN")) || validateNumber(json.Number("")) { t.Fatal("number validation changed") }
}

func TestScoreValueUnionIsLossless(t *testing.T) {
	for _, input := range []string{`9007199254740993`, `0.1234567890123456789`, `true`, `false`, `"category"`, `"correction text"`} {
		var value ScoreValue
		if err := json.Unmarshal([]byte(input), &value); err != nil { t.Fatal(err) }
		data, err := json.Marshal(value)
		if err != nil { t.Fatal(err) }
		assertJSONEqual(t, data, input)
	}
	for _, input := range []string{`null`, `[]`, `{}`, `NaN`} {
		var value ScoreValue
		if err := json.Unmarshal([]byte(input), &value); err == nil { t.Fatalf("accepted %s", input) }
	}
	if _, err := json.Marshal(ScoreValue{}); err == nil { t.Fatal("accepted an empty score union") }
}

func TestWalkPagesEmptyIntermediateAndCancellation(t *testing.T) {
	var pages []int
	var items []int
	err := WalkPages(t.Context(), WalkOptions{PageSize: 1}, func(_ context.Context, options PageOptions) (Page[int], error) {
		pages = append(pages, options.Page)
		data := []int{}
		if options.Page != 2 { data = []int{options.Page} }
		return Page[int]{Data: data, Meta: PageMeta{Page: options.Page, Limit: 1, TotalItems: 2, TotalPages: 3}}, nil
	}, func(value int) error { items = append(items, value); return nil })
	if err != nil || !reflect.DeepEqual(pages, []int{1, 2, 3}) || !reflect.DeepEqual(items, []int{1, 3}) { t.Fatalf("pages=%v items=%v err=%v", pages, items, err) }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	err = WalkPages(ctx, WalkOptions{}, func(_ context.Context, options PageOptions) (Page[int], error) {
		calls++
		return Page[int]{Data: []int{1}, Meta: PageMeta{Page: options.Page, TotalPages: 3}}, nil
	}, func(_ int) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 { t.Fatalf("cancel calls=%d err=%v", calls, err) }
}

func TestWalkBoundsAndRepeatedCursor(t *testing.T) {
	calls := 0
	next := "opaque & cursor"
	err := WalkCursor(t.Context(), WalkOptions{}, func(_ context.Context, options CursorOptions) (CursorPage[int], error) {
		calls++
		if calls == 2 && options.Cursor != next { t.Fatal("cursor was modified") }
		return CursorPage[int]{Data: []int{}, Meta: CursorMeta{NextCursor: &next}}, nil
	}, func(_ int) error { t.Fatal("empty page emitted an item"); return nil })
	if !errors.Is(err, ErrPagination) || calls != 2 { t.Fatalf("loop calls=%d err=%v", calls, err) }
	calls = 0
	err = WalkPages(t.Context(), WalkOptions{MaxPages: 2}, func(_ context.Context, options PageOptions) (Page[int], error) {
		calls++
		return Page[int]{Data: []int{}, Meta: PageMeta{Page: options.Page, TotalPages: 999}}, nil
	}, func(_ int) error { return nil })
	if !errors.Is(err, ErrWalkLimit) || calls != 2 { t.Fatalf("bound calls=%d err=%v", calls, err) }
	count := 0
	err = WalkPages(t.Context(), WalkOptions{MaxItems: 1}, func(_ context.Context, options PageOptions) (Page[int], error) { return Page[int]{Data: []int{1, 2}, Meta: PageMeta{Page: options.Page, TotalPages: 1}}, nil }, func(_ int) error { count++; return nil })
	if !errors.Is(err, ErrWalkLimit) || count != 1 { t.Fatalf("item bound count=%d err=%v", count, err) }
}

func TestPaginationRejectsMalformedEnvelopes(t *testing.T) {
	for _, input := range []string{`{}`, `{"data":null,"meta":{"page":1,"limit":1}}`, `{"data":[],"meta":null}`, `{"data":[1,2],"meta":{"page":1,"limit":1,"totalItems":2,"totalPages":2}}`} {
		var page Page[int]
		if err := json.Unmarshal([]byte(input), &page); err == nil { t.Fatalf("accepted page %s", input) }
	}
	for _, input := range []string{`{}`, `{"data":null,"meta":{"nextCursor":null}}`, `{"data":[],"meta":null}`, `{"data":[]}`} {
		var page CursorPage[int]
		if err := json.Unmarshal([]byte(input), &page); err == nil { t.Fatalf("accepted cursor page %s", input) }
	}
}

func FuzzPromptContent(f *testing.F) {
	for _, seed := range []string{`"Hi {{name}}"`, `[{"role":"user","content":""}]`, `[{"type":"placeholder","name":"history"}]`, `null`, `[]`} { f.Add(seed) }
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 { t.Skip() }
		var content PromptContent
		if json.Unmarshal([]byte(input), &content) != nil { return }
		data, err := json.Marshal(content)
		if err != nil || !json.Valid(data) { t.Fatalf("accepted content cannot marshal: %v", err) }
		var again PromptContent
		if err := json.Unmarshal(data, &again); err != nil { t.Fatal(err) }
	})
}

func FuzzScoreValue(f *testing.F) {
	for _, seed := range []string{`9007199254740993`, `true`, `"label"`, `null`, `[]`} { f.Add(seed) }
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 { t.Skip() }
		var value ScoreValue
		if json.Unmarshal([]byte(input), &value) != nil { return }
		data, err := json.Marshal(value)
		if err != nil || !json.Valid(data) { t.Fatalf("accepted value cannot marshal: %v", err) }
	})
}
