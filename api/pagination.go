package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
)

// PageOptions selects a single numbered page. Zero values select page 1 and
// limit 50. This SDK bounds a page to 100 items; List never walks automatically.
type PageOptions struct {
	Page  int
	Limit int
}

// PageMeta is the numbered pagination metadata returned by project APIs.
type PageMeta struct {
	Page       int `json:"page"`
	Limit      int `json:"limit"`
	TotalItems int `json:"totalItems"`
	TotalPages int `json:"totalPages"`
}

// Page contains one server page, including an explicitly empty data array.
type Page[T any] struct {
	Data []T      `json:"data"`
	Meta PageMeta `json:"meta"`
}

// UnmarshalJSON validates the page envelope and preserves dynamic JSON numbers.
func (p *Page[T]) UnmarshalJSON(data []byte) error {
	type wire Page[T]
	var decoded wire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil { return err }
	if decoded.Data == nil || decoded.Meta.Page < 1 || decoded.Meta.Limit < 1 || decoded.Meta.TotalItems < len(decoded.Data) || decoded.Meta.TotalPages < 0 || len(decoded.Data) > decoded.Meta.Limit || (decoded.Meta.TotalPages == 0 && len(decoded.Data) != 0) {
		return ErrInvalidResponse
	}
	*p = Page[T](decoded)
	return nil
}

// CursorMeta contains the opaque cursor for the next page. Null or empty ends
// the collection. Cursor values must not be interpreted, incremented, or logged.
type CursorMeta struct { NextCursor *string `json:"nextCursor"` }

// CursorPage contains one page of a current cursor-based read API.
type CursorPage[T any] struct {
	Data []T        `json:"data"`
	Meta CursorMeta `json:"meta"`
}

// UnmarshalJSON rejects omitted/null data and omitted/null metadata.
func (p *CursorPage[T]) UnmarshalJSON(data []byte) error {
	var envelope struct {
		Data json.RawMessage `json:"data"`
		Meta json.RawMessage `json:"meta"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil { return err }
	if len(envelope.Data) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) || len(envelope.Meta) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Meta), []byte("null")) { return ErrInvalidResponse }
	type wire CursorPage[T]
	var decoded wire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil { return err }
	if decoded.Data == nil { return ErrInvalidResponse }
	*p = CursorPage[T](decoded)
	return nil
}

// CursorOptions selects one opaque cursor page. Zero limit selects 50.
type CursorOptions struct {
	Cursor string
	Limit  int
}

func pageValues(options PageOptions) (url.Values, error) {
	if options.Page < 0 || options.Limit < 0 || options.Limit > 100 { return nil, errors.New("langfuse api: invalid page or limit") }
	if options.Page == 0 { options.Page = 1 }
	if options.Limit == 0 { options.Limit = 50 }
	return url.Values{"page": {strconv.Itoa(options.Page)}, "limit": {strconv.Itoa(options.Limit)}}, nil
}

func cursorValues(options CursorOptions) (url.Values, error) {
	if options.Limit < 0 || options.Limit > 100 || len(options.Cursor) > 16384 { return nil, errors.New("langfuse api: invalid cursor or limit") }
	if options.Limit == 0 { options.Limit = 50 }
	query := url.Values{"limit": {strconv.Itoa(options.Limit)}}
	if options.Cursor != "" { query.Set("cursor", options.Cursor) }
	return query, nil
}

// WalkOptions bounds explicit pagination. Defaults are 100 pages, 10,000 items,
// and 100 items per page; hard limits are 1,000 pages and 100,000 items. Callbacks
// receive one item at a time, so the SDK retains only one page and cursor history.
type WalkOptions struct {
	MaxPages int
	MaxItems int
	PageSize int
}

var (
	// ErrWalkLimit reports a non-terminal collection reaching an explicit bound.
	ErrWalkLimit = errors.New("langfuse api: pagination bound reached")
	// ErrPagination reports a repeated cursor or inconsistent numbered page.
	ErrPagination = errors.New("langfuse api: pagination did not advance")
)

func normalizeWalk(options WalkOptions) (WalkOptions, error) {
	if options.MaxPages == 0 { options.MaxPages = 100 }
	if options.MaxItems == 0 { options.MaxItems = 10000 }
	if options.PageSize == 0 { options.PageSize = 100 }
	if options.MaxPages < 1 || options.MaxPages > 1000 || options.MaxItems < 1 || options.MaxItems > 100000 || options.PageSize < 1 || options.PageSize > 100 { return WalkOptions{}, errors.New("langfuse api: invalid pagination bounds") }
	return options, nil
}

// WalkPages performs explicitly bounded numbered pagination. Empty intermediate
// pages do not terminate a collection whose metadata reports later pages. A
// callback error or caller cancellation stops before another request is made.
func WalkPages[T any](ctx context.Context, options WalkOptions, fetch func(context.Context, PageOptions) (Page[T], error), yield func(T) error) error {
	if ctx == nil || fetch == nil || yield == nil { return errors.New("langfuse api: nil pagination context or callback") }
	options, err := normalizeWalk(options)
	if err != nil { return err }
	count := 0
	for pageNumber := 1; pageNumber <= options.MaxPages; pageNumber++ {
		if err := ctx.Err(); err != nil { return err }
		page, err := fetch(ctx, PageOptions{Page: pageNumber, Limit: options.PageSize})
		if err != nil { return err }
		if page.Meta.Page != pageNumber || page.Meta.TotalPages < 0 || len(page.Data) > options.PageSize { return ErrPagination }
		for _, item := range page.Data {
			if err := ctx.Err(); err != nil { return err }
			if count == options.MaxItems { return ErrWalkLimit }
			if err := yield(item); err != nil { return err }
			count++
		}
		if pageNumber >= page.Meta.TotalPages { return ctx.Err() }
		if count == options.MaxItems { return ErrWalkLimit }
	}
	return ErrWalkLimit
}

// WalkCursor performs explicitly bounded cursor pagination, detecting repeated
// opaque cursors even across empty pages. It never logs cursor values.
func WalkCursor[T any](ctx context.Context, options WalkOptions, fetch func(context.Context, CursorOptions) (CursorPage[T], error), yield func(T) error) error {
	if ctx == nil || fetch == nil || yield == nil { return errors.New("langfuse api: nil pagination context or callback") }
	options, err := normalizeWalk(options)
	if err != nil { return err }
	seen := make(map[string]struct{}, options.MaxPages)
	cursor := ""
	count := 0
	for range options.MaxPages {
		if err := ctx.Err(); err != nil { return err }
		page, err := fetch(ctx, CursorOptions{Cursor: cursor, Limit: options.PageSize})
		if err != nil { return err }
		if len(page.Data) > options.PageSize { return ErrPagination }
		for _, item := range page.Data {
			if err := ctx.Err(); err != nil { return err }
			if count == options.MaxItems { return ErrWalkLimit }
			if err := yield(item); err != nil { return err }
			count++
		}
		if page.Meta.NextCursor == nil || *page.Meta.NextCursor == "" { return ctx.Err() }
		cursor = *page.Meta.NextCursor
		if len(cursor) > 16384 { return ErrPagination }
		if _, exists := seen[cursor]; exists { return ErrPagination }
		seen[cursor] = struct{}{}
		if count == options.MaxItems { return ErrWalkLimit }
	}
	return ErrWalkLimit
}
