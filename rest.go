package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/big"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrWriteOutcomeUnknown reports a REST write, such as a dataset or score
// config write or [Client.CreateScore], that failed after it was sent and
// may have been applied. The SDK never repeats such a write.
var ErrWriteOutcomeUnknown = errors.New("langfuse: write outcome unknown")

// restOperationBudget bounds each public REST API operation, including retries
// of a read.
const restOperationBudget = 30 * time.Second

type restGate struct {
	mu                  sync.Mutex
	closing             bool
	wg                  sync.WaitGroup
	newOperationContext func(parent context.Context) (context.Context, context.CancelFunc)
	cancelLifecycle     context.CancelFunc
	stop                <-chan struct{}
}

func newRESTGate() *restGate {
	lifecycle, cancel := context.WithCancel(context.Background())
	return &restGate{
		newOperationContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			operation, cancelOperation := context.WithTimeout(parent, restOperationBudget)
			detach := context.AfterFunc(lifecycle, cancelOperation)
			return operation, func() { detach(); cancelOperation() }
		},
		cancelLifecycle: cancel,
		stop:            lifecycle.Done(),
	}
}

var errRESTShutdown = errors.New("langfuse: API request after client shutdown")

// enter must follow Mask and serialization, so a callback that calls
// Shutdown cannot block the drain. Call release once the I/O has finished.
func (g *restGate) enter(parent context.Context) (ctx context.Context, release func(), err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return nil, nil, errRESTShutdown
	}
	g.wg.Add(1)
	ctx, cancel := g.newOperationContext(parent)
	return ctx, func() { cancel(); g.wg.Done() }, nil
}

func (g *restGate) lifecycleEnded() bool {
	select {
	case <-g.stop:
		return true
	default:
		return false
	}
}

func (g *restGate) beginShutdown() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closing = true
	g.mu.Unlock()
	g.cancelLifecycle()
}

func (g *restGate) shutdown(ctx context.Context) error {
	if g == nil {
		return nil
	}
	g.beginShutdown()
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("langfuse: API request shutdown: %w", ctx.Err())
	}
}

func (c *Client) restReady(ctx context.Context, invalid error) error {
	if ctx == nil {
		return errors.New("langfuse: request context is nil")
	}
	if invalid != nil {
		return invalid
	}
	return c.restUnavailable()
}

func (c *Client) restUnavailable() error {
	if c == nil || c.isDisabled() || c.rest == nil || c.restTransport == nil {
		return errors.New("langfuse: API request on a disabled client")
	}
	if c.stopped.Load() {
		return errRESTShutdown
	}
	return nil
}

func requireField(field, value string) error {
	if value == "" {
		return errors.New("langfuse: " + field + " is required")
	}
	return nil
}

// runREST maps a 404 to notFound unless it is nil.
func runREST[T any](
	ctx context.Context, c *Client, notFound error, call func(context.Context) (T, error),
) (T, error) {
	var zero T
	operationCtx, release, err := c.rest.enter(ctx)
	if err != nil {
		return zero, err
	}
	result, err := call(operationCtx)
	release()
	if err != nil {
		return zero, c.restFailure(ctx, notFound, err)
	}
	return result, nil
}

func (c *Client) restFailure(ctx context.Context, notFound, err error) error {
	var failure *transport.RESTError
	if !errors.As(err, &failure) {
		return errors.New("langfuse: API request failed")
	}
	if failure.NotFound && notFound != nil {
		return notFound
	}
	result := errors.New("langfuse: " + failure.Message)
	if failure.OutcomeUnknown {
		result = fmt.Errorf("%w: %s", ErrWriteOutcomeUnknown, failure.Message)
	}
	cause := failure.Cause // the operation budget expired, or nil
	switch {
	case ctx.Err() != nil:
		cause = ctx.Err()
	case c.rest.lifecycleEnded():
		cause = errRESTShutdown
	}
	if cause != nil {
		result = fmt.Errorf("%w: %w", result, cause)
	}
	return result
}

// numberedPages yields the values of fetch page by page until a short listing
// or the reported page count ends it.
func numberedPages[T any](
	ctx context.Context, c *Client, invalid, notFound error,
	fetch func(ctx context.Context, page int) ([]T, int, error),
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		if ctx == nil {
			yield(zero, errors.New("langfuse: request context is nil"))
			return
		}
		if invalid != nil {
			yield(zero, invalid)
			return
		}
		for page := 1; ; page++ {
			if err := c.restUnavailable(); err != nil {
				yield(zero, err)
				return
			}
			type pageResult struct {
				values []T
				total  int
			}
			result, err := runREST(ctx, c, notFound, func(ctx context.Context) (pageResult, error) {
				values, total, err := fetch(ctx, page)
				return pageResult{values, total}, err
			})
			if err != nil {
				yield(zero, err)
				return
			}
			for _, value := range result.values {
				if !yield(value, nil) {
					return
				}
			}
			// An empty page ends a listing whose page count overstates it.
			if len(result.values) == 0 || page >= result.total {
				return
			}
		}
	}
}

// marshalBody checks strings first because encoding/json silently
// replaces invalid UTF-8.
func marshalBody(body map[string]any, limit int, kind string) ([]byte, error) {
	for _, value := range body {
		if text, ok := value.(string); ok && !utf8.ValidString(text) {
			return nil, errors.New("langfuse: " + kind + " request contains invalid UTF-8")
		}
	}
	data, err := json.Marshal(body)
	if err != nil || !utf8.Valid(data) {
		return nil, errors.New("langfuse: " + kind + " request could not be serialized")
	}
	if len(data) > limit {
		return nil, fmt.Errorf("langfuse: %s request exceeds the %d MiB limit", kind, limit>>20)
	}
	return data, nil
}

// cursorPages yields the values of fetch page by page until a page has no
// next cursor. A repeated cursor ends the iteration with an error rather than
// looping.
func cursorPages[T any](
	ctx context.Context, c *Client, invalid error, query url.Values,
	fetch func(ctx context.Context, query url.Values) ([]T, string, error),
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		if ctx == nil {
			yield(zero, errors.New("langfuse: request context is nil"))
			return
		}
		if invalid != nil {
			yield(zero, invalid)
			return
		}
		request := cloneValues(query) // each traversal starts at the first page
		seen := map[string]bool{}
		for {
			if err := c.restUnavailable(); err != nil {
				yield(zero, err)
				return
			}
			type pageResult struct {
				values []T
				cursor string
			}
			result, err := runREST(ctx, c, nil, func(ctx context.Context) (pageResult, error) {
				values, cursor, err := fetch(ctx, request)
				return pageResult{values, cursor}, err
			})
			if err != nil {
				yield(zero, err)
				return
			}
			for _, value := range result.values {
				if !yield(value, nil) {
					return
				}
			}
			if result.cursor == "" {
				return
			}
			if seen[result.cursor] {
				yield(zero, errors.New("langfuse: listing repeated a page cursor"))
				return
			}
			seen[result.cursor] = true
			request.Set("cursor", result.cursor)
		}
	}
}

func cloneValues(values url.Values) url.Values {
	clone := make(url.Values, len(values))
	for key, value := range values {
		clone[key] = append([]string(nil), value...)
	}
	return clone
}

// setListFilters sets each non-empty filter as one comma-separated query
// parameter. Langfuse splits on commas and trims and drops blank members, so
// a value with a comma or surrounding whitespace, or a blank one, would
// change or remove the filter.
func setListFilters(values url.Values, filters map[string][]string) error {
	for key, filter := range filters {
		for _, value := range filter {
			if value == "" || strings.Contains(value, ",") || trimJS(value) != value {
				return errors.New("langfuse: query filter values must be non-empty, without commas or surrounding whitespace")
			}
		}
		if len(filter) != 0 {
			values.Set(key, strings.Join(filter, ","))
		}
	}
	return nil
}

// trimJS trims the whitespace that JavaScript's String.prototype.trim
// removes, which is how Langfuse trims names and lists.
func trimJS(text string) string {
	return strings.TrimFunc(text, func(r rune) bool { return r == '\uFEFF' || r != '\u0085' && unicode.IsSpace(r) })
}

// lengthJS returns the length of text in UTF-16 code units, the unit of
// Langfuse's length limits.
func lengthJS(text string) int {
	length := 0
	for _, r := range text {
		length += utf16.RuneLen(r)
	}
	return length
}

var (
	jsDecimal = regexp.MustCompile(`^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)
	jsInteger = map[string]int{"0x": 16, "0X": 16, "0o": 8, "0O": 8, "0b": 2, "0B": 2}
)

// finiteJSNumber reports whether JavaScript's Number(text) is finite for a
// text without surrounding whitespace, as Langfuse checks numeric filters.
func finiteJSNumber(text string) bool {
	if prefix := text[:min(2, len(text))]; jsInteger[prefix] != 0 {
		digits := text[2:]
		integer, ok := new(big.Int).SetString(digits, jsInteger[prefix])
		if !ok || digits == "" || strings.ContainsAny(digits, "+-_") {
			return false
		}
		number, _ := new(big.Float).SetInt(integer).Float64()
		return !math.IsInf(number, 0)
	}
	if !jsDecimal.MatchString(text) {
		return false
	}
	_, err := strconv.ParseFloat(text, 64)
	return err == nil
}
