package wiretap

import (
	"io"
	"net/http"
	"sync"
)

// requestRecorder captures the request body actually transmitted to the
// provider. The transport clones the request and replaces both Body and,
// when present, GetBody, so transparent net/http replays (HTTP/1 rewind,
// HTTP/2 connection-loss recovery) are also captured: each replay starts
// a new capture generation that replaces the previous transmission's
// bytes. All state is mutex-guarded because transports may read and
// close request bodies asynchronously.
type requestRecorder struct {
	mu       sync.Mutex
	buf      []byte
	cap      int
	overCap  bool
	closed   bool // the current generation's body reached Close or EOF
	active   bool // a generation exists
	disabled bool // capture abandoned (over cap or finalized)
}

// instrument replaces body and GetBody on the already-cloned request.
// nil bodies and http.NoBody are preserved exactly and record nothing.
func (r *requestRecorder) instrument(clone *http.Request) {
	if clone.Body == nil || clone.Body == http.NoBody {
		return
	}
	clone.Body = r.newGeneration(clone.Body)
	if original := clone.GetBody; original != nil {
		clone.GetBody = func() (io.ReadCloser, error) {
			replay, err := original()
			if err != nil {
				return nil, err
			}
			return r.newGeneration(replay), nil
		}
	}
}

// newGeneration resets capture state for a (re)transmission and wraps
// its reader. The newest generation's bytes replace older ones, so the
// capture always describes the transmission the provider accepted.
func (r *requestRecorder) newGeneration(rc io.ReadCloser) io.ReadCloser {
	r.mu.Lock()
	r.buf = r.buf[:0]
	r.overCap = false
	r.closed = false
	r.active = true
	r.mu.Unlock()
	return &recorderBody{rec: r, rc: rc}
}

func (r *requestRecorder) observe(p []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.disabled || r.overCap {
		return
	}
	if len(r.buf)+len(p) > r.cap {
		// Drop, never truncate: retention stops but forwarding
		// continues unchanged elsewhere.
		r.overCap = true
		r.buf = nil
		return
	}
	r.buf = append(r.buf, p...)
}

func (r *requestRecorder) markClosed() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
}

// snapshot returns the captured body when, and only when, the current
// generation was completely transmitted (closed or fully read) and the
// capture stayed within its cap. ok is false otherwise; the caller then
// omits input rather than parsing a racy or partial capture.
func (r *requestRecorder) snapshot() (body []byte, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active || !r.closed || r.overCap || r.disabled {
		r.disabled = true
		r.buf = nil
		return nil, false
	}
	r.disabled = true
	captured := r.buf
	r.buf = nil
	return captured, true
}

// overCapped reports whether capture was abandoned for size, for the
// omission diagnostic.
func (r *requestRecorder) overCapped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.overCap
}

// recorderBody forwards reads with exact (n, err) passthrough. EOF also
// marks the generation complete: transports may fully read a body
// without a separate Close, and http.Request.Body owners may close
// asynchronously afterwards.
type recorderBody struct {
	rec *requestRecorder
	rc  io.ReadCloser
}

func (b *recorderBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.rec.observe(p[:n])
	}
	if err == io.EOF {
		b.rec.markClosed()
	}
	return n, err
}

func (b *recorderBody) Close() error {
	err := b.rc.Close()
	b.rec.markClosed()
	return err
}
