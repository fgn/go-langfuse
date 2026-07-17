package langfuse_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
)

func newBufferingClient(t *testing.T, receiver *otlpreceiver.Receiver, change func(*langfuse.Config)) *langfuse.Client {
	t.Helper()
	config := langfuse.Config{
		BaseURL:   receiver.URL(),
		PublicKey: "pk-lf-buffering",
		SecretKey: "sk-lf-buffering",
	}
	if change != nil {
		change(&config)
	}
	client, err := langfuse.New(context.Background(), config)
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	return client
}

func deliveredSpanCount(receiver *otlpreceiver.Receiver) int {
	total := 0
	for _, request := range receiver.Requests() {
		total += len(otlpreceiver.Spans(request))
	}
	return total
}

func TestNewRejectsNegativeMaxQueueSize(t *testing.T) {
	t.Parallel()

	_, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:      "https://cloud.langfuse.com",
		PublicKey:    "pk-lf-negative-queue",
		SecretKey:    "sk-lf-negative-queue",
		MaxQueueSize: -1,
	})
	if err == nil {
		t.Fatal("New() error = nil, want negative MaxQueueSize validation failure")
	}
}

func TestBlockOnQueueFullBlocksEndAndLosesNoObservations(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	// Cleanups run last-in-first-out, so this Release runs before Close and
	// the stalled handlers cannot deadlock server shutdown.
	t.Cleanup(receiver.Release)
	receiver.Stall()

	client := newBufferingClient(t, receiver, func(config *langfuse.Config) {
		config.MaxQueueSize = 2
		config.BlockOnQueueFull = true
	})

	const total = 6
	observations := make([]*langfuse.Observation, 0, total)
	for index := range total {
		_, observation := client.StartObservation(context.Background(),
			fmt.Sprintf("blocked-%d", index), langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observations = append(observations, observation)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, observation := range observations {
			observation.End()
		}
	}()

	select {
	case <-done:
		t.Fatal("every End() returned against a stalled receiver with a full 2-span queue; want backpressure to block")
	case <-time.After(500 * time.Millisecond):
	}

	receiver.Release()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("End() calls stayed blocked after the receiver was released")
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if delivered := deliveredSpanCount(receiver); delivered == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered spans = %d, want all %d ended observations (zero loss)",
				deliveredSpanCount(receiver), total)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestDefaultDropOnQueueFullNeverBlocksEnd(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	t.Cleanup(receiver.Release)
	receiver.Stall()

	client := newBufferingClient(t, receiver, func(config *langfuse.Config) {
		config.MaxQueueSize = 2
	})

	const total = 20
	observations := make([]*langfuse.Observation, 0, total)
	for index := range total {
		_, observation := client.StartObservation(context.Background(),
			fmt.Sprintf("dropped-%d", index), langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observations = append(observations, observation)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, observation := range observations {
			observation.End()
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("End() blocked against a stalled receiver; the default must drop instead of blocking")
	}

	receiver.Release()
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	delivered := deliveredSpanCount(receiver)
	if delivered == 0 {
		t.Fatal("delivered spans = 0, want the buffered subset to arrive after release")
	}
	if delivered > total {
		t.Fatalf("delivered spans = %d, want at most the %d ended observations", delivered, total)
	}
}

func TestShutdownReturnsToBaselineGoroutineCount(t *testing.T) {
	receiver := otlpreceiver.New()

	baseline := runtime.NumGoroutine()

	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:   receiver.URL(),
		PublicKey: "pk-lf-goroutines",
		SecretKey: "sk-lf-goroutines",
	})
	if err != nil {
		receiver.Close()
		t.Fatalf("langfuse.New() error = %v", err)
	}
	for index := range 5 {
		_, observation := client.StartObservation(context.Background(),
			fmt.Sprintf("hygiene-%d", index), langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()
	}
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	flushErr := client.Flush(flushCtx)
	cancelFlush()
	if flushErr != nil {
		t.Errorf("Flush() error = %v", flushErr)
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := client.Shutdown(shutdownCtx)
	cancelShutdown()
	if shutdownErr != nil {
		t.Errorf("Shutdown() error = %v", shutdownErr)
	}
	// Closing the receiver terminates kept-alive idle connections so the HTTP
	// transport's per-connection goroutines can exit.
	receiver.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if current := runtime.NumGoroutine(); current <= baseline+2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want at most baseline %d + 2 after Shutdown", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
