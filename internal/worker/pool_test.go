package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"pulseflow/internal/domain"
)

func TestPoolBoundsConcurrencyAndDrains(t *testing.T) {
	const workers = 3
	var active atomic.Int32
	var maximum atomic.Int32
	var completed atomic.Int32
	release := make(chan struct{})
	handler := func(ctx context.Context, delivery domain.DeliveryAttempt) {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		active.Add(-1)
		completed.Add(1)
	}
	pool, err := NewPool(workers, 12, time.Second, handler)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if err := pool.Submit(context.Background(), domain.DeliveryAttempt{ID: string(rune(i))}); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	if err := pool.CloseAndDrain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() > workers {
		t.Fatalf("maximum concurrency = %d, want <= %d", maximum.Load(), workers)
	}
	if completed.Load() != 12 {
		t.Fatalf("completed = %d, want 12", completed.Load())
	}
}

func TestPoolCancelsWorkAfterDrainDeadline(t *testing.T) {
	started := make(chan struct{})
	handler := func(ctx context.Context, delivery domain.DeliveryAttempt) {
		close(started)
		<-ctx.Done()
	}
	pool, err := NewPool(1, 1, time.Hour, handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(context.Background(), domain.DeliveryAttempt{}); err != nil {
		t.Fatal(err)
	}
	<-started
	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pool.CloseAndDrain(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain error = %v, want deadline exceeded", err)
	}
}
