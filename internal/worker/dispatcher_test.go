package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type recordingClaimer struct {
	mu       sync.Mutex
	claimed  int
	maxLimit int
}

func (s *recordingClaimer) ClaimDueDeliveriesByChannel(ctx context.Context, owner, channel string, limit int, lease time.Duration) ([]domain.DeliveryAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit > s.maxLimit {
		s.maxLimit = limit
	}
	s.claimed++
	return []domain.DeliveryAttempt{{ID: "delivery", Channel: channel}}, nil
}

func TestDispatcherStopsClaimingWhenPoolBufferIsFull(t *testing.T) {
	release := make(chan struct{})
	pool, err := NewPool(1, 1, time.Second, func(ctx context.Context, delivery domain.DeliveryAttempt) {
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingClaimer{}
	dispatcher, err := NewDispatcher(store, pool, "owner", domain.NotificationChannelWebhook, 100, time.Minute, 2*time.Millisecond, logger.New("error", "text"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = dispatcher.Run(ctx)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done

	store.mu.Lock()
	claimed, maxLimit := store.claimed, store.maxLimit
	store.mu.Unlock()
	if claimed > 2 {
		t.Fatalf("claimed %d deliveries with one active and one buffered slot", claimed)
	}
	if maxLimit > 1 {
		t.Fatalf("claim limit = %d, want <= available buffer size 1", maxLimit)
	}
	close(release)
	if err := pool.CloseAndDrain(context.Background()); err != nil {
		t.Fatal(err)
	}
}
