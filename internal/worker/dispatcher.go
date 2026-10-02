package worker

import (
	"context"
	"fmt"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type DeliveryClaimer interface {
	ClaimDueDeliveriesByChannel(context.Context, string, string, int, time.Duration) ([]domain.DeliveryAttempt, error)
}

type Dispatcher struct {
	store        DeliveryClaimer
	pool         *Pool
	owner        string
	channel      string
	batchSize    int
	lease        time.Duration
	pollInterval time.Duration
	logger       *logger.Logger
}

func NewDispatcher(store DeliveryClaimer, pool *Pool, owner, channel string, batchSize int, lease, pollInterval time.Duration, log *logger.Logger) (*Dispatcher, error) {
	if store == nil || pool == nil || owner == "" || channel == "" || batchSize < 1 || lease <= 0 || pollInterval <= 0 || log == nil {
		return nil, fmt.Errorf("dispatcher dependencies and positive configuration are required")
	}
	return &Dispatcher{store: store, pool: pool, owner: owner, channel: channel,
		batchSize: batchSize, lease: lease, pollInterval: pollInterval, logger: log}, nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		available := d.pool.Available()
		if available > 0 {
			limit := min(available, d.batchSize)
			deliveries, err := d.store.ClaimDueDeliveriesByChannel(ctx, d.owner, d.channel, limit, d.lease)
			if err != nil && ctx.Err() == nil {
				d.logger.Error("delivery claim failed", "channel", d.channel, "error", err)
			} else {
				for _, delivery := range deliveries {
					if err := d.pool.Submit(ctx, delivery); err != nil {
						return nil
					}
				}
				if len(deliveries) == limit {
					continue
				}
			}
		}
		if !waitFor(ctx, d.pollInterval) {
			return nil
		}
	}
}

func waitFor(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
