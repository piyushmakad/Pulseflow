package worker

import (
	"context"
	"fmt"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type MaintenanceStore interface {
	RecoverExpiredDeliveries(context.Context, int, string) ([]domain.DeliveryAttempt, error)
	FinalizeReadyEvents(context.Context, int) (int64, error)
}

type Maintenance struct {
	store            MaintenanceStore
	deadLetterTopic  string
	batchSize        int
	recoveryInterval time.Duration
	finalizeInterval time.Duration
	logger           *logger.Logger
}

func NewMaintenance(store MaintenanceStore, deadLetterTopic string, batchSize int, recoveryInterval, finalizeInterval time.Duration, log *logger.Logger) (*Maintenance, error) {
	if store == nil || deadLetterTopic == "" || batchSize < 1 || recoveryInterval <= 0 || finalizeInterval <= 0 || log == nil {
		return nil, fmt.Errorf("maintenance dependencies and positive configuration are required")
	}
	return &Maintenance{store: store, deadLetterTopic: deadLetterTopic, batchSize: batchSize,
		recoveryInterval: recoveryInterval, finalizeInterval: finalizeInterval, logger: log}, nil
}

func (m *Maintenance) Run(ctx context.Context) error {
	recoveryTicker := time.NewTicker(m.recoveryInterval)
	finalizeTicker := time.NewTicker(m.finalizeInterval)
	defer recoveryTicker.Stop()
	defer finalizeTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-recoveryTicker.C:
			recovered, err := m.store.RecoverExpiredDeliveries(ctx, m.batchSize, m.deadLetterTopic)
			if err != nil && ctx.Err() == nil {
				m.logger.Error("expired delivery recovery failed", "error", err)
			} else if len(recovered) > 0 {
				m.logger.Warn("expired delivery leases recovered", "count", len(recovered))
			}
		case <-finalizeTicker.C:
			count, err := m.store.FinalizeReadyEvents(ctx, m.batchSize)
			if err != nil && ctx.Err() == nil {
				m.logger.Error("event finalization sweep failed", "error", err)
			} else if count > 0 {
				m.logger.Info("events finalized by recovery sweep", "count", count)
			}
		}
	}
}
