package observability

import (
	"context"
	"fmt"
	"time"

	"pulseflow/internal/platform/logger"
)

type DatabaseSnapshot struct {
	OutboxPending          int64   `json:"outbox_pending"`
	OutboxPublishing       int64   `json:"outbox_publishing"`
	ExpiredOutboxLeases    int64   `json:"expired_outbox_leases"`
	OldestOutboxAgeSeconds float64 `json:"oldest_outbox_age_seconds"`
	DueDeliveries          int64   `json:"due_deliveries"`
	ExpiredDeliveryLeases  int64   `json:"expired_delivery_leases"`
	PostgresAcquired       int32   `json:"postgres_acquired"`
	PostgresIdle           int32   `json:"postgres_idle"`
	PostgresTotal          int32   `json:"postgres_total"`
	PostgresMax            int32   `json:"postgres_max"`
	PostgresAcquireWaitMs  int64   `json:"postgres_acquire_wait_ms"`
}

type SnapshotStore interface {
	OperationalSnapshot(context.Context) (DatabaseSnapshot, error)
}

type MonitorConfig struct {
	Interval                  time.Duration
	QueryTimeout              time.Duration
	OutboxPendingAlert        int64
	OutboxOldestAgeAlert      time.Duration
	ExpiredOutboxLeaseAlert   int64
	DueDeliveriesAlert        int64
	ExpiredDeliveryLeaseAlert int64
	PostgresPoolAlertRatio    float64
}

// Monitor logs backend-neutral operational snapshots and emits alerts only
// when a condition starts firing or recovers.
type Monitor struct {
	store    SnapshotStore
	registry *Registry
	config   MonitorConfig
	logger   *logger.Logger
	firing   map[string]bool
}

func NewMonitor(store SnapshotStore, registry *Registry, cfg MonitorConfig, log *logger.Logger) (*Monitor, error) {
	if store == nil || registry == nil || log == nil || cfg.Interval <= 0 || cfg.QueryTimeout <= 0 {
		return nil, fmt.Errorf("observability store, registry, logger, interval, and query timeout are required")
	}
	if cfg.PostgresPoolAlertRatio < 0 || cfg.PostgresPoolAlertRatio > 1 {
		return nil, fmt.Errorf("PostgreSQL pool alert ratio must be within [0, 1]")
	}
	return &Monitor{store: store, registry: registry, config: cfg, logger: log, firing: make(map[string]bool)}, nil
}

func (m *Monitor) Run(ctx context.Context) error {
	m.collect(ctx)
	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.collect(ctx)
		}
	}
}

func (m *Monitor) collect(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, m.config.QueryTimeout)
	defer cancel()
	database, err := m.store.OperationalSnapshot(ctx)
	if err != nil {
		m.transitionAlert("operational_metrics_collection", true, "error", err)
		return
	}
	m.transitionAlert("operational_metrics_collection", false)
	runtime := m.registry.Snapshot()
	m.logger.Info("operational metrics", "database", database, "runtime", runtime)

	m.transitionAlert("outbox_pending", m.config.OutboxPendingAlert > 0 && database.OutboxPending >= m.config.OutboxPendingAlert,
		"value", database.OutboxPending, "threshold", m.config.OutboxPendingAlert)
	m.transitionAlert("outbox_oldest_age", m.config.OutboxOldestAgeAlert > 0 && database.OldestOutboxAgeSeconds >= m.config.OutboxOldestAgeAlert.Seconds(),
		"value_seconds", database.OldestOutboxAgeSeconds, "threshold_seconds", m.config.OutboxOldestAgeAlert.Seconds())
	m.transitionAlert("expired_outbox_leases", m.config.ExpiredOutboxLeaseAlert > 0 && database.ExpiredOutboxLeases >= m.config.ExpiredOutboxLeaseAlert,
		"value", database.ExpiredOutboxLeases, "threshold", m.config.ExpiredOutboxLeaseAlert)
	m.transitionAlert("due_deliveries", m.config.DueDeliveriesAlert > 0 && database.DueDeliveries >= m.config.DueDeliveriesAlert,
		"value", database.DueDeliveries, "threshold", m.config.DueDeliveriesAlert)
	m.transitionAlert("expired_delivery_leases", m.config.ExpiredDeliveryLeaseAlert > 0 && database.ExpiredDeliveryLeases >= m.config.ExpiredDeliveryLeaseAlert,
		"value", database.ExpiredDeliveryLeases, "threshold", m.config.ExpiredDeliveryLeaseAlert)
	ratio := 0.0
	if database.PostgresMax > 0 {
		ratio = float64(database.PostgresAcquired) / float64(database.PostgresMax)
	}
	m.transitionAlert("postgres_pool_saturation", m.config.PostgresPoolAlertRatio > 0 && ratio >= m.config.PostgresPoolAlertRatio,
		"value", ratio, "threshold", m.config.PostgresPoolAlertRatio,
		"acquired", database.PostgresAcquired, "max", database.PostgresMax)
}

func (m *Monitor) transitionAlert(name string, nowFiring bool, attributes ...any) {
	wasFiring := m.firing[name]
	if nowFiring == wasFiring {
		return
	}
	m.firing[name] = nowFiring
	if nowFiring {
		m.logger.Warn("operational alert firing", append([]any{"alert", name}, attributes...)...)
		return
	}
	m.logger.Info("operational alert recovered", "alert", name)
}
