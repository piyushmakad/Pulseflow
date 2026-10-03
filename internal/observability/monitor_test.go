package observability

import (
	"context"
	"testing"
	"time"

	"pulseflow/internal/platform/logger"
)

type snapshotStore struct {
	snapshot DatabaseSnapshot
	err      error
}

func (s *snapshotStore) OperationalSnapshot(context.Context) (DatabaseSnapshot, error) {
	return s.snapshot, s.err
}

func TestMonitorAlertsOnlyChangeOnThresholdTransitions(t *testing.T) {
	store := &snapshotStore{snapshot: DatabaseSnapshot{
		OutboxPending: 10, OldestOutboxAgeSeconds: 120, ExpiredOutboxLeases: 1, DueDeliveries: 20,
		ExpiredDeliveryLeases: 2, PostgresAcquired: 9, PostgresMax: 10,
	}}
	monitor, err := NewMonitor(store, NewRegistry(), MonitorConfig{
		Interval: time.Second, QueryTimeout: time.Second,
		OutboxPendingAlert: 10, OutboxOldestAgeAlert: time.Minute,
		ExpiredOutboxLeaseAlert: 1,
		DueDeliveriesAlert:      20, ExpiredDeliveryLeaseAlert: 1,
		PostgresPoolAlertRatio: 0.9,
	}, logger.New("error", "text"))
	if err != nil {
		t.Fatal(err)
	}
	monitor.collect(context.Background())
	for _, name := range []string{"outbox_pending", "outbox_oldest_age", "expired_outbox_leases", "due_deliveries", "expired_delivery_leases", "postgres_pool_saturation"} {
		if !monitor.firing[name] {
			t.Fatalf("alert %q is not firing", name)
		}
	}

	store.snapshot = DatabaseSnapshot{PostgresAcquired: 1, PostgresMax: 10}
	monitor.collect(context.Background())
	for name, firing := range monitor.firing {
		if firing {
			t.Fatalf("alert %q did not recover", name)
		}
	}
}

func TestMonitorTracksCollectionFailureAndRecovery(t *testing.T) {
	store := &snapshotStore{err: context.DeadlineExceeded}
	monitor, err := NewMonitor(store, NewRegistry(), MonitorConfig{
		Interval: time.Second, QueryTimeout: time.Second, PostgresPoolAlertRatio: 0.9,
	}, logger.New("error", "text"))
	if err != nil {
		t.Fatal(err)
	}
	monitor.collect(context.Background())
	if !monitor.firing["operational_metrics_collection"] {
		t.Fatal("collection alert is not firing")
	}
	store.err = nil
	monitor.collect(context.Background())
	if monitor.firing["operational_metrics_collection"] {
		t.Fatal("collection alert did not recover")
	}
}
