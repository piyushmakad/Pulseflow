package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"pulseflow/internal/domain"
)

func TestEventLifecycleIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()

	tenant, err := store.CreateTenant(ctx, "integration tenant", json.RawMessage(`{"tier":"test"}`))
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, err = store.CreateNotificationRule(ctx, CreateNotificationRuleParams{
		TenantID: tenant.ID, EventType: "order.created", Channel: "webhook",
		Config: json.RawMessage(`{"url":"https://example.invalid/hook"}`),
	})
	if err != nil {
		t.Fatalf("create rule: %v", err)
	}

	params := CreateEventParams{
		TenantID: tenant.ID, Type: "order.created", IdempotencyKey: "order-123",
		Data: json.RawMessage(`{"order_id":"123"}`), Topic: "events.ingested",
	}
	event, created, err := store.CreateEvent(ctx, params)
	if err != nil || !created {
		t.Fatalf("create event: created=%v err=%v", created, err)
	}
	duplicate, created, err := store.CreateEvent(ctx, params)
	if err != nil || created || duplicate.ID != event.ID {
		t.Fatalf("idempotent create: created=%v event=%s err=%v", created, duplicate.ID, err)
	}
	conflict := params
	conflict.Data = json.RawMessage(`{"order_id":"different"}`)
	if _, _, err := store.CreateEvent(ctx, conflict); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}

	eventMessage := domain.EventMessage{
		Version: domain.EventMessageVersion, EventID: event.ID, TenantID: event.TenantID,
		Type: event.Type, Data: event.Data, OccurredAt: event.CreatedAt,
	}
	deliveries, err := store.RouteEventMessage(ctx, eventMessage, 5)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("route event: deliveries=%d err=%v", len(deliveries), err)
	}
	deliveries, err = store.RouteEventMessage(ctx, eventMessage, 5)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("idempotent route: deliveries=%d err=%v", len(deliveries), err)
	}

	claimed, err := store.ClaimDueDeliveries(ctx, "test-worker", 10, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].Status != domain.DeliveryStatusDelivering {
		t.Fatalf("claim delivery: deliveries=%d err=%v", len(claimed), err)
	}
	status := 200
	_, err = store.RecordDeliveryResult(ctx, claimed[0].ID, "test-worker", domain.DeliveryResult{
		Status: domain.DeliveryStatusDelivered, ResponseStatus: &status,
	})
	if err != nil {
		t.Fatalf("record result: %v", err)
	}

	finalStatus, finalized, err := store.FinalizeEvent(ctx, event.ID)
	if err != nil || !finalized || finalStatus != domain.EventStatusCompleted {
		t.Fatalf("finalize: status=%s finalized=%v err=%v", finalStatus, finalized, err)
	}

	outbox, err := store.ClaimOutbox(ctx, "test-relay", 10, time.Minute)
	if err != nil || len(outbox) != 1 {
		t.Fatalf("claim outbox: rows=%d err=%v", len(outbox), err)
	}
	count, err := store.MarkOutboxPublished(ctx, "test-relay", []int64{outbox[0].ID})
	if err != nil || count != 1 {
		t.Fatalf("mark outbox: count=%d err=%v", count, err)
	}

	quarantineParams := domain.QuarantineMessageParams{
		SourceTopic: "events.ingested", SourcePartition: 3, SourceOffset: 42,
		MessageKey: []byte("unknown"), Payload: []byte(`not-json`),
		ErrorMessage: "invalid character", DeadLetterTopic: "events.deadletter",
	}
	quarantined, created, err := store.QuarantineMessage(ctx, quarantineParams)
	if err != nil || !created || quarantined.SourceOffset != 42 {
		t.Fatalf("quarantine message: created=%v message=%+v err=%v", created, quarantined, err)
	}
	duplicateQuarantine, created, err := store.QuarantineMessage(ctx, quarantineParams)
	if err != nil || created || duplicateQuarantine.ID != quarantined.ID {
		t.Fatalf("idempotent quarantine: created=%v message=%+v err=%v", created, duplicateQuarantine, err)
	}
	deadLetters, err := store.ClaimOutbox(ctx, "dead-letter-relay", 10, time.Minute)
	if err != nil || len(deadLetters) != 1 || deadLetters[0].Topic != "events.deadletter" {
		t.Fatalf("claim dead-letter outbox: rows=%d err=%v", len(deadLetters), err)
	}
}

func TestConcurrentOutboxClaimsIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	tenant, err := store.CreateTenant(ctx, "claim tenant", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	for i := 0; i < 10; i++ {
		_, created, err := store.CreateEvent(ctx, CreateEventParams{
			TenantID: tenant.ID, Type: "claim.test", IdempotencyKey: fmt.Sprintf("claim-%d", i),
			Data: json.RawMessage(`{"ok":true}`), Topic: "events.ingested",
		})
		if err != nil || !created {
			t.Fatalf("create event %d: created=%v err=%v", i, created, err)
		}
	}

	type claimResult struct {
		entries []domain.OutboxEntry
		err     error
	}
	results := make(chan claimResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	for _, owner := range []string{"relay-a", "relay-b"} {
		owner := owner
		go func() {
			ready.Done()
			<-start
			entries, err := store.ClaimOutbox(ctx, owner, 6, time.Minute)
			results <- claimResult{entries: entries, err: err}
		}()
	}
	ready.Wait()
	close(start)

	seen := make(map[int64]struct{}, 10)
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("claim outbox: %v", result.err)
		}
		for _, entry := range result.entries {
			if _, duplicate := seen[entry.ID]; duplicate {
				t.Fatalf("outbox row %d was claimed by both relays", entry.ID)
			}
			seen[entry.ID] = struct{}{}
		}
	}
	if len(seen) != 10 {
		t.Fatalf("expected 10 uniquely claimed rows, got %d", len(seen))
	}
}

func integrationStore(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	schema := fmt.Sprintf("pulseflow_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}

	migrationPattern := filepath.Join("..", "..", "..", "migrations", "*.up.sql")
	migrationPaths, err := filepath.Glob(migrationPattern)
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(migrationPaths) == 0 {
		t.Fatalf("no migrations matched %s", migrationPattern)
	}
	for _, migrationPath := range migrationPaths {
		migration, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf("read migration %s: %v", migrationPath, err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", migrationPath, err)
		}
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return NewWithPool(pool)
}
