package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"

	"pulseflow/internal/domain"
)

type CreateEventParams struct {
	TenantID       string
	Type           string
	IdempotencyKey string
	Data           json.RawMessage
	Topic          string
}

// CreateEvent writes the event and its Kafka outbox message atomically.
// The bool result is true only when a new event was created.
func (s *Store) CreateEvent(ctx context.Context, p CreateEventParams) (domain.Event, bool, error) {
	if strings.TrimSpace(p.TenantID) == "" || strings.TrimSpace(p.Type) == "" || strings.TrimSpace(p.IdempotencyKey) == "" {
		return domain.Event{}, false, domain.NewValidationError("event", "tenant_id, type, and idempotency_key are required")
	}
	if len(p.Type) > 255 || len(p.IdempotencyKey) > 255 || len(p.Topic) > 255 {
		return domain.Event{}, false, domain.NewValidationError("event", "type, idempotency_key, and topic must be at most 255 characters")
	}
	if !json.Valid(p.Data) {
		return domain.Event{}, false, domain.NewValidationError("data", "must be valid JSON")
	}
	if strings.TrimSpace(p.Topic) == "" {
		return domain.Event{}, false, domain.NewValidationError("topic", "is required")
	}

	var event domain.Event
	created := false
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		err := scanEvent(tx.QueryRow(ctx, `
			INSERT INTO events (tenant_id, type, idempotency_key, data)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
			RETURNING id, tenant_id, type, idempotency_key, data, status,
			          created_at, updated_at, processed_at`,
			p.TenantID, p.Type, p.IdempotencyKey, p.Data,
		), &event)
		if errors.Is(err, pgx.ErrNoRows) {
			if err := getEventByIdempotencyKey(ctx, tx, p.TenantID, p.IdempotencyKey, &event); err != nil {
				return err
			}
			if event.Type != p.Type || !jsonEquivalent(event.Data, p.Data) {
				return fmt.Errorf("%w: idempotency key is already used for a different event", domain.ErrAlreadyExists)
			}
			return nil
		}
		if err != nil {
			return mapError(err)
		}

		created = true
		payload, err := json.Marshal(domain.EventMessage{
			Version:    1,
			EventID:    event.ID,
			TenantID:   event.TenantID,
			Type:       event.Type,
			Data:       event.Data,
			OccurredAt: event.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("marshal event outbox payload: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (aggregate_id, topic, partition_key, payload)
			VALUES ($1, $2, $3, $4)`, event.ID, p.Topic, event.TenantID, payload)
		return mapError(err)
	})
	if err != nil {
		return domain.Event{}, false, err
	}
	return event, created, nil
}

func (s *Store) GetEvent(ctx context.Context, tenantID, eventID string) (domain.Event, error) {
	var event domain.Event
	err := scanEvent(s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, type, idempotency_key, data, status,
		       created_at, updated_at, processed_at
		FROM events
		WHERE tenant_id=$1 AND id=$2`, tenantID, eventID), &event)
	if err != nil {
		return domain.Event{}, mapError(err)
	}
	return event, nil
}

func (s *Store) TransitionEvent(ctx context.Context, eventID string, from, to domain.EventStatus) (domain.Event, error) {
	if err := from.ValidateTransition(to); err != nil {
		return domain.Event{}, err
	}

	var event domain.Event
	err := scanEvent(s.pool.QueryRow(ctx, `
		UPDATE events
		SET status=$3,
		    updated_at=now(),
		    processed_at=CASE WHEN $3 IN ('completed', 'partially_failed', 'failed') THEN now() ELSE processed_at END
		WHERE id=$1 AND status=$2
		RETURNING id, tenant_id, type, idempotency_key, data, status,
		          created_at, updated_at, processed_at`, eventID, from, to), &event)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Event{}, domain.ErrInvalidStateTransition
	}
	if err != nil {
		return domain.Event{}, mapError(err)
	}
	return event, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(row rowScanner, event *domain.Event) error {
	return row.Scan(
		&event.ID,
		&event.TenantID,
		&event.Type,
		&event.IdempotencyKey,
		&event.Data,
		&event.Status,
		&event.CreatedAt,
		&event.UpdatedAt,
		&event.ProcessedAt,
	)
}

func getEventByIdempotencyKey(ctx context.Context, tx pgx.Tx, tenantID, key string, event *domain.Event) error {
	return scanEvent(tx.QueryRow(ctx, `
		SELECT id, tenant_id, type, idempotency_key, data, status,
		       created_at, updated_at, processed_at
		FROM events
		WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key), event)
}

func jsonEquivalent(left, right json.RawMessage) bool {
	var a any
	var b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}
