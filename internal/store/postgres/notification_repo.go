package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"pulseflow/internal/domain"
)

type CreateNotificationRuleParams struct {
	TenantID  string
	EventType string
	Channel   string
	Config    json.RawMessage
}

func (s *Store) CreateNotificationRule(ctx context.Context, p CreateNotificationRuleParams) (domain.NotificationRule, error) {
	if p.TenantID == "" || p.EventType == "" || p.Channel == "" || !json.Valid(p.Config) {
		return domain.NotificationRule{}, domain.NewValidationError("notification_rule", "tenant_id, event_type, channel, and valid config are required")
	}
	if p.Channel != domain.NotificationChannelWebhook && p.Channel != domain.NotificationChannelEmail {
		return domain.NotificationRule{}, domain.NewValidationError("channel", "must be webhook or email")
	}
	var rule domain.NotificationRule
	err := s.pool.QueryRow(ctx, `
		INSERT INTO notification_rules (tenant_id, event_type, channel, config)
		VALUES ($1, $2, $3, $4)
		RETURNING id, tenant_id, event_type, channel, config, enabled, created_at, updated_at`,
		p.TenantID, p.EventType, p.Channel, p.Config,
	).Scan(&rule.ID, &rule.TenantID, &rule.EventType, &rule.Channel, &rule.Config, &rule.Enabled, &rule.CreatedAt, &rule.UpdatedAt)
	if err != nil {
		return domain.NotificationRule{}, mapError(err)
	}
	return rule, nil
}

func (s *Store) ListNotificationRules(ctx context.Context, tenantID, eventType string) ([]domain.NotificationRule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, event_type, channel, config, enabled, created_at, updated_at
		FROM notification_rules
		WHERE tenant_id=$1 AND enabled=true AND (event_type=$2 OR event_type='*')
		ORDER BY created_at, id`, tenantID, eventType)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	return collectRules(rows)
}

// RouteEvent performs TX 2 from the architecture. It claims an accepted event
// and creates one durable logical delivery per matching rule. Redelivery is
// safe: processing/terminal events return their existing delivery rows.
func (s *Store) RouteEvent(ctx context.Context, eventID string, maxAttempts int) ([]domain.DeliveryAttempt, error) {
	return s.routeEvent(ctx, eventID, nil, maxAttempts)
}

// RouteEventMessage verifies that the message agrees with the immutable event
// stored in PostgreSQL before it creates delivery work. This prevents a valid
// JSON message with a forged tenant, type, payload, or timestamp from routing.
func (s *Store) RouteEventMessage(ctx context.Context, message domain.EventMessage, maxAttempts int) ([]domain.DeliveryAttempt, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return s.routeEvent(ctx, message.EventID, &message, maxAttempts)
}

func (s *Store) routeEvent(ctx context.Context, eventID string, expected *domain.EventMessage, maxAttempts int) ([]domain.DeliveryAttempt, error) {
	if maxAttempts < 1 {
		return nil, domain.NewValidationError("max_attempts", "must be positive")
	}

	var attempts []domain.DeliveryAttempt
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var event domain.Event
		if err := scanEvent(tx.QueryRow(ctx, `
			SELECT id, tenant_id, type, idempotency_key, data, status,
			       created_at, updated_at, processed_at
			FROM events WHERE id=$1 FOR UPDATE`, eventID), &event); err != nil {
			return mapError(err)
		}
		if expected != nil && (event.TenantID != expected.TenantID ||
			event.Type != expected.Type ||
			!jsonEquivalent(event.Data, expected.Data) ||
			!event.CreatedAt.Equal(expected.OccurredAt)) {
			return domain.NewValidationError("event_message", "does not match the persisted event")
		}

		if event.Status != domain.EventStatusAccepted {
			var err error
			attempts, err = listDeliveryAttemptsTx(ctx, tx, event.ID)
			return err
		}

		if _, err := tx.Exec(ctx, `
			UPDATE events SET status='processing', updated_at=now()
			WHERE id=$1 AND status='accepted'`, event.ID); err != nil {
			return mapError(err)
		}

		rules, err := listRulesTx(ctx, tx, event.TenantID, event.Type)
		if err != nil {
			return err
		}
		if len(rules) == 0 {
			_, err := tx.Exec(ctx, `
				UPDATE events
				SET status='completed', processed_at=now(), updated_at=now()
				WHERE id=$1 AND status='processing'`, event.ID)
			return mapError(err)
		}

		for _, rule := range rules {
			_, err := tx.Exec(ctx, `
				INSERT INTO delivery_attempts
				    (event_id, notification_rule_id, tenant_id, channel, max_attempts,
				     request_payload, destination_config)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (event_id, notification_rule_id) DO NOTHING`,
				event.ID, rule.ID, event.TenantID, rule.Channel, maxAttempts, event.Data, rule.Config)
			if err != nil {
				return mapError(err)
			}
		}

		attempts, err = listDeliveryAttemptsTx(ctx, tx, event.ID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}

func (s *Store) ListDeliveryAttempts(ctx context.Context, eventID string) ([]domain.DeliveryAttempt, error) {
	rows, err := s.pool.Query(ctx, deliverySelect+` WHERE event_id=$1 ORDER BY created_at, id`, eventID)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	return collectDeliveries(rows)
}

func (s *Store) ClaimDueDeliveries(ctx context.Context, owner string, batchSize int, lease time.Duration) ([]domain.DeliveryAttempt, error) {
	return s.claimDueDeliveries(ctx, owner, "", batchSize, lease)
}

// ClaimDueDeliveriesByChannel claims only work that can be accepted by the
// named channel pool. This is the database side of worker-pool backpressure.
func (s *Store) ClaimDueDeliveriesByChannel(ctx context.Context, owner, channel string, batchSize int, lease time.Duration) ([]domain.DeliveryAttempt, error) {
	if channel != domain.NotificationChannelWebhook && channel != domain.NotificationChannelEmail {
		return nil, domain.NewValidationError("channel", "must be webhook or email")
	}
	return s.claimDueDeliveries(ctx, owner, channel, batchSize, lease)
}

func (s *Store) claimDueDeliveries(ctx context.Context, owner, channel string, batchSize int, lease time.Duration) ([]domain.DeliveryAttempt, error) {
	if owner == "" || batchSize < 1 || lease <= 0 {
		return nil, domain.NewValidationError("delivery_claim", "owner, positive batch size, and positive lease are required")
	}

	var attempts []domain.DeliveryAttempt
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH candidates AS (
				SELECT id
				FROM delivery_attempts
				WHERE attempt_number < max_attempts
				  AND ($4 = '' OR channel=$4)
				  AND (
				      status='pending'
				      OR (status='retrying' AND next_retry_at <= now())
				  )
				ORDER BY COALESCE(next_retry_at, created_at), created_at, id
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			)
			UPDATE delivery_attempts AS d
			SET status='delivering', locked_by=$2,
			    locked_until=now() + ($3 * interval '1 millisecond'), updated_at=now()
			FROM candidates
			WHERE d.id=candidates.id
			RETURNING d.id, d.event_id, d.notification_rule_id, d.tenant_id,
			          d.channel, d.status, d.attempt_number, d.max_attempts,
			          d.next_retry_at, d.locked_by, d.locked_until,
			          d.request_payload, d.destination_config,
			          d.response_status, d.response_body,
			          d.error_message, d.duration_ms, d.created_at, d.updated_at,
			          d.completed_at`, batchSize, owner, lease.Milliseconds(), channel)
		if err != nil {
			return mapError(err)
		}
		defer rows.Close()
		attempts, err = collectDeliveries(rows)
		return err
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}

func (s *Store) RecordDeliveryResult(ctx context.Context, deliveryID, owner string, result domain.DeliveryResult, deadLetterTopic string) (domain.DeliveryAttempt, error) {
	if result.Status != domain.DeliveryStatusDelivered && result.Status != domain.DeliveryStatusFailed && result.Status != domain.DeliveryStatusRetrying {
		return domain.DeliveryAttempt{}, domain.ErrInvalidStateTransition
	}
	if strings.TrimSpace(deadLetterTopic) == "" {
		return domain.DeliveryAttempt{}, domain.NewValidationError("dead_letter_topic", "is required")
	}

	var updated domain.DeliveryAttempt
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var current domain.DeliveryAttempt
		if err := scanDelivery(tx.QueryRow(ctx, deliverySelect+`
			WHERE id=$1 AND status='delivering' AND locked_by=$2
			FOR UPDATE`, deliveryID, owner), &current); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrInvalidStateTransition
			}
			return mapError(err)
		}

		nextStatus := result.Status
		nextAttempt := current.AttemptNumber + 1
		if nextStatus == domain.DeliveryStatusRetrying && nextAttempt >= current.MaxAttempts {
			// The logical delivering->retrying->dead_letter transition is atomic.
			nextStatus = domain.DeliveryStatusDeadLetter
		}
		if nextStatus == domain.DeliveryStatusRetrying && result.NextRetryAt == nil {
			return domain.NewValidationError("next_retry_at", "is required for a retrying delivery")
		}

		completed := nextStatus.IsTerminal()
		err := scanDelivery(tx.QueryRow(ctx, `
			UPDATE delivery_attempts
			SET status=$3::varchar, attempt_number=$4,
			    next_retry_at=CASE WHEN $3::varchar='retrying' THEN $5 ELSE NULL END,
			    response_status=$6, response_body=$7, error_message=$8,
			    duration_ms=$9, locked_by=NULL, locked_until=NULL,
			    completed_at=CASE WHEN $10 THEN now() ELSE NULL END,
			    updated_at=now()
			WHERE id=$1 AND locked_by=$2 AND status='delivering'
			RETURNING id, event_id, notification_rule_id, tenant_id, channel,
			          status, attempt_number, max_attempts, next_retry_at,
			          locked_by, locked_until, request_payload, destination_config, response_status,
			          response_body, error_message, duration_ms, created_at,
			          updated_at, completed_at`,
			deliveryID, owner, nextStatus, nextAttempt, result.NextRetryAt,
			result.ResponseStatus, result.ResponseBody, result.ErrorMessage,
			result.DurationMs, completed,
		), &updated)
		if err != nil {
			return mapError(err)
		}
		if updated.Status == domain.DeliveryStatusDeadLetter {
			return insertDeliveryDeadLetter(ctx, tx, updated, deadLetterTopic)
		}
		return nil
	})
	if err != nil {
		return domain.DeliveryAttempt{}, err
	}
	return updated, nil
}

// RecoverExpiredDeliveries handles workers that died after claiming a row.
// Lease expiry counts as a failed attempt; exhausted rows and their outbox
// messages are committed atomically.
func (s *Store) RecoverExpiredDeliveries(ctx context.Context, limit int, deadLetterTopic string) ([]domain.DeliveryAttempt, error) {
	if limit < 1 || strings.TrimSpace(deadLetterTopic) == "" {
		return nil, domain.NewValidationError("delivery_recovery", "positive limit and dead-letter topic are required")
	}

	var recovered []domain.DeliveryAttempt
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, deliverySelect+`
			WHERE id IN (
				SELECT id FROM delivery_attempts
				WHERE status='delivering' AND locked_until < now()
				ORDER BY locked_until, id
				LIMIT $1 FOR UPDATE SKIP LOCKED
			)
			ORDER BY locked_until, id`, limit)
		if err != nil {
			return mapError(err)
		}
		expired, err := collectDeliveries(rows)
		rows.Close()
		if err != nil {
			return err
		}

		for _, current := range expired {
			nextAttempt := current.AttemptNumber + 1
			nextStatus := domain.DeliveryStatusRetrying
			if nextAttempt >= current.MaxAttempts {
				nextStatus = domain.DeliveryStatusDeadLetter
			}
			message := fmt.Sprintf("delivery lease expired while owned by %s", valueOrEmpty(current.LockedBy))
			var updated domain.DeliveryAttempt
			err := scanDelivery(tx.QueryRow(ctx, `
				UPDATE delivery_attempts
				SET status=$2::varchar, attempt_number=$3,
				    next_retry_at=CASE WHEN $2::varchar='retrying' THEN now() ELSE NULL END,
				    error_message=$4, locked_by=NULL, locked_until=NULL,
				    completed_at=CASE WHEN $2::varchar='dead_letter' THEN now() ELSE NULL END,
				    updated_at=now()
				WHERE id=$1 AND status='delivering'
				RETURNING id, event_id, notification_rule_id, tenant_id, channel,
				          status, attempt_number, max_attempts, next_retry_at,
				          locked_by, locked_until, request_payload, destination_config,
				          response_status, response_body, error_message, duration_ms,
				          created_at, updated_at, completed_at`,
				current.ID, nextStatus, nextAttempt, message), &updated)
			if err != nil {
				return mapError(err)
			}
			if updated.Status == domain.DeliveryStatusDeadLetter {
				if err := insertDeliveryDeadLetter(ctx, tx, updated, deadLetterTopic); err != nil {
					return err
				}
			}
			recovered = append(recovered, updated)
		}
		return nil
	})
	return recovered, err
}

// FinalizeReadyEvents repairs the small gap between storing a terminal
// delivery and finalizing its parent event (for example, after a DB outage).
func (s *Store) FinalizeReadyEvents(ctx context.Context, limit int) (int64, error) {
	if limit < 1 {
		return 0, domain.NewValidationError("finalize_limit", "must be positive")
	}
	result, err := s.pool.Exec(ctx, `
		WITH ready AS (
			SELECT e.id,
			       CASE
			         WHEN count(*) FILTER (WHERE d.status='delivered') = count(*) THEN 'completed'
			         WHEN count(*) FILTER (WHERE d.status='delivered') > 0 THEN 'partially_failed'
			         ELSE 'failed'
			       END AS final_status
			FROM events e
			JOIN delivery_attempts d ON d.event_id=e.id
			WHERE e.status='processing'
			GROUP BY e.id
			HAVING bool_and(d.status IN ('delivered', 'failed', 'dead_letter'))
			ORDER BY min(d.updated_at), e.id
			LIMIT $1
		)
		UPDATE events e
		SET status=ready.final_status, processed_at=now(), updated_at=now()
		FROM ready WHERE e.id=ready.id`, limit)
	if err != nil {
		return 0, mapError(err)
	}
	return result.RowsAffected(), nil
}

func insertDeliveryDeadLetter(ctx context.Context, tx pgx.Tx, delivery domain.DeliveryAttempt, topic string) error {
	failedAt := delivery.UpdatedAt
	if delivery.CompletedAt != nil {
		failedAt = *delivery.CompletedAt
	}
	payload, err := json.Marshal(domain.DeliveryDeadLetterMessage{
		Version: domain.DeliveryDeadLetterMessageVersion, DeliveryID: delivery.ID,
		EventID: delivery.EventID, NotificationRuleID: delivery.NotificationRuleID,
		TenantID: delivery.TenantID, Channel: delivery.Channel,
		AttemptNumber: delivery.AttemptNumber, RequestPayload: delivery.RequestPayload,
		ResponseStatus: delivery.ResponseStatus, ErrorMessage: delivery.ErrorMessage,
		FailedAt: failedAt, Status: delivery.Status,
	})
	if err != nil {
		return fmt.Errorf("marshal delivery dead-letter payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_id, topic, partition_key, payload)
		VALUES ($1, $2, $3, $4)`, delivery.ID, topic, delivery.TenantID, payload)
	return mapError(err)
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return "unknown worker"
	}
	return *value
}

// FinalizeEvent transitions a processing event only when all deliveries are terminal.
func (s *Store) FinalizeEvent(ctx context.Context, eventID string) (domain.EventStatus, bool, error) {
	var finalStatus domain.EventStatus
	finalized := false
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT status FROM delivery_attempts WHERE event_id=$1 FOR SHARE`, eventID)
		if err != nil {
			return mapError(err)
		}
		defer rows.Close()

		total := 0
		delivered := 0
		for rows.Next() {
			var status domain.DeliveryAttemptStatus
			if err := rows.Scan(&status); err != nil {
				return err
			}
			total++
			if !status.IsTerminal() {
				return nil
			}
			if status == domain.DeliveryStatusDelivered {
				delivered++
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		switch {
		case total == 0 || delivered == total:
			finalStatus = domain.EventStatusCompleted
		case delivered > 0:
			finalStatus = domain.EventStatusPartiallyFailed
		default:
			finalStatus = domain.EventStatusFailed
		}

		result, err := tx.Exec(ctx, `
			UPDATE events
			SET status=$2, processed_at=now(), updated_at=now()
			WHERE id=$1 AND status='processing'`, eventID, finalStatus)
		if err != nil {
			return mapError(err)
		}
		finalized = result.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return finalStatus, finalized, nil
}

const deliverySelect = `
	SELECT id, event_id, notification_rule_id, tenant_id, channel,
	       status, attempt_number, max_attempts, next_retry_at,
	       locked_by, locked_until, request_payload, destination_config, response_status,
	       response_body, error_message, duration_ms, created_at,
	       updated_at, completed_at
	FROM delivery_attempts`

func scanDelivery(row rowScanner, d *domain.DeliveryAttempt) error {
	return row.Scan(
		&d.ID, &d.EventID, &d.NotificationRuleID, &d.TenantID, &d.Channel,
		&d.Status, &d.AttemptNumber, &d.MaxAttempts, &d.NextRetryAt,
		&d.LockedBy, &d.LockedUntil, &d.RequestPayload, &d.DestinationConfig,
		&d.ResponseStatus, &d.ResponseBody, &d.ErrorMessage, &d.DurationMs, &d.CreatedAt,
		&d.UpdatedAt, &d.CompletedAt,
	)
}

func listDeliveryAttemptsTx(ctx context.Context, tx pgx.Tx, eventID string) ([]domain.DeliveryAttempt, error) {
	rows, err := tx.Query(ctx, deliverySelect+` WHERE event_id=$1 ORDER BY created_at, id`, eventID)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	return collectDeliveries(rows)
}

func collectDeliveries(rows pgx.Rows) ([]domain.DeliveryAttempt, error) {
	var deliveries []domain.DeliveryAttempt
	for rows.Next() {
		var delivery domain.DeliveryAttempt
		if err := scanDelivery(rows, &delivery); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

func listRulesTx(ctx context.Context, tx pgx.Tx, tenantID, eventType string) ([]domain.NotificationRule, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, tenant_id, event_type, channel, config, enabled, created_at, updated_at
		FROM notification_rules
		WHERE tenant_id=$1 AND enabled=true AND (event_type=$2 OR event_type='*')
		ORDER BY created_at, id`, tenantID, eventType)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	return collectRules(rows)
}

func collectRules(rows pgx.Rows) ([]domain.NotificationRule, error) {
	var rules []domain.NotificationRule
	for rows.Next() {
		var rule domain.NotificationRule
		if err := rows.Scan(&rule.ID, &rule.TenantID, &rule.EventType, &rule.Channel,
			&rule.Config, &rule.Enabled, &rule.CreatedAt, &rule.UpdatedAt); err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}
