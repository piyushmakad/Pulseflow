package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"pulseflow/internal/domain"
)

func (s *Store) ClaimOutbox(ctx context.Context, owner string, batchSize int, lease time.Duration) ([]domain.OutboxEntry, error) {
	if owner == "" || batchSize < 1 || lease <= 0 {
		return nil, domain.NewValidationError("outbox_claim", "owner, positive batch size, and positive lease are required")
	}

	var entries []domain.OutboxEntry
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH candidates AS (
				SELECT id
				FROM outbox
				WHERE available_at <= now()
				  AND (status='pending' OR (status='publishing' AND locked_until < now()))
				ORDER BY created_at, id
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			)
			UPDATE outbox AS o
			SET status='publishing',
			    locked_by=$2,
			    locked_until=now() + ($3 * interval '1 millisecond'),
			    publish_attempts=o.publish_attempts+1,
			    last_error=NULL
			FROM candidates
			WHERE o.id=candidates.id
			RETURNING o.id, o.aggregate_id, o.topic, o.partition_key, o.payload,
			          o.status, o.publish_attempts, o.available_at, o.locked_by,
			          o.locked_until, o.last_error, o.created_at, o.published_at`,
			batchSize, owner, lease.Milliseconds())
		if err != nil {
			return mapError(err)
		}
		defer rows.Close()

		for rows.Next() {
			var entry domain.OutboxEntry
			if err := scanOutbox(rows, &entry); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, owner string, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET status='published', published_at=now(), locked_by=NULL, locked_until=NULL, last_error=NULL
		WHERE id=ANY($1) AND status='publishing' AND locked_by=$2`, ids, owner)
	if err != nil {
		return 0, mapError(err)
	}
	return result.RowsAffected(), nil
}

func (s *Store) ReleaseOutboxForRetry(ctx context.Context, owner string, ids []int64, availableAt time.Time, cause string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET status='pending', available_at=$3, locked_by=NULL, locked_until=NULL, last_error=$4
		WHERE id=ANY($1) AND status='publishing' AND locked_by=$2`, ids, owner, availableAt, cause)
	if err != nil {
		return 0, mapError(err)
	}
	return result.RowsAffected(), nil
}

func (s *Store) DeletePublishedOutboxBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit < 1 {
		return 0, domain.NewValidationError("limit", "must be positive")
	}
	result, err := s.pool.Exec(ctx, `
		DELETE FROM outbox
		WHERE id IN (
			SELECT id FROM outbox
			WHERE status='published' AND published_at < $1
			ORDER BY published_at
			LIMIT $2
		)`, cutoff, limit)
	if err != nil {
		return 0, mapError(err)
	}
	return result.RowsAffected(), nil
}

func scanOutbox(row rowScanner, entry *domain.OutboxEntry) error {
	return row.Scan(
		&entry.ID,
		&entry.AggregateID,
		&entry.Topic,
		&entry.PartitionKey,
		&entry.Payload,
		&entry.Status,
		&entry.PublishAttempts,
		&entry.AvailableAt,
		&entry.LockedBy,
		&entry.LockedUntil,
		&entry.LastError,
		&entry.CreatedAt,
		&entry.PublishedAt,
	)
}
