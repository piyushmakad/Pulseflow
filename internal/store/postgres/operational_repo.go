package postgres

import (
	"context"

	"pulseflow/internal/observability"
)

// OperationalSnapshot reads durable backlog signals from PostgreSQL and pool
// utilization from pgx. No delivery correctness decision depends on it.
func (s *Store) OperationalSnapshot(ctx context.Context) (observability.DatabaseSnapshot, error) {
	var snapshot observability.DatabaseSnapshot
	err := s.pool.QueryRow(ctx, `
		WITH outbox_stats AS (
			SELECT
				count(*) FILTER (WHERE status='pending') AS pending,
				count(*) FILTER (WHERE status='publishing') AS publishing,
				count(*) FILTER (WHERE status='publishing' AND locked_until < now()) AS expired,
				COALESCE(EXTRACT(EPOCH FROM now() - min(created_at))::double precision, 0) AS oldest_age
			FROM outbox
			WHERE status <> 'published'
		), delivery_stats AS (
			SELECT
				count(*) FILTER (WHERE status='pending' OR (status='retrying' AND next_retry_at <= now())) AS due,
				count(*) FILTER (WHERE status='delivering' AND locked_until < now()) AS expired
			FROM delivery_attempts
			WHERE status IN ('pending', 'retrying', 'delivering')
		)
		SELECT
			outbox_stats.pending, outbox_stats.publishing, outbox_stats.expired, outbox_stats.oldest_age,
			delivery_stats.due, delivery_stats.expired
		FROM outbox_stats CROSS JOIN delivery_stats`).Scan(
		&snapshot.OutboxPending,
		&snapshot.OutboxPublishing,
		&snapshot.ExpiredOutboxLeases,
		&snapshot.OldestOutboxAgeSeconds,
		&snapshot.DueDeliveries,
		&snapshot.ExpiredDeliveryLeases,
	)
	if err != nil {
		return observability.DatabaseSnapshot{}, mapError(err)
	}
	stats := s.pool.Stat()
	snapshot.PostgresAcquired = stats.AcquiredConns()
	snapshot.PostgresIdle = stats.IdleConns()
	snapshot.PostgresTotal = stats.TotalConns()
	snapshot.PostgresMax = stats.MaxConns()
	snapshot.PostgresAcquireWaitMs = stats.AcquireDuration().Milliseconds()
	return snapshot, nil
}
