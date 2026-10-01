package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"pulseflow/internal/domain"
)

// QuarantineMessage atomically stores the rejected source message and creates
// a dead-letter outbox entry. The bool is true only for the first insertion;
// Kafka redelivery of the same source offset is idempotent.
func (s *Store) QuarantineMessage(ctx context.Context, p domain.QuarantineMessageParams) (domain.QuarantinedMessage, bool, error) {
	if strings.TrimSpace(p.SourceTopic) == "" || strings.TrimSpace(p.DeadLetterTopic) == "" || strings.TrimSpace(p.ErrorMessage) == "" {
		return domain.QuarantinedMessage{}, false, domain.NewValidationError("quarantine", "source topic, dead-letter topic, and error are required")
	}
	if p.SourcePartition < 0 || p.SourceOffset < 0 {
		return domain.QuarantinedMessage{}, false, domain.NewValidationError("quarantine", "partition and offset must be non-negative")
	}
	if len(p.SourceTopic) > 255 || len(p.DeadLetterTopic) > 255 {
		return domain.QuarantinedMessage{}, false, domain.NewValidationError("quarantine", "topic names must be at most 255 characters")
	}

	var quarantined domain.QuarantinedMessage
	created := false
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		err := scanQuarantinedMessage(tx.QueryRow(ctx, `
			INSERT INTO quarantined_messages
			    (source_topic, source_partition, source_offset, message_key, payload, error_message)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
			RETURNING id, source_topic, source_partition, source_offset,
			          message_key, payload, error_message, created_at`,
			p.SourceTopic, p.SourcePartition, p.SourceOffset, p.MessageKey, p.Payload, p.ErrorMessage,
		), &quarantined)
		if errors.Is(err, pgx.ErrNoRows) {
			return mapError(scanQuarantinedMessage(tx.QueryRow(ctx, `
				SELECT id, source_topic, source_partition, source_offset,
				       message_key, payload, error_message, created_at
				FROM quarantined_messages
				WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3`,
				p.SourceTopic, p.SourcePartition, p.SourceOffset,
			), &quarantined))
		}
		if err != nil {
			return mapError(err)
		}

		created = true
		payload, err := json.Marshal(domain.DeadLetterMessage{
			Version:         domain.DeadLetterMessageVersion,
			QuarantineID:    quarantined.ID,
			SourceTopic:     quarantined.SourceTopic,
			SourcePartition: quarantined.SourcePartition,
			SourceOffset:    quarantined.SourceOffset,
			MessageKey:      quarantined.MessageKey,
			Payload:         quarantined.Payload,
			ErrorMessage:    quarantined.ErrorMessage,
			FailedAt:        quarantined.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("marshal dead-letter payload: %w", err)
		}

		partitionKey := deadLetterPartitionKey(p)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (aggregate_id, topic, partition_key, payload)
			VALUES ($1, $2, $3, $4)`, quarantined.ID, p.DeadLetterTopic, partitionKey, payload)
		return mapError(err)
	})
	if err != nil {
		return domain.QuarantinedMessage{}, false, err
	}
	return quarantined, created, nil
}

func deadLetterPartitionKey(p domain.QuarantineMessageParams) string {
	if len(p.MessageKey) == 0 {
		return fmt.Sprintf("%s:%d", p.SourceTopic, p.SourcePartition)
	}
	if len(p.MessageKey) <= 255 && utf8.Valid(p.MessageKey) {
		return string(p.MessageKey)
	}
	digest := sha256.Sum256(p.MessageKey)
	return fmt.Sprintf("sha256:%x", digest)
}

func scanQuarantinedMessage(row rowScanner, message *domain.QuarantinedMessage) error {
	return row.Scan(
		&message.ID,
		&message.SourceTopic,
		&message.SourcePartition,
		&message.SourceOffset,
		&message.MessageKey,
		&message.Payload,
		&message.ErrorMessage,
		&message.CreatedAt,
	)
}
