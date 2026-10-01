package domain

import "time"

const DeadLetterMessageVersion = 1

type QuarantineMessageParams struct {
	SourceTopic     string
	SourcePartition int
	SourceOffset    int64
	MessageKey      []byte
	Payload         []byte
	ErrorMessage    string
	DeadLetterTopic string
}

// QuarantinedMessage is the durable PostgreSQL record for a Kafka message
// that cannot be decoded, validated, or matched to its source event.
type QuarantinedMessage struct {
	ID              string    `json:"id"`
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int       `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	MessageKey      []byte    `json:"message_key,omitempty"`
	Payload         []byte    `json:"payload"`
	ErrorMessage    string    `json:"error_message"`
	CreatedAt       time.Time `json:"created_at"`
}

// DeadLetterMessage is the versioned payload eventually published to the
// dead-letter topic through the same transactional outbox as normal events.
type DeadLetterMessage struct {
	Version         int       `json:"version"`
	QuarantineID    string    `json:"quarantine_id"`
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int       `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	MessageKey      []byte    `json:"message_key,omitempty"`
	Payload         []byte    `json:"payload"`
	ErrorMessage    string    `json:"error_message"`
	FailedAt        time.Time `json:"failed_at"`
}
