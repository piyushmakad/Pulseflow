package kafka

import (
	"context"
	"time"
)

// Message is the small transport-neutral shape used by the relay and its
// tests. The concrete Kafka client is kept behind Publisher and Reader.
type Message struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Time          time.Time
}

type Publisher interface {
	// Publish returns one result per input message. A nil entry means that
	// message was acknowledged by Kafka.
	Publish(ctx context.Context, messages []Message) []error
	Close() error
}

type Reader interface {
	FetchMessage(ctx context.Context) (Message, error)
	CommitMessages(ctx context.Context, messages ...Message) error
	Close() error
}
