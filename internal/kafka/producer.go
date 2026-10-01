package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer is the concrete synchronous Kafka publisher. Synchronous writes
// let the outbox relay distinguish broker acknowledgement from failure before
// it changes the PostgreSQL outbox state.
type Producer struct {
	writer messageWriter
}

type messageWriter interface {
	WriteMessages(ctx context.Context, messages ...kafkago.Message) error
	Close() error
}

func NewProducer(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("Kafka broker address cannot be empty")
		}
	}

	return &Producer{writer: &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}}, nil
}

func (p *Producer) Publish(ctx context.Context, messages []Message) []error {
	results := make([]error, len(messages))
	if len(messages) == 0 {
		return results
	}

	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{
			Topic: message.Topic,
			Key:   message.Key,
			Value: message.Value,
			Time:  message.Time,
		}
	}

	err := p.writer.WriteMessages(ctx, kafkaMessages...)
	if err == nil {
		return results
	}

	var writeErrors kafkago.WriteErrors
	if errors.As(err, &writeErrors) && len(writeErrors) == len(results) {
		copy(results, writeErrors)
		return results
	}
	for i := range results {
		results[i] = err
	}
	return results
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
