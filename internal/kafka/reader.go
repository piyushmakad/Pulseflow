package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafkago.Reader
}

type ReaderConfig struct {
	Brokers []string
	GroupID string
	Topic   string
}

func NewReader(cfg ReaderConfig) (*KafkaReader, error) {
	if len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.GroupID) == "" || strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("Kafka brokers, consumer group, and topic are required")
	}
	return &KafkaReader{reader: kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		MaxWait:        time.Second,
		CommitInterval: 0,
	})}, nil
}

func (r *KafkaReader) FetchMessage(ctx context.Context) (Message, error) {
	message, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Topic:         message.Topic,
		Partition:     message.Partition,
		Offset:        message.Offset,
		HighWaterMark: message.HighWaterMark,
		Key:           message.Key,
		Value:         message.Value,
		Time:          message.Time,
	}, nil
}

func (r *KafkaReader) CommitMessages(ctx context.Context, messages ...Message) error {
	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{
			Topic:     message.Topic,
			Partition: message.Partition,
			Offset:    message.Offset,
		}
	}
	return r.reader.CommitMessages(ctx, kafkaMessages...)
}

func (r *KafkaReader) Close() error {
	return r.reader.Close()
}
