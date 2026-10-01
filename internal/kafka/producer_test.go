package kafka

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestProducerPreservesPerMessageWriteResults(t *testing.T) {
	publishErr := errors.New("partition unavailable")
	writer := &fakeMessageWriter{err: kafkago.WriteErrors{nil, publishErr}}
	producer := &Producer{writer: writer}

	results := producer.Publish(context.Background(), []Message{
		{Topic: "events.ingested", Key: []byte("tenant-1"), Value: []byte(`{"id":1}`)},
		{Topic: "events.ingested", Key: []byte("tenant-2"), Value: []byte(`{"id":2}`)},
	})

	if len(results) != 2 || results[0] != nil || !errors.Is(results[1], publishErr) {
		t.Fatalf("unexpected publish results: %v", results)
	}
	if len(writer.messages) != 2 || string(writer.messages[0].Key) != "tenant-1" {
		t.Fatalf("unexpected Kafka messages: %+v", writer.messages)
	}
}

func TestProducerTreatsUnknownBatchErrorAsAmbiguousForEveryMessage(t *testing.T) {
	publishErr := errors.New("write canceled")
	producer := &Producer{writer: &fakeMessageWriter{err: publishErr}}

	results := producer.Publish(context.Background(), []Message{{}, {}})
	if len(results) != 2 || !errors.Is(results[0], publishErr) || !errors.Is(results[1], publishErr) {
		t.Fatalf("expected every result to contain the ambiguous batch error: %v", results)
	}
}

type fakeMessageWriter struct {
	err      error
	messages []kafkago.Message
}

func (w *fakeMessageWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.messages = append(w.messages, messages...)
	return w.err
}

func (w *fakeMessageWriter) Close() error { return nil }
