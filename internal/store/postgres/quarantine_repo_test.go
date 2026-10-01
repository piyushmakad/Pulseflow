package postgres

import (
	"strings"
	"testing"
	"unicode/utf8"

	"pulseflow/internal/domain"
)

func TestDeadLetterPartitionKeyPreservesSafeKeys(t *testing.T) {
	params := domain.QuarantineMessageParams{MessageKey: []byte("tenant-123")}
	if got := deadLetterPartitionKey(params); got != "tenant-123" {
		t.Fatalf("expected tenant key, got %q", got)
	}
}

func TestDeadLetterPartitionKeyHashesUnsafeKeys(t *testing.T) {
	tests := [][]byte{
		{0xff, 0xfe},
		[]byte(strings.Repeat("x", 256)),
	}
	for _, key := range tests {
		got := deadLetterPartitionKey(domain.QuarantineMessageParams{MessageKey: key})
		if !utf8.ValidString(got) || len(got) > 255 || !strings.HasPrefix(got, "sha256:") {
			t.Fatalf("expected bounded UTF-8 hash key, got %q", got)
		}
	}
}
