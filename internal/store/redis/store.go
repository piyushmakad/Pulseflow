package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Store owns the Redis client used for disposable cache and rate-limit data.
// PostgreSQL remains the source of truth.
type Store struct {
	client *goredis.Client
}

func New(rawURL string) (*Store, error) {
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	return &Store{client: goredis.NewClient(options)}, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
