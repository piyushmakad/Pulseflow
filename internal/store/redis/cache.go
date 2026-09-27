package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type apiKeyCacheEntry struct {
	TenantID string `json:"tenant_id"`
	APIKeyID string `json:"api_key_id"`
}

func (s *Store) GetAPIKey(ctx context.Context, hash string) (tenantID, apiKeyID string, found bool, err error) {
	value, err := s.client.Get(ctx, "apikey:"+hash).Bytes()
	if err == goredis.Nil {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}

	var entry apiKeyCacheEntry
	if err := json.Unmarshal(value, &entry); err != nil {
		return "", "", false, fmt.Errorf("decode API key cache entry: %w", err)
	}
	if entry.TenantID == "" || entry.APIKeyID == "" {
		return "", "", false, fmt.Errorf("API key cache entry is incomplete")
	}
	return entry.TenantID, entry.APIKeyID, true, nil
}

func (s *Store) PutAPIKey(ctx context.Context, hash, tenantID, apiKeyID string, ttl time.Duration) error {
	value, err := json.Marshal(apiKeyCacheEntry{TenantID: tenantID, APIKeyID: apiKeyID})
	if err != nil {
		return fmt.Errorf("encode API key cache entry: %w", err)
	}
	return s.client.Set(ctx, "apikey:"+hash, value, ttl).Err()
}

func (s *Store) DeleteAPIKey(ctx context.Context, hash string) error {
	return s.client.Del(ctx, "apikey:"+hash).Err()
}
