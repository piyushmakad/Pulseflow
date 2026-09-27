package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRedisCacheAndTokenBucketIntegration(t *testing.T) {
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set TEST_REDIS_URL to run Redis integration tests")
	}

	store, err := New(redisURL)
	if err != nil {
		t.Fatalf("create Redis store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	hash := "integration-" + unique
	if err := store.PutAPIKey(ctx, hash, "tenant-"+unique, "key-"+unique, time.Minute); err != nil {
		t.Fatalf("put API key cache: %v", err)
	}
	t.Cleanup(func() { _ = store.DeleteAPIKey(context.Background(), hash) })
	tenantID, apiKeyID, found, err := store.GetAPIKey(ctx, hash)
	if err != nil || !found || tenantID != "tenant-"+unique || apiKeyID != "key-"+unique {
		t.Fatalf("get API key cache: tenant=%q key=%q found=%v err=%v", tenantID, apiKeyID, found, err)
	}

	allowed, remaining, err := store.Allow(ctx, "integration-"+unique, 2, time.Minute)
	if err != nil || !allowed || remaining != 1 {
		t.Fatalf("first token: allowed=%v remaining=%d err=%v", allowed, remaining, err)
	}
	allowed, remaining, err = store.Allow(ctx, "integration-"+unique, 2, time.Minute)
	if err != nil || !allowed || remaining != 0 {
		t.Fatalf("second token: allowed=%v remaining=%d err=%v", allowed, remaining, err)
	}
	allowed, remaining, err = store.Allow(ctx, "integration-"+unique, 2, time.Minute)
	if err != nil || allowed || remaining != 0 {
		t.Fatalf("exhausted token: allowed=%v remaining=%d err=%v", allowed, remaining, err)
	}
}
