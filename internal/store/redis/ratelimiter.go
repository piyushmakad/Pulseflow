package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var tokenBucketScript = goredis.NewScript(`
local capacity = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(bucket[1])
local updated_at = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  updated_at = now_ms
else
  local elapsed = math.max(0, now_ms - updated_at)
  local refill = elapsed * capacity / window_ms
  tokens = math.min(capacity, tokens + refill)
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now_ms)
redis.call('PEXPIRE', KEYS[1], window_ms * 2)

return {allowed, math.floor(tokens)}
`)

func (s *Store) Allow(ctx context.Context, tenantID string, capacity int, window time.Duration) (allowed bool, remaining int64, err error) {
	if tenantID == "" || capacity < 1 || window <= 0 {
		return false, 0, fmt.Errorf("invalid token bucket arguments")
	}

	result, err := tokenBucketScript.Run(
		ctx,
		s.client,
		[]string{"rl:{" + tenantID + "}"},
		capacity,
		window.Milliseconds(),
		time.Now().UnixMilli(),
	).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(result) != 2 {
		return false, 0, fmt.Errorf("unexpected token bucket result length: %d", len(result))
	}

	allowedValue, ok := result[0].(int64)
	if !ok {
		return false, 0, fmt.Errorf("unexpected token bucket allowed value: %T", result[0])
	}
	remainingValue, ok := result[1].(int64)
	if !ok {
		return false, 0, fmt.Errorf("unexpected token bucket remaining value: %T", result[1])
	}
	return allowedValue == 1, remainingValue, nil
}
