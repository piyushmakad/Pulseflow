package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"pulseflow/internal/api/response"
	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type APIKeyRepository interface {
	FindActiveAPIKeyByHash(ctx context.Context, hash string) (domain.APIKey, error)
}

type APIKeyCache interface {
	GetAPIKey(ctx context.Context, hash string) (tenantID, apiKeyID string, found bool, err error)
	PutAPIKey(ctx context.Context, hash, tenantID, apiKeyID string, ttl time.Duration) error
}

type Principal struct {
	TenantID string
	APIKeyID string
}

type principalContextKey struct{}

type Auth struct {
	repository APIKeyRepository
	cache      APIKeyCache
	cacheTTL   time.Duration
	logger     *logger.Logger
}

func NewAuth(repository APIKeyRepository, cache APIKeyCache, cacheTTL time.Duration, log *logger.Logger) *Auth {
	return &Auth{repository: repository, cache: cache, cacheTTL: cacheTTL, logger: log}
}

func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawKey, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || !strings.HasPrefix(rawKey, "pf_live_") {
			response.Error(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
			return
		}

		hashBytes := sha256.Sum256([]byte(rawKey))
		hash := hex.EncodeToString(hashBytes[:])

		if a.cache != nil {
			tenantID, apiKeyID, found, err := a.cache.GetAPIKey(r.Context(), hash)
			if err != nil {
				a.logger.Warn("API key cache lookup failed; falling back to PostgreSQL", "error", err)
			} else if found {
				next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), Principal{TenantID: tenantID, APIKeyID: apiKeyID})))
				return
			}
		}

		key, err := a.repository.FindActiveAPIKeyByHash(r.Context(), hash)
		if errors.Is(err, domain.ErrNotFound) {
			response.Error(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
			return
		}
		if err != nil {
			a.logger.Error("API key PostgreSQL lookup failed", "error", err)
			response.Error(w, http.StatusServiceUnavailable, "service_unavailable", "authentication is temporarily unavailable")
			return
		}

		if a.cache != nil {
			if err := a.cache.PutAPIKey(r.Context(), hash, key.TenantID, key.ID, a.cacheTTL); err != nil {
				a.logger.Warn("API key cache write failed", "error", err)
			}
		}

		ctx := WithPrincipal(r.Context(), Principal{TenantID: key.TenantID, APIKeyID: key.ID})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}
