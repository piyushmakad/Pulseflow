package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"pulseflow/internal/domain"
)

func (s *Store) CreateTenant(ctx context.Context, name string, config json.RawMessage) (domain.Tenant, error) {
	if strings.TrimSpace(name) == "" {
		return domain.Tenant{}, domain.NewValidationError("name", "is required")
	}
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	if !json.Valid(config) {
		return domain.Tenant{}, domain.NewValidationError("config", "must be valid JSON")
	}

	var tenant domain.Tenant
	err := s.pool.QueryRow(ctx, `
		INSERT INTO tenants (name, config)
		VALUES ($1, $2)
		RETURNING id, name, status, config, created_at, updated_at`, name, config).Scan(
		&tenant.ID, &tenant.Name, &tenant.Status, &tenant.Config, &tenant.CreatedAt, &tenant.UpdatedAt,
	)
	if err != nil {
		return domain.Tenant{}, mapError(err)
	}
	return tenant, nil
}

func (s *Store) GetTenant(ctx context.Context, tenantID string) (domain.Tenant, error) {
	var tenant domain.Tenant
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, status, config, created_at, updated_at
		FROM tenants WHERE id=$1`, tenantID).Scan(
		&tenant.ID, &tenant.Name, &tenant.Status, &tenant.Config, &tenant.CreatedAt, &tenant.UpdatedAt,
	)
	if err != nil {
		return domain.Tenant{}, mapError(err)
	}
	return tenant, nil
}

type CreateAPIKeyParams struct {
	TenantID  string
	KeyHash   string
	Prefix    string
	Name      string
	ExpiresAt *time.Time
}

func (s *Store) CreateAPIKey(ctx context.Context, p CreateAPIKeyParams) (domain.APIKey, error) {
	if p.TenantID == "" || len(p.KeyHash) != 64 || p.Prefix == "" || strings.TrimSpace(p.Name) == "" {
		return domain.APIKey{}, domain.NewValidationError("api_key", "tenant_id, 64-character key_hash, prefix, and name are required")
	}

	var key domain.APIKey
	err := s.pool.QueryRow(ctx, `
		INSERT INTO api_keys (tenant_id, key_hash, key_prefix, name, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, tenant_id, key_hash, key_prefix, name, status, created_at, expires_at`,
		p.TenantID, p.KeyHash, p.Prefix, p.Name, p.ExpiresAt,
	).Scan(&key.ID, &key.TenantID, &key.KeyHash, &key.KeyPrefix, &key.Name, &key.Status, &key.CreatedAt, &key.ExpiresAt)
	if err != nil {
		return domain.APIKey{}, mapError(err)
	}
	return key, nil
}

func (s *Store) FindActiveAPIKeyByHash(ctx context.Context, hash string) (domain.APIKey, error) {
	var key domain.APIKey
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, key_hash, key_prefix, name, status, created_at, expires_at
		FROM api_keys
		WHERE key_hash=$1
		  AND status='active'
		  AND (expires_at IS NULL OR expires_at > now())`, hash).Scan(
		&key.ID, &key.TenantID, &key.KeyHash, &key.KeyPrefix, &key.Name, &key.Status, &key.CreatedAt, &key.ExpiresAt,
	)
	if err != nil {
		return domain.APIKey{}, mapError(err)
	}
	return key, nil
}

func (s *Store) RevokeAPIKey(ctx context.Context, tenantID, keyID string) error {
	result, err := s.pool.Exec(ctx, `
		UPDATE api_keys SET status='revoked'
		WHERE id=$1 AND tenant_id=$2 AND status='active'`, keyID, tenantID)
	if err != nil {
		return mapError(err)
	}
	if result.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}
