package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"pulseflow/internal/domain"
	"pulseflow/internal/store/postgres"
)

type ProvisionStore interface {
	CreateTenant(ctx context.Context, name string, config json.RawMessage) (domain.Tenant, error)
	CreateAPIKey(ctx context.Context, params postgres.CreateAPIKeyParams) (domain.APIKey, error)
}

type ProvisionResult struct {
	Tenant domain.Tenant
	APIKey domain.APIKey
	RawKey string
}

func Provision(ctx context.Context, store ProvisionStore, tenantName, keyName string) (ProvisionResult, error) {
	tenant, err := store.CreateTenant(ctx, tenantName, json.RawMessage(`{}`))
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("create tenant: %w", err)
	}

	rawKey, hash, prefix, err := generateAPIKey()
	if err != nil {
		return ProvisionResult{}, err
	}
	apiKey, err := store.CreateAPIKey(ctx, postgres.CreateAPIKeyParams{
		TenantID: tenant.ID,
		KeyHash:  hash,
		Prefix:   prefix,
		Name:     keyName,
	})
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("create API key: %w", err)
	}
	return ProvisionResult{Tenant: tenant, APIKey: apiKey, RawKey: rawKey}, nil
}

func generateAPIKey() (rawKey, hash, prefix string, err error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", "", "", fmt.Errorf("generate API key: %w", err)
	}
	rawKey = "pf_live_" + base64.RawURLEncoding.EncodeToString(random)
	hashBytes := sha256.Sum256([]byte(rawKey))
	hash = hex.EncodeToString(hashBytes[:])
	prefix = rawKey[:16]
	return rawKey, hash, prefix, nil
}
