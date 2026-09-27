package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"pulseflow/internal/domain"
	"pulseflow/internal/store/postgres"
)

type fakeProvisionStore struct {
	apiKeyParams postgres.CreateAPIKeyParams
}

func (f *fakeProvisionStore) CreateTenant(context.Context, string, json.RawMessage) (domain.Tenant, error) {
	return domain.Tenant{ID: "tenant-1"}, nil
}

func (f *fakeProvisionStore) CreateAPIKey(_ context.Context, params postgres.CreateAPIKeyParams) (domain.APIKey, error) {
	f.apiKeyParams = params
	return domain.APIKey{ID: "key-1", TenantID: params.TenantID, KeyHash: params.KeyHash, KeyPrefix: params.Prefix}, nil
}

func TestProvisionGeneratesStoredHashAndOneTimeRawKey(t *testing.T) {
	store := &fakeProvisionStore{}
	result, err := Provision(context.Background(), store, "Test Tenant", "default")
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if !strings.HasPrefix(result.RawKey, "pf_live_") {
		t.Fatalf("unexpected raw key prefix: %q", result.RawKey)
	}
	hashBytes := sha256.Sum256([]byte(result.RawKey))
	if store.apiKeyParams.KeyHash != hex.EncodeToString(hashBytes[:]) {
		t.Fatal("stored hash does not match raw key")
	}
	if len(store.apiKeyParams.Prefix) != 16 || store.apiKeyParams.Prefix != result.RawKey[:16] {
		t.Fatalf("unexpected display prefix: %q", store.apiKeyParams.Prefix)
	}
}
