package config

import "testing"

func TestLoadRateLimitBypassTenant(t *testing.T) {
	for _, value := range []string{"", "  test-tenant  "} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("RATE_LIMIT_BYPASS_TENANT_ID", value)
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			want := ""
			if value != "" {
				want = "test-tenant"
			}
			if cfg.RateLimitBypassTenantID != want {
				t.Fatalf("bypass tenant = %q, want %q", cfg.RateLimitBypassTenantID, want)
			}
		})
	}
}
