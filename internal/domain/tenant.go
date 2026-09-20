package domain

import (
	"time"
)

// In Node.js/TypeScript, you'd define this as:
// interface Tenant { id: string, name: string, status: string ... }
// Go structs are exactly like TypeScript interfaces or classes used for data.

// Tenant represents an organization using PulseFlow.
type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"` // "active" or "suspended"
	Config    []byte    `json:"config"` // JSONB stored as raw bytes (like Buffer in Node)
	CreatedAt time.Time `json:"created_at"` // time.Time is Go's equivalent to JS Date
	UpdatedAt time.Time `json:"updated_at"`
}

// APIKey represents a credential used by a Tenant to authenticate.
type APIKey struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	KeyHash   string     `json:"-"` // the "-" struct tag means "never include this in JSON output", keeping it secure!
	KeyPrefix string     `json:"key_prefix"`
	Name      string     `json:"name"`
	Status    string     `json:"status"` // "active" or "revoked"
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // Pointer (*) means it can be null/undefined in Go
}

// Struct tags (like `json:"id"`) tell Go's JSON parser (like JSON.stringify/JSON.parse)
// how to map the Go field Name to the JSON property name.
