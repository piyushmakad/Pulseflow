// Package migrations embeds PulseFlow's versioned SQL migrations in the
// application binary so the exact same image can run migrations before a
// deployment starts serving traffic.
package migrations

import "embed"

// Files contains every up and down migration next to this source file.
//
//go:embed *.sql
var Files embed.FS
