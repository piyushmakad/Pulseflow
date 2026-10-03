package migration

import "testing"

func TestPgxMigrationURL(t *testing.T) {
	got, err := pgxMigrationURL("postgres://pulseflow:secret@postgres:5432/pulseflow?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxMigrationURL returned an error: %v", err)
	}
	want := "pgx5://pulseflow:secret@postgres:5432/pulseflow?sslmode=disable"
	if got != want {
		t.Fatalf("pgxMigrationURL = %q, want %q", got, want)
	}
}

func TestPgxMigrationURLRejectsUnsupportedScheme(t *testing.T) {
	if _, err := pgxMigrationURL("mysql://localhost/pulseflow"); err == nil {
		t.Fatal("pgxMigrationURL accepted a non-PostgreSQL URL")
	}
}
