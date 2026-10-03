package migration

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"pulseflow/migrations"
)

// Up applies every unapplied embedded SQL migration. golang-migrate uses a
// PostgreSQL advisory lock, so two deployment jobs cannot migrate the same
// database concurrently.
func Up(databaseURL string) error {
	source, err := iofs.New(migrations.Files, ".")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}

	driverURL, err := pgxMigrationURL(databaseURL)
	if err != nil {
		return err
	}

	runner, err := migrate.NewWithSourceInstance("iofs", source, driverURL)
	if err != nil {
		return fmt.Errorf("create migration runner: %w", err)
	}
	defer func() {
		_, _ = runner.Close()
	}()

	if err := runner.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func pgxMigrationURL(databaseURL string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", fmt.Errorf("parse migration database URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" && parsed.Scheme != "pgx5" {
		return "", fmt.Errorf("migration database URL must use postgres or postgresql scheme")
	}
	parsed.Scheme = "pgx5"
	return parsed.String(), nil
}
