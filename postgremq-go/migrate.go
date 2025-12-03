package postgremq_go

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/slavakl/postgremq/mq"
)

const MigrationsTable = "postgremq_migrations"

// MigrateOptions configures migration behavior
type MigrateOptions struct {
	// TargetVersion to migrate to. 0 means latest
	TargetVersion int
}

// MigrationStatus represents the current state of migrations
type MigrationStatus struct {
	CurrentVersion uint
	Dirty          bool
	LatestVersion  uint
	NeedsMigration bool
}

// Migrate runs database migrations using the provided pool.
// This is a standalone function for schema management, separate from
// the Connection type which is used for message queue operations.
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts MigrateOptions) error {
	source, err := iofs.New(mq.MigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create a sql.DB connection using pgx stdlib driver
	db, err := openMigrationDB(pool)
	if err != nil {
		return fmt.Errorf("failed to open database for migration: %w", err)
	}
	defer db.Close()

	// Create the database driver with our custom config
	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{
		MigrationsTable: MigrationsTable,
	})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	// Apply migration (only up migrations are supported)
	var migErr error
	if opts.TargetVersion > 0 {
		migErr = m.Migrate(uint(opts.TargetVersion))
	} else {
		migErr = m.Up()
	}

	if migErr != nil && migErr != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", migErr)
	}

	return nil
}

// GetMigrationStatus returns current migration status using the provided pool.
// This is a standalone function for schema management, separate from
// the Connection type which is used for message queue operations.
func GetMigrationStatus(ctx context.Context, pool *pgxpool.Pool) (*MigrationStatus, error) {
	source, err := iofs.New(mq.MigrationsFS, "migrations")
	if err != nil {
		return nil, err
	}

	// Create a sql.DB connection using pgx stdlib driver
	db, err := openMigrationDB(pool)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create the database driver with our custom config
	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{
		MigrationsTable: MigrationsTable,
	})
	if err != nil {
		return nil, err
	}

	m, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return nil, err
	}
	defer m.Close()

	currentVersion, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return nil, err
	}

	// Find latest version from embedded migrations
	latestVersion := getLatestMigrationVersion()

	return &MigrationStatus{
		CurrentVersion: currentVersion,
		Dirty:          dirty,
		LatestVersion:  latestVersion,
		NeedsMigration: currentVersion < latestVersion,
	}, nil
}

// openMigrationDB creates a sql.DB connection from the pgxpool configuration
// using the pgx stdlib driver. By using RegisterConnConfig, we ensure the exact
// same connection parameters (including TLS settings) as the pool are used.
func openMigrationDB(pool *pgxpool.Pool) (*sql.DB, error) {
	cfg := pool.Config().ConnConfig.Copy()

	// Register the config and get a connection string identifier
	connStr := stdlib.RegisterConnConfig(cfg)

	return sql.Open("pgx/v5", connStr)
}

// getLatestMigrationVersion returns the highest migration version from embedded files
func getLatestMigrationVersion() uint {
	entries, err := mq.MigrationsFS.ReadDir("migrations")
	if err != nil {
		return 0
	}

	var latestVersion uint
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		// Parse version from filename like "000001_initial_schema.up.sql"
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 1 {
			continue
		}

		ver, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			continue
		}

		if uint(ver) > latestVersion {
			latestVersion = uint(ver)
		}
	}

	return latestVersion
}
