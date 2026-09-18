package postgremq_go

import (
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
//
// No ctx parameter: golang-migrate's API doesn't accept one, so a ctx
// would be ignored anyway.
func Migrate(pool *pgxpool.Pool, opts MigrateOptions) error {
	source, err := iofs.New(mq.MigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create a sql.DB connection using pgx stdlib driver
	db := openMigrationDB(pool)
	defer db.Close()

	// The driver creates its version table before executing migration SQL.
	if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS postgremq"); err != nil {
		return fmt.Errorf("failed to create queue schema: %w", err)
	}

	// Create the database driver with our custom config
	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{
		MigrationsTable: MigrationsTable,
		SchemaName:      "postgremq",
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
func GetMigrationStatus(pool *pgxpool.Pool) (*MigrationStatus, error) {
	db := openMigrationDB(pool)
	defer db.Close()

	status := &MigrationStatus{LatestVersion: getLatestMigrationVersion()}
	var exists bool
	if err := db.QueryRow("SELECT to_regclass('postgremq.postgremq_migrations') IS NOT NULL").Scan(&exists); err != nil {
		return nil, err
	}
	if exists {
		err := db.QueryRow("SELECT version, dirty FROM postgremq.postgremq_migrations LIMIT 1").Scan(&status.CurrentVersion, &status.Dirty)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}
	status.NeedsMigration = status.CurrentVersion < status.LatestVersion
	return status, nil
}

// openMigrationDB preserves the pool's connection parameters without changing
// its search_path or registering a global connection configuration.
func openMigrationDB(pool *pgxpool.Pool) *sql.DB {
	cfg := pool.Config().ConnConfig.Copy()
	return stdlib.OpenDB(*cfg)
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
