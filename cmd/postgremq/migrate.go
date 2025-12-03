package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/spf13/cobra"
)

var (
	dsn       string
	targetVer int
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long:  "Apply pending database migrations to the latest or specified version",
	RunE:  runMigrate,
}

func init() {
	migrateCmd.Flags().StringVar(&dsn, "dsn", "", "Database connection string (required)")
	migrateCmd.Flags().IntVar(&targetVer, "target", 0, "Target version (0 = latest)")
	migrateCmd.MarkFlagRequired("dsn")
	rootCmd.AddCommand(migrateCmd)
}

func runMigrate(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer pool.Close()

	// Check status first
	status, err := postgremq.GetMigrationStatus(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	fmt.Printf("Current version: %d\n", status.CurrentVersion)
	fmt.Printf("Latest version:  %d\n", status.LatestVersion)

	if status.Dirty {
		return fmt.Errorf("database is in dirty state - manual intervention required")
	}

	if !status.NeedsMigration && targetVer == 0 {
		fmt.Println("✓ Database is up to date")
		return nil
	}

	// Run migration
	fmt.Println("Running migrations...")
	if err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{
		TargetVersion: targetVer,
	}); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	fmt.Println("✓ Migration completed successfully")
	return nil
}
