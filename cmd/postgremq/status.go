package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  "Display current database migration version and status",
	RunE:  runStatus,
}

func init() {
	statusCmd.Flags().StringVar(&dsn, "dsn", "", "Database connection string (required)")
	statusCmd.MarkFlagRequired("dsn")
	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer pool.Close()

	status, err := postgremq.GetMigrationStatus(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	fmt.Printf("Current version: %d\n", status.CurrentVersion)
	fmt.Printf("Latest version:  %d\n", status.LatestVersion)
	fmt.Printf("Dirty:           %v\n", status.Dirty)

	if status.NeedsMigration {
		pending := status.LatestVersion - status.CurrentVersion
		fmt.Printf("\n⚠ Migration needed: %d pending migration(s)\n", pending)
	} else {
		fmt.Println("\n✓ Database is up to date")
	}

	return nil
}
