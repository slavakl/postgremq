package postgremq_go_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupEmptyTestDatabase creates a database WITHOUT the PostgreMQ schema
// This is different from setupTestConnection which pre-initializes the schema
func setupEmptyTestDatabase(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()

	// Create a unique database for this test
	dbName := fmt.Sprintf("pgmq_migrate_test_%s_%d", sanitizeTestName(t.Name()), time.Now().UnixNano())

	// Connect to the container's default postgres database
	adminConn, err := pgxpool.NewWithConfig(ctx, sharedContainer.config)
	require.NoError(t, err, "Failed to connect to postgres")
	defer adminConn.Close()

	// Create the test database
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	require.NoError(t, err, "Failed to create test database")

	// Update the connection config to point to the new database
	testConfig := sharedContainer.config.Copy()
	testConfig.ConnConfig.Database = dbName
	testConfig.MaxConns = 10
	testConfig.MinConns = 2

	// Connect to the new test database (WITHOUT initializing schema)
	testPool, err := pgxpool.NewWithConfig(ctx, testConfig)
	require.NoError(t, err, "Failed to connect to test database")

	t.Logf("Created empty test database: %s", dbName)

	// Register cleanup to drop the database when the test is done
	t.Cleanup(func() {
		testPool.Close()
		time.Sleep(100 * time.Millisecond)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = sharedContainer.adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			t.Logf("Warning: Failed to drop test database %s: %v", dbName, err)
		}
	})

	return testPool, ctx
}

func TestMigration_StatusOnEmptyDatabase(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	status, err := postgremq.GetMigrationStatus(ctx, pool)
	require.NoError(t, err)

	assert.Equal(t, uint(0), status.CurrentVersion, "Empty database should have version 0")
	assert.False(t, status.Dirty, "Empty database should not be dirty")
	assert.True(t, status.NeedsMigration, "Empty database should need migration")
	assert.Greater(t, status.LatestVersion, uint(0), "Latest version should be > 0")
}

func TestMigration_MigrateEmptyDatabase(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Run migration
	err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{})
	require.NoError(t, err)

	// Verify migration succeeded
	status, err := postgremq.GetMigrationStatus(ctx, pool)
	require.NoError(t, err)

	assert.Equal(t, uint(1), status.CurrentVersion, "Should be at version 1 after migration")
	assert.False(t, status.Dirty, "Should not be dirty after successful migration")
	assert.False(t, status.NeedsMigration, "Should not need migration after migrating")
}

func TestMigration_Idempotency(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Run migration first time
	err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{})
	require.NoError(t, err)

	// Run migration second time - should be idempotent
	err = postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{})
	require.NoError(t, err, "Running migrate twice should not fail")

	// Verify still at correct version
	status, err := postgremq.GetMigrationStatus(ctx, pool)
	require.NoError(t, err)

	assert.Equal(t, uint(1), status.CurrentVersion)
	assert.False(t, status.NeedsMigration)
}

func TestMigration_SchemaIsUsable(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Run migration first
	err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{})
	require.NoError(t, err)

	// Now create a connection to test message queue operations
	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	// Verify the schema is actually usable by performing typical operations
	topicName := "test-topic"
	queueName := "test-queue"

	// Create topic
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Should be able to create topic after migration")

	// Create queue
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Should be able to create queue after migration")

	// Publish message
	msgID, err := conn.Publish(ctx, topicName, []byte(`{"test": "data"}`))
	require.NoError(t, err, "Should be able to publish message after migration")
	assert.Greater(t, msgID, 0, "Message ID should be positive")

	// Consume message
	consumer, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(1), postgremq.WithVT(30))
	require.NoError(t, err, "Should be able to create consumer after migration")
	defer consumer.Stop()

	// Wait for message
	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, msgID, msg.ID)
		err = msg.Ack(ctx)
		require.NoError(t, err, "Should be able to ack message after migration")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestMigration_MigrationsTableName(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Run migration
	err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{})
	require.NoError(t, err)

	// Verify the migrations table uses our custom name
	var tableName string
	err = pool.QueryRow(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_name = 'postgremq_migrations'
	`).Scan(&tableName)
	require.NoError(t, err, "Should find postgremq_migrations table")
	assert.Equal(t, "postgremq_migrations", tableName)

	// Verify the default golang-migrate table was NOT created
	var count int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_name = 'schema_migrations'
	`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Default schema_migrations table should not exist")
}

func TestMigration_TargetVersion(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Migrate to specific version (currently only version 1 exists)
	err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{
		TargetVersion: 1,
	})
	require.NoError(t, err)

	status, err := postgremq.GetMigrationStatus(ctx, pool)
	require.NoError(t, err)
	assert.Equal(t, uint(1), status.CurrentVersion)
}
