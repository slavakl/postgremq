# PostgreMQ CLI

Command-line tool for managing PostgreMQ database schema and migrations.

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/slavakl/postgremq.git
cd postgremq

# Build the CLI
cd cmd/postgremq
go build -o postgremq .

# Optionally, move to a directory in your PATH
mv postgremq /usr/local/bin/
```

### Using Go Install

```bash
go install github.com/slavakl/postgremq/cmd/postgremq@latest
```

## Usage

### Database Connection

All commands require a `--dsn` flag with a PostgreSQL connection string:

```bash
postgremq <command> --dsn "postgres://user:password@host:port/database?sslmode=disable"
```

**Connection string formats:**

```bash
# Standard format
postgres://user:password@localhost:5432/mydb

# With SSL disabled (for local development)
postgres://user:password@localhost:5432/mydb?sslmode=disable

# With SSL required (for production)
postgres://user:password@host:5432/mydb?sslmode=require

# Using environment variable
export DATABASE_URL="postgres://user:password@localhost:5432/mydb"
postgremq migrate --dsn "$DATABASE_URL"
```

## Commands

### `migrate`

Apply pending database migrations to bring the schema up to date.

```bash
postgremq migrate --dsn <connection-string> [--target <version>]
```

**Flags:**

| Flag | Description | Default |
|------|-------------|---------|
| `--dsn` | Database connection string (required) | - |
| `--target` | Target migration version (0 = latest) | 0 |

**Examples:**

```bash
# Apply all pending migrations
postgremq migrate --dsn "postgres://postgres:postgres@localhost:5432/mydb"

# Migrate to a specific version
postgremq migrate --dsn "postgres://postgres:postgres@localhost:5432/mydb" --target 3
```

**Output:**

```
Current version: 0
Latest version:  1
Running migrations...
✓ Migration completed successfully
```

If the database is already up to date:

```
Current version: 1
Latest version:  1
✓ Database is up to date
```

### `status`

Display the current migration status of the database.

```bash
postgremq status --dsn <connection-string>
```

**Flags:**

| Flag | Description | Default |
|------|-------------|---------|
| `--dsn` | Database connection string (required) | - |

**Examples:**

```bash
postgremq status --dsn "postgres://postgres:postgres@localhost:5432/mydb"
```

**Output (migrations needed):**

```
Current version: 0
Latest version:  1
Dirty:           false

⚠ Migration needed: 1 pending migration(s)
```

**Output (up to date):**

```
Current version: 1
Latest version:  1
Dirty:           false

✓ Database is up to date
```

**Output (dirty state):**

```
Current version: 1
Latest version:  2
Dirty:           true

⚠ Migration needed: 1 pending migration(s)
```

A "dirty" state indicates a previous migration failed partway through. Manual intervention is required to resolve this.

## Migration Tracking

PostgreMQ uses a dedicated migrations table named `postgremq_migrations` to track applied migrations. This table is created automatically when you run your first migration.

**Schema:**

```sql
CREATE TABLE postgremq_migrations (
    version BIGINT PRIMARY KEY,
    dirty BOOLEAN NOT NULL DEFAULT FALSE
);
```

## Common Workflows

### Fresh Database Setup

```bash
# 1. Create your database (using psql or your preferred tool)
createdb myapp_db

# 2. Apply PostgreMQ migrations
postgremq migrate --dsn "postgres://postgres:postgres@localhost:5432/myapp_db"
```

### CI/CD Pipeline

```bash
#!/bin/bash
set -e

# Check if migrations are needed
postgremq status --dsn "$DATABASE_URL"

# Apply migrations
postgremq migrate --dsn "$DATABASE_URL"

# Start application
./myapp
```

### Development Workflow

```bash
# Check current status
postgremq status --dsn "postgres://localhost:5432/dev_db?sslmode=disable"

# Apply new migrations after pulling latest code
postgremq migrate --dsn "postgres://localhost:5432/dev_db?sslmode=disable"
```

## Error Handling

### Dirty State

If a migration fails partway through, the database is left in a "dirty" state:

```
Error: database is in dirty state - manual intervention required
```

**Resolution:**

1. Check the `postgremq_migrations` table to see which version failed
2. Manually fix the database state (complete or rollback the partial migration)
3. Update the `dirty` column to `false`
4. Re-run the migration

```sql
-- Check current state
SELECT * FROM postgremq_migrations;

-- After manually fixing the schema, clear the dirty flag
UPDATE postgremq_migrations SET dirty = false WHERE version = <failed_version>;
```

### Connection Errors

```
Error: failed to connect: connection refused
```

**Common causes:**

- PostgreSQL is not running
- Incorrect host/port
- Firewall blocking connection
- Invalid credentials

### Permission Errors

```
Error: migration failed: permission denied for schema public
```

**Resolution:** Ensure the database user has sufficient privileges:

```sql
GRANT ALL PRIVILEGES ON DATABASE mydb TO myuser;
GRANT ALL PRIVILEGES ON SCHEMA public TO myuser;
```

## Programmatic Migration

For applications that need to run migrations programmatically (e.g., at startup), use the standalone migration functions from the Go client library:

```go
package main

import (
    "context"
    "log"

    "github.com/jackc/pgx/v5/pgxpool"
    postgremq "github.com/slavakl/postgremq/postgremq-go"
)

func main() {
    ctx := context.Background()

    pool, _ := pgxpool.New(ctx, "postgres://...")
    defer pool.Close()

    // Check status
    status, _ := postgremq.GetMigrationStatus(ctx, pool)
    if status.NeedsMigration {
        // Apply migrations
        if err := postgremq.Migrate(ctx, pool, postgremq.MigrateOptions{}); err != nil {
            log.Fatal(err)
        }
    }

    // Now create a connection for message queue operations
    conn, _ := postgremq.DialFromPool(ctx, pool)
    defer conn.Close()
    // Use conn for publish/consume...
}
```

See `postgremq-go/examples/migration/` for a complete example.

## Environment Variables

The CLI reads the connection string from the `--dsn` flag. For convenience, you can use shell expansion:

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb"
postgremq migrate --dsn "$DATABASE_URL"
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Error (connection failed, migration failed, etc.) |

## See Also

- [PostgreMQ Go Client](../../postgremq-go/README.md) - Go client library documentation
- [Migration Example](../../postgremq-go/examples/migration/) - Programmatic migration example
