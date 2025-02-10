# PostgreMQ Go Client

Go client library for PostgreMQ - a message queue system built on PostgreSQL.

## Testing Approach

The test suite uses a robust approach designed for reliable parallel test execution:

1. **Database per Test**: Each test gets its own isolated PostgreSQL database, allowing tests to run in true parallel mode without interfering with each other.

2. **Docker-based**: Tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a single PostgreSQL container that's shared across all tests.

3. **Synchronized Operations**: Critical database operations like schema initialization and database cleanup use mutexes to prevent race conditions when running in parallel.

4. **Automatic Cleanup**: Test databases are automatically created before each test and dropped after all its subtests complete, ensuring no leftover test data.

5. **Subtests Support**: For tests using `t.Run()` with multiple subtests, the `cleanTestData()` function cleans the database between subtests since they share the same database connection.

6. **Error Resilience**: The test framework handles common PostgreSQL errors gracefully, especially during parallel execution, making the tests more reliable.

### Running Tests

#### Running all tests:
```bash
go test ./...
```

#### Running tests in short mode (skips long-running tests):
```bash
go test -short ./...
```

#### Running tests in parallel:
```bash
# Run with specified parallelism level (e.g., 4 parallel tests)
go test -parallel 4 ./...

# Run short tests in parallel
go test -short -parallel 4 ./...
```

#### Using the quick test script:
```bash
# Makes the script executable if needed
chmod +x scripts/quicktest.sh

# Run quick tests with default parallelism (4)
./scripts/quicktest.sh

# Run quick tests with custom parallelism
./scripts/quicktest.sh 8  # Run with 8 parallel tests
```

## Usage

[Include client usage examples here]