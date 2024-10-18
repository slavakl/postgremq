# Contributing to PostgreMQ

Thank you for your interest in contributing to PostgreMQ! We welcome contributions from the community.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Guidelines](#coding-guidelines)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. Please read [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) before contributing.

## Getting Started

PostgreMQ consists of three main components:

1. **mq** - Core PostgreSQL schema and functions
2. **postgremq-go** - Go client library
3. **postgremq-ts** - TypeScript client library

You can contribute to any or all of these components.

## Development Setup

### Prerequisites

- PostgreSQL 15 or later
- Go 1.23 or later
- Node.js 14 or later
- Python 3.8+ (for SQL tests)
- Docker (for testcontainers in tests)
- Git

### Clone the Repository

```bash
git clone https://github.com/slavakl/postgremq.git
cd postgremq
```

### SQL Schema Development

```bash
cd mq

# Install Python dependencies
pip install -r tests/requirements.txt

# Run tests
pytest tests/tests.py -v
```

The SQL schema is located in `mq/sql/latest.sql`.

### Go Client Development

```bash
cd postgremq-go

# Download dependencies
go mod download

# Run tests
go test -v ./...

# Run tests with coverage
go test -v -coverprofile=coverage.out ./...

# View coverage
go tool cover -html=coverage.out
```

### TypeScript Client Development

```bash
cd postgremq-ts

# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Run tests with coverage
npm run test:coverage

# Run tests in watch mode
npm run test:watch
```

## How to Contribute

### Types of Contributions

We welcome many types of contributions:

- **Bug fixes**: Fix issues in existing code
- **New features**: Add new functionality
- **Documentation**: Improve or add documentation
- **Tests**: Add or improve test coverage
- **Performance improvements**: Optimize existing code
- **Examples**: Add usage examples
- **Client libraries**: Add support for other languages (Python, Rust, etc.)

### Before You Start

1. **Check existing issues**: Look for existing issues or feature requests
2. **Create an issue**: If you're working on something new, create an issue first to discuss it
3. **Get feedback**: Wait for feedback from maintainers before starting large changes

## Coding Guidelines

### SQL (PostgreSQL Functions)

- Use lowercase with underscores for function and table names
- Add comments explaining complex logic
- Use proper error handling with `RAISE EXCEPTION`
- Ensure backward compatibility when modifying existing functions
- Use transactions where appropriate
- Follow PostgreSQL best practices

### Go

- Follow standard Go conventions and formatting (`gofmt`, `golint`)
- Write clear, self-documenting code with comments for public APIs
- Use meaningful variable and function names
- Add package-level documentation in `doc.go`
- Handle errors explicitly - never ignore errors
- Use contexts for cancellation and timeouts
- Write table-driven tests where appropriate
- Example format:

```go
// ExampleConnection_Publish demonstrates how to publish messages.
func ExampleConnection_Publish() {
    // Example code here
}
```

### TypeScript

- Use TypeScript's type system - avoid `any` types
- Follow consistent naming conventions (camelCase for variables/functions)
- Use async/await for asynchronous operations
- Add JSDoc comments for public APIs
- Use meaningful variable and function names
- Export types for public APIs
- Write unit tests for all new functionality

### General Guidelines

- Keep pull requests focused on a single concern
- Write clear commit messages (see below)
- Add tests for new functionality
- Update documentation when changing behavior
- Ensure all tests pass before submitting PR

## Testing

All code changes should include appropriate tests.

### SQL Tests

Tests are located in `mq/tests/tests.py` and use pytest with testcontainers.

```bash
cd mq
pytest tests/tests.py -v
```

### Go Tests

Tests follow Go's standard testing conventions. Integration tests use testcontainers.

```bash
cd postgremq-go

# All tests
go test -v ./...

# Specific test
go test -v -run TestConsumer_BasicFlow

# Parallel tests
go test -v -parallel 4 ./...

# Short mode (skip long tests)
go test -short -v ./...
```

### TypeScript Tests

Tests use Jest and testcontainers.

```bash
cd postgremq-ts

# All tests
npm test

# Watch mode
npm run test:watch

# Coverage
npm run test:coverage

# Integration tests only
npm run test:integration
```

### Test Coverage

We aim for high test coverage but prioritize meaningful tests over coverage percentages. Focus on:

- Edge cases
- Error handling
- Concurrent access scenarios
- Transaction behavior
- Message lifecycle (publish, consume, ack, nack, release)

## Pull Request Process

### 1. Fork and Create a Branch

```bash
git checkout -b feature/my-new-feature
# or
git checkout -b fix/bug-description
```

### 2. Make Your Changes

- Write clean, well-documented code
- Add or update tests
- Update documentation as needed
- Ensure all tests pass locally

### 3. Commit Your Changes

Follow conventional commit format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Build process or auxiliary tool changes

Examples:

```
feat(go): add batch publish support

Implemented batch publishing to reduce database round-trips.
Includes new PublishBatch method with transaction support.

Closes #123
```

```
fix(ts): resolve visibility timeout extension race condition

Fixed race condition where messages could timeout during extension.
Added mutex to protect concurrent extension requests.

Fixes #456
```

### 4. Push to Your Fork

```bash
git push origin feature/my-new-feature
```

### 5. Create Pull Request

- Fill out the pull request template
- Reference related issues
- Describe what changed and why
- Include screenshots for UI changes (if applicable)
- Ensure CI passes

### 6. Code Review

- Address review feedback promptly
- Keep discussions focused and professional
- Be open to suggestions and alternative approaches

### 7. Merge

Once approved, a maintainer will merge your PR.

## Reporting Bugs

### Before Submitting a Bug Report

- Check existing issues to avoid duplicates
- Verify the bug exists in the latest version
- Collect relevant information (versions, logs, etc.)

### Submitting a Bug Report

Create an issue with:

- **Clear title**: Summarize the problem
- **Description**: Detailed description of the issue
- **Steps to reproduce**: Step-by-step instructions
- **Expected behavior**: What should happen
- **Actual behavior**: What actually happens
- **Environment**:
  - PostgreSQL version
  - Go version (if applicable)
  - Node.js version (if applicable)
  - Operating system
- **Logs/Screenshots**: Relevant error messages or screenshots
- **Possible fix**: If you have ideas on how to fix it

## Suggesting Enhancements

### Before Submitting an Enhancement

- Check if it already exists or is planned
- Consider if it fits the project's scope and goals
- Think about backward compatibility

### Submitting an Enhancement

Create an issue with:

- **Clear title**: Summarize the enhancement
- **Use case**: Describe the problem this solves
- **Proposed solution**: How you envision it working
- **Alternatives**: Other approaches you've considered
- **Additional context**: Any other relevant information

## Development Workflow

### Typical Workflow for SQL Schema Changes

1. Modify `mq/sql/latest.sql`
2. Update embedded SQL in `mq/sql.go` if needed
3. Run SQL tests: `pytest tests/tests.py -v`
4. Update client libraries if schema changes affect them
5. Run client tests
6. Update documentation

### Typical Workflow for Go Client Changes

1. Modify Go source files
2. Update or add tests
3. Run tests: `go test -v ./...`
4. Update documentation/examples if needed
5. Run `gofmt -s -w .`
6. Verify test coverage

### Typical Workflow for TypeScript Client Changes

1. Modify TypeScript source files
2. Update or add tests
3. Run tests: `npm test`
4. Build: `npm run build`
5. Update documentation/examples if needed
6. Verify no type errors: `tsc --noEmit`

## Communication

- **GitHub Issues**: For bugs, features, and discussions
- **GitHub Discussions**: For questions and general discussions
- **Pull Requests**: For code review and contributions

## Recognition

Contributors will be recognized in:

- Release notes
- GitHub contributors list
- CHANGELOG.md for significant contributions

## Questions?

If you have questions about contributing:

1. Check existing documentation
2. Search closed issues
3. Ask in GitHub Discussions
4. Create an issue with the `question` label

## License

By contributing to PostgreMQ, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to PostgreMQ!
