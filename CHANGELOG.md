# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open source release preparation
- Comprehensive documentation (README, CONTRIBUTING, SECURITY)
- CI/CD workflows for automated testing

## [0.1.0] - 2025-02-XX

### Added

#### Core SQL Implementation (mq)
- PostgreSQL schema with topics, queues, messages, and queue_messages tables
- Dead letter queue (DLQ) support for failed messages
- Message visibility timeout mechanism (VT) instead of traditional locks
- Automatic message distribution via triggers
- Support for delayed message delivery
- Exclusive (temporary) and non-exclusive (persistent) queue types
- Keep-alive mechanism for exclusive queues
- Comprehensive set of SQL functions for queue operations
- Test suite using pytest and testcontainers

#### Go Client (postgremq-go)
- Connection management with pgx/v5 connection pooling
- Consumer with automatic message fetching
- Automatic visibility timeout extension using min-heap tracking
- LISTEN/NOTIFY event listener for real-time message notifications
- Transaction support for Publish and Ack operations
- Configurable retry logic with exponential backoff
- Graceful shutdown with in-flight message tracking
- Message acknowledgment modes: Ack, Nack, Release
- Support for delayed message delivery
- Comprehensive test suite with parallel test execution
- Examples and documentation

#### TypeScript Client (postgremq-ts)
- Connection management with node-postgres (pg) pooling
- Async iterator-based message consumption
- Automatic visibility timeout extension using sorted array tracking
- LISTEN/NOTIFY event listener for real-time notifications
- Transaction support for atomic operations
- Configurable retry logic for transient errors
- Graceful shutdown handling
- Message acknowledgment: ack, nack, release
- Support for delayed delivery
- Admin functions for queue/topic management
- Comprehensive test suite using Jest and testcontainers
- TypeScript type definitions
- Example code

### Features

- **Reliability**: ACID guarantees through PostgreSQL
- **Real-time**: Sub-second message delivery via LISTEN/NOTIFY
- **Scalability**: Support for multiple concurrent consumers
- **Flexibility**: Configurable visibility timeouts, retry policies, and batch sizes
- **Transaction Support**: Publish and acknowledge within database transactions
- **Auto-Extension**: Automatic visibility timeout extension during processing
- **Dead Letter Queue**: Automatic handling of repeatedly failed messages
- **No Additional Infrastructure**: Uses existing PostgreSQL database

### Documentation

- README files for root, Go client, and TypeScript client
- Package documentation (Go doc.go)
- Code examples
- Test examples
- CLAUDE.md with project architecture and patterns

## Version History

- **0.1.0** - Initial release

## Release Notes Format

Starting with version 0.1.0, all notable changes will be documented in this changelog.

### Categories

Changes are grouped by:

- **Added**: New features
- **Changed**: Changes in existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security vulnerability fixes

### Component Tags

Changes will be tagged by component:

- `[sql]` - Core SQL implementation
- `[go]` - Go client library
- `[ts]` - TypeScript client library
- `[docs]` - Documentation changes
- `[ci]` - CI/CD and tooling changes

[Unreleased]: https://github.com/slavakl/postgremq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/slavakl/postgremq/releases/tag/v0.1.0
