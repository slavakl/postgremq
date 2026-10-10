# Changelog

Each component is versioned and released on its own (see
[RELEASE.md](./RELEASE.md)), with its own changelog:

| Component | Changelog | Tags |
|-----------|-----------|------|
| SQL implementation and Go `mq` module | [mq/CHANGELOG.md](./mq/CHANGELOG.md) | `mq/vX.Y.Z` |
| Go client | [postgremq-go/CHANGELOG.md](./postgremq-go/CHANGELOG.md) | `postgremq-go/vX.Y.Z` |
| CLI | [cmd/postgremq/CHANGELOG.md](./cmd/postgremq/CHANGELOG.md) | `cmd/postgremq/vX.Y.Z` |
| Rust client | [postgremq-rs/CHANGELOG.md](./postgremq-rs/CHANGELOG.md) | `rust/vX.Y.Z` |
| TypeScript client | [postgremq-ts/CHANGELOG.md](./postgremq-ts/CHANGELOG.md) | `npm/vX.Y.Z` |

GitHub releases carry the same notes per tag.

## Shared, unversioned parts

### Observability

- A shared client metrics contract (`docs/observability.md`,
  `observability/client-contract.json`), a tested OpenTelemetry Collector
  configuration, and runnable metrics examples for every client.
