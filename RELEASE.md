# Releasing PostgreMQ

PostgreMQ's components are versioned and released independently, by
[Release Please](https://github.com/googleapis/release-please) running in
`.github/workflows/release.yml`. Releasing a component means merging its
release PR; tags, GitHub releases and registry publishing follow
automatically.

## Components

| Component | Directory | Version source | Tag | Published as |
|-----------|-----------|----------------|-----|--------------|
| SQL implementation and Go `mq` module | `mq/` | `mq/VERSION` | `mq/vX.Y.Z` | Go module `postgremq.dev/mq` (embeds `sql/latest.sql` and `migrations/`) |
| Go client | `postgremq-go/` | the tag | `postgremq-go/vX.Y.Z` | Go module `postgremq.dev/postgremq-go` (package `postgremq`) |
| CLI | `cmd/postgremq/` | the tag | `cmd/postgremq/vX.Y.Z` | Go module `postgremq.dev/cmd/postgremq` |
| Rust client | `postgremq-rs/` | `Cargo.toml` | `rust/vX.Y.Z` | crates.io crate `postgremq` |
| TypeScript client | `postgremq-ts/` | `package.json` | `npm/vX.Y.Z` | npm package `postgremq` |

`release-please-config.json` defines them; `.release-please-manifest.json`
holds each one's last released version. Release PRs are rebuilt on every
run (`always-update`), so a release of one component (which edits the shared
manifest) never leaves the others conflicting. Release Please assigns commits to
components by the paths they change. Changes outside these directories (docs,
`observability/`, CI) release nothing.

## Versioning rules

- Semantic versioning per component. Client versions describe the public
  language API; the SQL version describes the database implementation
  (migrations, fixes, features). Versions need not match across components.
- After 1.0: incompatible API changes bump MAJOR, compatible features MINOR,
  fixes PATCH.
- Before 1.0 (now): a breaking change bumps MINOR, as does a feature
  (`bump-minor-pre-major: true`, `bump-patch-for-minor-pre-major: false`); a
  fix bumps PATCH. Breaking changes are listed under **⚠ BREAKING CHANGES** in
  the release notes, from the commit's `!` or `BREAKING CHANGE:` footer.
- Published versions and tags are immutable. A correction is a new release.

## The SQL implementation

### Protocol major and discovery

The protocol major is the contract clients rely on: SQL function signatures,
result types, error codes, ownership rules and delivery semantics. It is
currently **1**. Additive changes (a new function, an added optional
parameter with a default, an added result field) keep it; incompatible ones
need a new major.

```sql
SELECT postgremq.info();
-- {"schema_version": 7, "protocol_major": 1}
```

`schema_version` is the number of the last migration applied: the exact
schema and function state of the database. `info()` reads it from the version
table that every install path maintains (the migrators and `latest.sql`), so
nothing has to write it at release time. The mq release version is a
packaging version (the Go module tag and the changelog); each mq GitHub
release states the schema version it ships.

`postgremq.info()` and its two fields are stable across protocol majors;
fields may be added. Each client declares the majors it implements
(`SupportedProtocolMajors()` in Go, `SUPPORTED_PROTOCOL_MAJORS` in TypeScript
and Rust), checks `info()` when it connects, and rejects any other major with
a typed compatibility error that names the schema version, its major and the
supported majors (Go `*CompatibilityError` / `ErrIncompatibleSchema`,
TypeScript `CompatibilityError`, Rust `Error::Incompatible`). A database
without `info()` gets the same error saying it needs an installation or
upgrade; connection and permission errors keep their real cause. Within a
supported major, clients call SQL functions directly: a function missing from
an older installation fails with the database's own error (SQLSTATE 42883),
through normal error handling. A client may declare a second major only when
it implements and tests both contracts.

### Migrations and latest.sql

- `mq/migrations/` holds numbered [golang-migrate](https://github.com/golang-migrate/migrate)
  migrations. A migration's number is a schema version, so **a migration
  never changes once it is on `main`**: a schema or function change adds a
  new migration. CI fails a PR that modifies, deletes or renames an existing
  migration (`scripts/release/check_migrations_immutable.sh`); until the first
  mq release, `000001_initial_schema` may still be edited.
- The protocol major is a literal in `info()`; only a migration that breaks
  the client contract redefines `info()` with a new major (a breaking change
  for every client).
- `mq/sql/latest.sql` is the fresh-install script: everything the migrations
  produce, in one file. A guard at the top refuses to run when the version
  table `postgremq.postgremq_migrations` exists (an existing installation is
  upgraded with the migrations), and the end records the latest migration's
  number. Change it in the same PR as the migration.
- The SQL test suite (`mq/tests/tests.py`) checks that a fresh `latest.sql`
  install and a database migrated through every migration have identical
  schema dumps, version rows and `info()`; that `info()` reports the latest
  migration; that every migration at an `mq/v*` tag in the branch's history is
  unchanged; and that upgrading from each such release's `latest.sql` equals
  a fresh install.

## Dependencies between components

A client release ships a released SQL implementation, and the dependency is
explicit in the client's source:

| Client | Dependency | Where |
|--------|-----------|-------|
| Go client | `postgremq.dev/mq` | `postgremq-go/go.mod` (`require`), hashes in `go.sum` |
| CLI | `postgremq.dev/postgremq-go` | `cmd/postgremq/go.mod` (`require`), hashes in `go.sum` |
| Rust client | mq schema version | `[package.metadata.postgremq] mq-schema` in `postgremq-rs/Cargo.toml` |
| TypeScript client | mq schema version | `"postgremq": { "mq-schema": … }` in `postgremq-ts/package.json` |

The Rust and TypeScript clients embed migrations 1..N for their pinned schema
version N (`postgremq-rs/build.rs`, `postgremq-ts/scripts/embed-migrations.js`);
newer migrations on the branch are not embedded until the pin moves.
Published modules have no `replace` directives; locally `go.work` resolves the
Go modules from the working tree.

A release is refused (`scripts/release/verify_release.py`, on the release PR
and again before tagging) unless its dependency is a released version in the
branch's history, the Go `go.sum` has its hashes, and the Rust/TypeScript
pinned migrations are part of an mq release in the branch's history and
byte-identical to it. So when a
client needs new SQL: release mq first, then bump the client's dependency in a
commit that touches the client (which also queues its release):

```bash
# Go client (after mq/vX.Y.Z is tagged; resolves through a local module proxy
# until postgremq.dev discovery is live)
scripts/release/go-standalone.sh --write postgremq-go go get postgremq.dev/mq@vX.Y.Z
scripts/release/go-standalone.sh --write postgremq-go go mod tidy
# CLI, after postgremq-go/vX.Y.Z
scripts/release/go-standalone.sh --write cmd/postgremq go get postgremq.dev/postgremq-go@vX.Y.Z
scripts/release/go-standalone.sh --write cmd/postgremq go mod tidy
# Rust / TypeScript: set the pin to the released schema version, then commit,
# e.g. feat(rs): bundle mq schema 7
```

A SQL change does not release the clients by itself. A breaking SQL change
(new protocol major) is breaking for every client and needs their explicit
updates.

## Day-to-day development

- PRs are squash-merged. The PR title becomes the commit message on `main` and
  must be a [Conventional Commit](https://www.conventionalcommits.org/)
  (checked by `.github/workflows/pr-title.yml`); see CONTRIBUTING.md for the
  types and scopes.
- Feature PRs never change versions, `.release-please-manifest.json`, the
  component changelogs, or existing migrations.
- `feat` and `fix` (and `perf`, `revert`, `deps`) appear in release notes;
  `docs`, `test`, `ci`, `refactor`, `build` and `chore` do not, and do not
  trigger a release on their own.

## The release pipeline

On every push to `main`, `release.yml`:

1. **Validates** the pushed commit with the reusable test workflows (SQL, Go
   standalone with `GOWORK=off`, Rust, TypeScript, observability) and
   `packages.yml` (Go modules installed from an external module through a
   module proxy, the packed npm package, the packaged crate, each run against
   a fresh database). The `Validated` job passes only if all of them do.
2. **Guards** tagging (`scripts/release/check_pending_releases.py`): every
   merged release PR still labelled `autorelease: pending` must have a merge
   commit that passed validation (this run, or an earlier run's `Validated`
   job on that commit) and must pass `verify_release.py` at that commit.
   Otherwise nothing is tagged and the run fails (see *Correcting a pending
   release*). A release PR merged after the run's own commit is left to its
   own run: this run then tags nothing, without failing.
3. **Tags** each such merge commit and creates its GitHub release with the
   changelog section as notes (Release Please, `skip-github-pull-request`).
4. **Opens or updates** one release PR per component with unreleased
   `feat`/`fix` changes: version bump, `CHANGELOG.md` entry, manifest update
   (Release Please, `skip-github-release`). For an mq tag, the release notes
   also state the schema version it ships.
5. **Publishes** each new tag through `publish.yml`.

A failed run tags nothing and opens no PRs. Release boundaries come from the
last release tag, so the next passing run includes every unreleased change.

### Publishing (`publish.yml`)

For a tag it checks out the tag, verifies it (`verify_release.py --tag-exists`:
manifest, version files and changelog match the tag; dependencies released),
smoke-tests the package built from the tag, skips a version already in its
registry, and publishes:

- `rust/v*`: `cargo publish`;
- `npm/v*`: `npm publish --provenance` (pre-release versions under the `next`
  dist-tag);
- `mq/v*`, `postgremq-go/v*`, `cmd/postgremq/v*`: Go modules are published by
  the tag itself (which is why Go validation precedes tagging); the job asks
  `proxy.golang.org` to fetch the version.

Until the repository variable `PUBLISH_ENABLED` is `true`, every publish step
is a dry run (`cargo publish --dry-run`, `npm publish --dry-run`, no proxy
request).

**Manual retry**: Actions → Publish → Run workflow with the existing tag (e.g.
`npm/v0.2.0`). It also re-runs the component's tests on the tag, and skips a
version that is already published.

### Releasing a component

1. Wait for its release PR (`chore(main): release <component> X.Y.Z`). Edit
   nothing in it except, if needed, the changelog wording; Release Please
   rewrites the branch on the next push to `main`.
2. Make sure its checks pass. A client release PR fails `Release PR
   consistency` until its mq (or client) dependency is released and pinned;
   release that first.
3. Squash-merge it. The next `release.yml` run on that merge commit tags it,
   creates the GitHub release, and publishes.

Order for a release that spans components: mq → (bump pins) → Go client, Rust,
TypeScript → (bump the CLI's client requirement) → CLI.

### Correcting a pending release

If the guard refuses a merged release PR because its merge commit failed
validation:

- If the failure was spurious, re-run the failed jobs of that commit's
  `release.yml` run. When its `Validated` job succeeds, the next run on `main`
  tags it (push any commit, or re-run the latest run).
- If the commit is broken, do not tag it: remove the `autorelease: pending`
  label from the release PR, revert the release commit on `main`, and fix
  forward. Release Please then proposes a fresh release PR.

If it refuses because the release is inconsistent (for example a pin to an
unreleased mq), fix `main` the same way and let Release Please propose the
release again.

## First release

Independent versioning starts from the baseline in
`.release-please-manifest.json`: every component at `0.1.0` (npm `postgremq`
0.1.0 and the crates.io `postgremq` 0.0.1 placeholder exist from before;
nothing else was published), with `bootstrap-sha` limiting history to commits
after the switch. The first release notes' compare link points at the
never-created `<component>/v0.1.0` tag; edit it out of the release PR's
changelog if you like. The first release of each
component is therefore `0.2.0` (its commits include `feat`s). The Go client
requires mq `0.2.0` and the CLI the Go client `0.2.0`, so release mq first,
then the clients (after adding the Go `go.sum` hashes; the Rust and TypeScript
clients already pin schema 1), then the CLI.

## One-time setup

- **GitHub App for release PRs.** Release PRs opened with the default
  `GITHUB_TOKEN` trigger no workflows, so their checks never run. Create a
  GitHub App with *Contents*, *Pull requests* and *Issues* read/write and
  *Actions* read on this repository, install it, and set the variable
  `RELEASE_APP_ID` and the secret `RELEASE_APP_PRIVATE_KEY`. Without them
  release.yml falls back to `GITHUB_TOKEN` (with a warning); then also enable
  *Settings → Actions → General → Allow GitHub Actions to create and approve
  pull requests*.
- **Repository settings.** Allow squash merging only, with *Default commit
  message: Pull request title*; protect `main` and require the PR Title check
  and, on release PRs, *Release PR consistency*. The test and Packages
  workflows run on pull requests only when their paths change, so a required
  status from them would block unrelated PRs: require them only after
  removing their `paths:` filters. Enable private vulnerability reporting
  (SECURITY.md) and Dependabot alerts.
- **Registries.** Prefer trusted publishing (OIDC from `publish.yml`): on
  crates.io (the crate `postgremq` exists, owned by the maintainer) and npm
  (package `postgremq`), register this repository and workflow `publish.yml`,
  then set the variables `CRATES_IO_TRUSTED_PUBLISHING=true` /
  `NPM_TRUSTED_PUBLISHING=true`. Otherwise add scoped tokens as the secrets
  `CARGO_REGISTRY_TOKEN` and `NPM_TOKEN` (an automation token for
  `postgremq`).
- **Go module paths.** `postgremq.dev/mq`, `postgremq.dev/postgremq-go` and
  `postgremq.dev/cmd/postgremq` need `go-import` discovery on postgremq.dev
  (the website repository). Until it works, resolve them with
  `scripts/release/go-proxy.sh` / `go-standalone.sh`, which build a local
  module proxy from tags or the working tree.
- **Enable publishing**: set the variable `PUBLISH_ENABLED=true`.

## Tools

| Script | Purpose |
|--------|---------|
| `scripts/release/check_migrations_immutable.sh [BASE]` | Fails if a migration already on BASE changed (run on PRs) |
| `scripts/release/verify_release.py <tag>` / `--component <c>` | Checks a release's consistency |
| `scripts/release/check_pending_releases.py` | The release guard |
| `scripts/release/go-proxy.sh DIR` | Builds a GOPROXY tree of this repo's Go modules (from tags when released) |
| `scripts/release/go-standalone.sh [--write] DIR go …` | Runs a go command with `GOWORK=off` against that proxy (`--write` records released hashes in go.mod/go.sum) |
| `scripts/release/smoke-go.sh`, `smoke-npm.sh`, `smoke-rust.sh` | Installs the packages outside the repository and runs them (`SMOKE_ADMIN_URL`) |

## Security releases

Security fixes follow SECURITY.md: the fix is developed in a private GitHub
security advisory, released as a PATCH of each affected component, and the
advisory is published with the release.

## Yanking a bad release

Published versions are never reused. To withdraw one:

- npm: `npm deprecate postgremq@X.Y.Z "<reason>"`;
- crates.io: `cargo yank --version X.Y.Z postgremq`;
- Go: add a `retract` directive for the version to the module's `go.mod` and
  release a new version.

Then release a fixed version.
