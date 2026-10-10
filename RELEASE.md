# Release Process

How PostgreMQ's components are versioned and published.

## Components

| Component | Published as | Tag |
|-----------|--------------|-----|
| SQL schema (`mq/`) | Go module `github.com/slavakl/postgremq/mq` (embeds `sql/latest.sql` and `migrations/`) | `mq/vX.Y.Z` |
| Go client (`postgremq-go/`) | Go module `github.com/slavakl/postgremq/postgremq-go` | `postgremq-go/vX.Y.Z` |
| CLI (`cmd/postgremq/`) | Go module `github.com/slavakl/postgremq/cmd/postgremq` | `cmd/postgremq/vX.Y.Z` |
| TypeScript client (`postgremq-ts/`) | npm package `postgremq` | covered by `vX.Y.Z` |
| Rust client (`postgremq-rs/`) | crates.io crate `postgremq` | covered by `vX.Y.Z` |

All components share one version number and are released together. The
repository tag `vX.Y.Z` marks the release; the Go modules also need their
path-prefixed tags, because Go resolves a module in a subdirectory only from
tags carrying that prefix.

The first public release is **0.2.0**: the npm name `postgremq` already has a
0.1.0 release, so every component starts at 0.2.0 to stay aligned.

## Versioning

PostgreMQ follows [Semantic Versioning](https://semver.org/). Before 1.0, a
MINOR bump may contain breaking changes; they are listed under a **Breaking**
heading in `CHANGELOG.md`.

### The SQL schema

`mq/sql/latest.sql` is the complete current schema, used for fresh installs.
`mq/migrations/` holds the same schema as numbered
[golang-migrate](https://github.com/golang-migrate/migrate) migrations, used to
upgrade existing installs (the Go client's `Migrate` and the CLI's `migrate`
command apply them).

- Until the first release, `000001_initial_schema.up.sql` is edited in place
  and must stay byte-identical to `latest.sql`.
- After a release, published migrations are never edited. Each schema change
  adds a new `0000NN_<name>.up.sql` and the same change is applied to
  `latest.sql`, so that a fresh `latest.sql` install equals the result of
  running every migration. The SQL test suite should verify this.
- Down migrations are not supported: each `.down.sql` is a comment-only
  placeholder. `Migrate` / `postgremq migrate --target` do not check the
  direction, so a target below the current version would run those
  placeholders and record the lower version without changing the schema.
  Removing an installation is `DROP SCHEMA postgremq CASCADE`.
- A schema change that existing clients cannot work with is a breaking
  change for every component.

## One-time setup for the new repository

The code currently uses the module path `github.com/slavakl/postgremq`. Before
the first release from a new repository:

1. **Rename the module path** if the repository lives elsewhere (replace
   `NEW` with the new path, e.g. `github.com/acme/postgremq`):

   ```bash
   git grep -l 'github.com/slavakl/postgremq' \
     | xargs sed -i '' 's#github.com/slavakl/postgremq#NEW#g'   # GNU sed: sed -i
   ```

   This covers the Go module paths and imports, `go.mod` replace directives,
   `package.json` (`repository`, `bugs`, `homepage`), `Cargo.toml`
   (`repository`), and links and badges in the documentation. Then run
   `go mod tidy` in `mq/`, `postgremq-go/`, `postgremq-go/examples/metrics/`
   and `cmd/postgremq/`, and `go work sync` at the root.
2. **Repository settings**: enable private vulnerability reporting
   (Security → Settings), which `SECURITY.md` relies on; protect `main`
   (required CI checks, reviews); set the description and topics; enable
   Dependabot alerts.
3. **Registry accounts**: an npm account that owns `postgremq`, and a
   crates.io account (the first `cargo publish` claims the crate name).
4. Search once more for leftovers: `git grep -n 'slavakl'`.

## Pre-release checklist

- [ ] CI is green on `main` (SQL, Go, TypeScript, Rust, observability).
- [ ] `CHANGELOG.md`: move the `Unreleased` entries under `## [X.Y.Z] - YYYY-MM-DD`.
- [ ] Versions bumped:
  - `postgremq-ts/package.json` and `package-lock.json` (`npm version X.Y.Z --no-git-tag-version`);
  - `postgremq-rs/Cargo.toml` (and `Cargo.lock` via `cargo update -p postgremq`);
  - version numbers in install snippets in the READMEs.
- [ ] If the schema changed since the last release, a new migration exists
  and matches `latest.sql` (see above).
- [ ] Vulnerability scans are clean:
  ```bash
  (cd postgremq-go && govulncheck ./...)
  (cd cmd/postgremq && govulncheck ./...)
  (cd postgremq-ts && npm audit --omit=dev)
  (cd postgremq-rs && cargo audit)
  ```
- [ ] Packages build and contain what they should:
  ```bash
  (cd postgremq-ts && npm pack --dry-run)
  (cd postgremq-rs && cargo package --list && cargo publish --dry-run)
  ```

## Publishing

Release from an up-to-date `main` after the release commit is merged. Each
block below starts at the repository root. With `main` protected, every commit
goes through a pull request; tags are pushed on the merged commit.

### 1. Go modules, in dependency order

The Go modules depend on each other (`cmd/postgremq` → `postgremq-go` →
`mq`). Locally the `go.work` workspace and `replace` directives point them at
each other's directories. Published versions must require tagged versions.
`go install …@version` also refuses a module whose `go.mod` has `replace`
directives.

```bash
# 1. The SQL module: tag main as it is.
git tag mq/vX.Y.Z && git push origin mq/vX.Y.Z

# 2. The client: require the tagged mq in a PR; after it merges, tag the merge.
git switch -c release/go-vX.Y.Z
(cd postgremq-go \
  && go mod edit -require=github.com/slavakl/postgremq/mq@vX.Y.Z \
  && GOWORK=off go mod tidy && GOWORK=off go test ./...)
git commit -am "chore(go): require mq vX.Y.Z"   # open a PR, merge it, then:
git switch main && git pull
git tag postgremq-go/vX.Y.Z && git push origin postgremq-go/vX.Y.Z

# 3. The CLI: require the tagged modules and drop its replace directives in a
#    PR; after it merges, tag the merge.
git switch -c release/cli-vX.Y.Z
(cd cmd/postgremq \
  && go mod edit -dropreplace=github.com/slavakl/postgremq/postgremq-go \
                 -dropreplace=github.com/slavakl/postgremq/mq \
                 -require=github.com/slavakl/postgremq/postgremq-go@vX.Y.Z \
                 -require=github.com/slavakl/postgremq/mq@vX.Y.Z \
  && GOWORK=off GOPROXY=direct go mod tidy && GOWORK=off go build ./...)
git commit -am "chore(cli): require postgremq-go vX.Y.Z"   # PR, merge, then:
git switch main && git pull
git tag cmd/postgremq/vX.Y.Z && git push origin cmd/postgremq/vX.Y.Z
```

After the CLI drops its `replace` directives, local development still resolves
both modules from the working tree through `go.work`.

`postgremq-go/go.mod` keeps its `replace` for `mq`: downstream builds ignore
the `replace` directives of dependencies, and CI tests the module with
`GOWORK=off`. After a release, verify that the modules resolve:

```bash
GOFLAGS=-mod=mod go list -m github.com/slavakl/postgremq/postgremq-go@vX.Y.Z
go install github.com/slavakl/postgremq/cmd/postgremq@vX.Y.Z
```

### 2. npm

```bash
(cd postgremq-ts && npm ci && npm test && npm publish)
# prepublishOnly runs the build; the package ships dist/, README.md and LICENSE
```

### 3. crates.io

```bash
(cd postgremq-rs && cargo publish)
```

### 4. Repository tag and GitHub release

```bash
git tag vX.Y.Z && git push origin vX.Y.Z
```

Create a GitHub release from `vX.Y.Z` with the `CHANGELOG.md` section as its
notes.

## Patch releases

Fix on `main` and release the next PATCH version of every component. If a
component has no changes it is still tagged and published, so that one version
number identifies a compatible set.

## Security releases

Security fixes follow `SECURITY.md`: the fix is developed in a private GitHub
security advisory, released as a PATCH version, and the advisory is published
with the release.

## Yanking a bad release

- npm: `npm deprecate postgremq@X.Y.Z "<reason>"` (unpublishing is only
  possible within 72 hours and is discouraged).
- crates.io: `cargo yank --version X.Y.Z postgremq`.
- Go: add a `retract` directive for the version to the module's `go.mod` and
  release a new version.

Then release a fixed version.
