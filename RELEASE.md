# Release Process

This document describes the release process for PostgreMQ.

## Versioning Strategy

PostgreMQ follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html):

- **MAJOR** version (x.0.0): Incompatible API changes
- **MINOR** version (0.x.0): New functionality in a backward compatible manner
- **PATCH** version (0.0.x): Backward compatible bug fixes

### Component Versioning

All three components (mq, postgremq-go, postgremq-ts) are versioned together for simplicity:

- They share the same version number
- Releases are coordinated across all components
- CHANGELOG.md tracks changes for all components

### Version Compatibility

- **Go client** and **TypeScript client** should be compatible with the same SQL schema version
- Breaking changes to the SQL schema require MAJOR version bump
- Client-only breaking changes may bump MINOR version if other clients are unaffected

## Pre-Release Checklist

### Code Quality

- [ ] All tests passing (Go, TypeScript, SQL/Python)
  ```bash
  # Go tests
  cd postgremq-go && go test -v ./...

  # TypeScript tests
  cd postgremq-ts && npm test

  # SQL tests
  cd mq && pytest tests/tests.py -v
  ```

- [ ] No linting errors
  ```bash
  # Go
  cd postgremq-go && golangci-lint run

  # TypeScript
  cd postgremq-ts && npm run lint
  ```

- [ ] Code formatted properly
  ```bash
  # Go
  gofmt -s -w .

  # TypeScript
  npm run format
  ```

- [ ] All examples work
- [ ] Documentation is up to date

### Version Updates

- [ ] Update version in `postgremq-ts/package.json`
- [ ] Update version references in documentation
- [ ] Update `CHANGELOG.md` with release date and version
- [ ] Move items from `[Unreleased]` to new version section in CHANGELOG

### Documentation

- [ ] README.md is accurate and up to date
- [ ] All component READMEs are updated
- [ ] API documentation is complete (Go doc.go, TypeScript JSDoc)
- [ ] Examples are tested and working
- [ ] CHANGELOG.md is complete with all notable changes
- [ ] Migration guide exists (if breaking changes)

### Dependencies

- [ ] Go dependencies are up to date and secure
  ```bash
  go list -m -u all
  go mod tidy
  ```

- [ ] TypeScript dependencies are up to date and secure
  ```bash
  npm audit
  npm outdated
  ```

- [ ] No known security vulnerabilities

## Release Process

### 1. Prepare Release Branch

```bash
git checkout main
git pull origin main
git checkout -b release/v0.x.0
```

### 2. Update Version Numbers

**TypeScript (postgremq-ts/package.json)**:
```json
{
  "version": "0.x.0"
}
```

**Go** uses git tags, no file changes needed.

### 3. Update CHANGELOG.md

```markdown
## [0.x.0] - 2025-MM-DD

### Added
- Feature 1
- Feature 2

### Changed
- Change 1

### Fixed
- Bug fix 1
```

### 4. Commit Changes

```bash
git add .
git commit -m "chore: prepare release v0.x.0"
git push origin release/v0.x.0
```

### 5. Create Pull Request

- Create PR from `release/v0.x.0` to `main`
- Title: "Release v0.x.0"
- Include release notes from CHANGELOG
- Wait for CI to pass
- Get review approval
- Merge to main

### 6. Tag Release

```bash
git checkout main
git pull origin main
git tag -a v0.x.0 -m "Release version 0.x.0"
git push origin v0.x.0
```

### 7. Create GitHub Release

1. Go to https://github.com/slavakl/postgremq/releases/new
2. Select tag `v0.x.0`
3. Release title: `v0.x.0`
4. Copy release notes from CHANGELOG.md
5. Check "Set as the latest release"
6. Publish release

### 8. Publish Packages

#### Publish TypeScript to npm

```bash
cd postgremq-ts
npm login
npm publish
```

#### Publish Go Module

Go modules are published automatically via git tags. Verify at:
```
https://pkg.go.dev/github.com/slavakl/postgremq/postgremq-go@v0.x.0
```

It may take a few minutes for pkg.go.dev to index the new version.

### 9. Verify Release

- [ ] npm package is available: `npm info postgremq`
- [ ] Go module is available: Check pkg.go.dev
- [ ] GitHub release is published
- [ ] Documentation is accessible
- [ ] Installation instructions work

### 10. Announce Release

- [ ] Post in GitHub Discussions
- [ ] Tweet/social media (if applicable)
- [ ] Update any external documentation

## Hotfix Process

For critical bugs in production:

### 1. Create Hotfix Branch

```bash
git checkout v0.x.0  # Checkout the release tag
git checkout -b hotfix/v0.x.1
```

### 2. Make Fix

- Fix the bug
- Add tests
- Update CHANGELOG.md

### 3. Version Bump

Update patch version (0.x.1)

### 4. Release

Follow steps 4-10 from regular release process.

### 5. Merge Back to Main

```bash
git checkout main
git merge hotfix/v0.x.1
git push origin main
```

## Breaking Changes

When introducing breaking changes:

### 1. Document in CHANGELOG

Clearly mark breaking changes:

```markdown
### Breaking Changes

- **[go]** Renamed `Consume()` to `Subscribe()` (#123)
  - **Migration**: Replace all `Consume()` calls with `Subscribe()`

- **[sql]** Removed `lock_timeout` column from queue_messages (#124)
  - **Migration**: Run migration script `migrations/v2.0.0.sql`
```

### 2. Provide Migration Guide

Create `MIGRATION.md` or add section to CHANGELOG with:

- What changed
- Why it changed
- Step-by-step migration instructions
- Code examples (before/after)

### 3. Version Bump

- Breaking changes require MAJOR version bump
- Update version to next major (e.g., 0.x.0 → 1.0.0)

### 4. Deprecation Period (if possible)

For non-urgent breaking changes:

1. Mark old API as deprecated in current version
2. Add warnings/logs when deprecated API is used
3. Wait for at least one MINOR version
4. Remove in next MAJOR version

## Release Cadence

- **PATCH releases**: As needed for critical bugs
- **MINOR releases**: Monthly or when significant features are ready
- **MAJOR releases**: When necessary for breaking changes

## Post-Release

- [ ] Monitor for issues
- [ ] Watch for bug reports
- [ ] Respond to questions in Discussions/Issues
- [ ] Plan next release

## Rollback Procedure

If a release has critical issues:

### 1. Unpublish npm Package (if needed)

```bash
npm unpublish postgremq@0.x.0
```

**Note**: npm unpublish is only possible within 72 hours and if package is not widely used.

### 2. Mark GitHub Release as Pre-release

Edit the GitHub release and check "This is a pre-release"

### 3. Communicate

- Create GitHub issue explaining the problem
- Post in Discussions
- Update release notes with warning

### 4. Fix and Re-release

- Fix the issue
- Bump patch version
- Release as normal

## Security Releases

For security vulnerabilities:

1. **Do not** create public issue or PR
2. Follow SECURITY.md reporting process
3. Prepare fix in private
4. Coordinate disclosure with reporter
5. Release fix quickly
6. Publish security advisory after release

## Version Support

- **Current major version**: Full support
- **Previous major version**: Security fixes only for 6 months
- **Older versions**: No support

## Questions?

For questions about the release process, open a discussion or contact the maintainers.
