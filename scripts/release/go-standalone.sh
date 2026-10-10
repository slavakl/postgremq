#!/usr/bin/env bash
# Runs a go command in one of this repository's published modules as a
# consumer would build it: GOWORK=off, with postgremq.dev dependencies from a
# local module proxy (scripts/release/go-proxy.sh) instead of the workspace.
#
#   scripts/release/go-standalone.sh postgremq-go go test ./...
#   scripts/release/go-standalone.sh --write postgremq-go go mod tidy
#
# A dependency whose release tag exists is built from the tag and checked
# against the committed go.sum (-mod=readonly). An unreleased one, or a released
# one whose hash is not committed yet, has no hash to check, so the command
# then runs on a temporary copy of go.mod/go.sum (-modfile) and the committed
# files are never changed. --write runs on the committed files, to record a
# released dependency's hashes after bumping it; it refuses unreleased ones.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
write=0
if [ "$1" = --write ]; then
  write=1
  shift
fi
dir=$1
shift

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
while IFS='=' read -r key value; do
  export "$key=$value"
done < <("$root/scripts/release/go-proxy.sh" "$work/proxy")

cd "$root/$dir"
if [ "$write" = 1 ]; then
  if [ "${GOFLAGS:-}" = "-mod=mod" ]; then
    echo "go-standalone: --write needs released postgremq.dev dependencies (tags); see the go-proxy output above" >&2
    exit 1
  fi
  unset GOFLAGS
  exec "$@"
fi
# A released dependency whose hash is not committed yet (the go.sum update
# follows the dependency's release) is also resolved in temporary files;
# verify_release.py requires the committed hash before this module's release.
missing_sum=0
while read -r module version; do
  grep -q "^$module $version h1:" go.sum 2>/dev/null || missing_sum=1
done < <(awk '/^\tpostgremq\.dev\// || /^require postgremq\.dev\// { if ($1 == "require") print $2, $3; else print $1, $2 }' go.mod)
if [ "$missing_sum" = 1 ] && [ "${GOFLAGS:-}" != "-mod=mod" ]; then
  echo "go-standalone: go.sum lacks a released postgremq.dev dependency's hash; not checking go.sum" >&2
  GOFLAGS=-mod=mod
fi
if [ "${GOFLAGS:-}" = "-mod=mod" ]; then
  cp go.mod "$work/go.mod"
  cp go.sum "$work/go.sum" 2>/dev/null || true
  export GOFLAGS="-mod=mod -modfile=$work/go.mod"
  echo "go-standalone: unreleased postgremq.dev dependencies; using a temporary go.mod/go.sum" >&2
else
  unset GOFLAGS
fi
"$@"
