#!/usr/bin/env bash
# Builds a GOPROXY file tree holding this repository's Go modules, so builds
# with GOWORK=off and installs from an external module resolve postgremq.dev
# paths without the vanity domain:
#
#   scripts/release/go-proxy.sh OUT_DIR [DIR@vVERSION ...]
#
# It always adds the versions the published modules require: postgremq.dev/mq
# as required by postgremq-go/go.mod, and postgremq.dev/postgremq-go as
# required by cmd/postgremq/go.mod. Arguments add more, e.g.
# `cmd/postgremq@v0.2.0`.
#
# A module whose release tag (DIR/vVERSION) exists is built from that tag, so
# its go.sum hash is the published one. Otherwise it is built from the working
# tree's files that git tracks or would track (dev mode: unreleased). Prints the env to use; with any
# dev module GOFLAGS=-mod=mod lets go.sum gain the unreleased hashes.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
out=$(mkdir -p "$1" && cd "$1" && pwd)
shift

required() { # required MODFILE MODULE -> version
  awk -v m="$2" '$1 == m { print $2; exit } $1 == "require" && $2 == m { print $3; exit }' "$1"
}

specs=(
  "mq@$(required "$root/postgremq-go/go.mod" postgremq.dev/mq)"
  "postgremq-go@$(required "$root/cmd/postgremq/go.mod" postgremq.dev/postgremq-go)"
  "$@"
)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
args=()
dev=0
for spec in "${specs[@]}"; do
  dir=${spec%@*}
  version=${spec#*@}
  [ -n "$version" ] || { echo "go-proxy: no version for $dir" >&2; exit 1; }
  module=$(awk '$1 == "module" { print $2; exit }' "$root/$dir/go.mod")
  tag="$dir/$version"
  src="$work/$dir@$version"
  mkdir -p "$src"
  # Only a release in this branch's history counts.
  if git -C "$root" rev-parse -q --verify "refs/tags/$tag" >/dev/null &&
    git -C "$root" merge-base --is-ancestor "$tag" HEAD; then
    git -C "$root" archive "$tag" "$dir" | tar -x -C "$src" --strip-components="$(tr -cd / <<<"$dir/" | wc -c)"
    echo "go-proxy: $module@$version from tag $tag" >&2
  else
    (cd "$root" && git ls-files -z --cached --others --exclude-standard -- "$dir" | while IFS= read -r -d '' file; do
      [ -e "$file" ] || continue
      mkdir -p "$src/$(dirname "${file#"$dir"/}")"
      cp "$file" "$src/${file#"$dir"/}"
    done)
    dev=1
    echo "go-proxy: $module@$version from the working tree (unreleased)" >&2
  fi
  args+=("$module@$version=$src")
done

(cd "$root/scripts/release/gomodproxy" && GOWORK=off go run . -out "$out" "${args[@]}")

# The module cache treats a version as immutable, but an unreleased version
# built from the working tree changes, and a cached dev build could shadow a
# later tag of the same version. Drop this repository's modules from it so
# the proxy's copies are used (they are small).
modcache=$(go env GOMODCACHE)
for dir in "$modcache/postgremq.dev" "$modcache/cache/download/postgremq.dev"; do
  if [ -e "$dir" ]; then
    chmod -R u+w "$dir"
    rm -rf "$dir"
  fi
done

echo "GOPROXY=file://$out,https://proxy.golang.org,direct"
echo "GONOSUMDB=postgremq.dev"
echo "GOWORK=off"
if [ "$dev" = 1 ]; then
  echo "GOFLAGS=-mod=mod"
fi
