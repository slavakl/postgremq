#!/usr/bin/env bash
# Installs the Go client and the CLI as an external module would, with
# GOWORK=off from a module proxy (scripts/release/go-proxy.sh), and runs them
# against a fresh database:
#
#   scripts/release/smoke-go.sh [CLI_VERSION [CLIENT_VERSION [MQ_VERSION]]]
#
# CLI_VERSION (default v0.0.0-smoke, built from this tree) names the CLI, e.g.
# v0.2.0 at its tag. CLIENT_VERSION / MQ_VERSION (default: the versions the
# CLI and the client require) pin the client and mq modules the external
# module uses, e.g. the version being published at its tag.
set -euo pipefail
root=$(git rev-parse --show-toplevel)
source "$root/scripts/release/smoke-lib.sh"
cli_version=${1:-v0.0.0-smoke}
client_version=${2:-$(awk '$1 == "postgremq.dev/postgremq-go" { print $2; exit }' "$root/cmd/postgremq/go.mod")}
mq_version=${3:-$(awk '$1 == "postgremq.dev/mq" { print $2; exit }' "$root/postgremq-go/go.mod")}

while IFS='=' read -r key value; do
  [ "$key" = GOFLAGS ] || export "$key=$value"
done < <("$root/scripts/release/go-proxy.sh" "$SMOKE_WORK/proxy" \
  "cmd/postgremq@$cli_version" "postgremq-go@$client_version" "mq@$mq_version")

mkdir -p "$SMOKE_WORK/app"
cp "$root/scripts/release/smoke/go/main.go" "$SMOKE_WORK/app/"
cd "$SMOKE_WORK/app"
go mod init example.com/postgremq-smoke >/dev/null 2>&1
go get "postgremq.dev/postgremq-go@$client_version" "postgremq.dev/mq@$mq_version" github.com/jackc/pgx/v5
# Minimal version selection may pick a newer mq than asked; say which ran.
go list -m postgremq.dev/postgremq-go postgremq.dev/mq
go mod tidy
smoke_database pmq_smoke_go
go run .

GOBIN="$SMOKE_WORK/bin" go install "postgremq.dev/cmd/postgremq@$cli_version"
smoke_database pmq_smoke_cli
"$SMOKE_WORK/bin/postgremq" migrate --dsn "$SMOKE_DATABASE_URL"
"$SMOKE_WORK/bin/postgremq" status --dsn "$SMOKE_DATABASE_URL" | tee "$SMOKE_WORK/status"
grep -q "Database is up to date" "$SMOKE_WORK/status"
echo "go cli smoke ok: postgremq.dev/cmd/postgremq@$cli_version"
