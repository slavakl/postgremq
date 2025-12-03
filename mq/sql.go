package mq

import "embed"

//go:embed sql/latest.sql
var LatestSQL string

//go:embed migrations/*.sql
var MigrationsFS embed.FS
