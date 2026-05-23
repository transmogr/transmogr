// Package migrationfiles provides embedded SQL migration assets.
package migrationfiles

import "embed"

// Files contains the SQL migration assets bundled into the binary.
//
//go:embed *.sql
var Files embed.FS
