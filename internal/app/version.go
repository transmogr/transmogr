// Package app wires the top-level application components.
package app

// Version is the application version, set at build time via
// -ldflags "-X github.com/transmogr/transmogr/internal/app.Version=<tag>".
// Falls back to "dev" when not injected.
var Version = "dev"
