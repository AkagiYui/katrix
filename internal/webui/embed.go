// Package webui embeds the built web panel (Vite output) into the binary.
// The dist directory is produced by `pnpm build` in ../../web and is git
// ignored; a placeholder index.html is committed so the package always builds.
package webui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// FS returns the embedded dist directory rooted at its top level.
func FS() (fs.FS, error) {
	return fs.Sub(dist, "dist")
}
