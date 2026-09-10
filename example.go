package mango

import (
	"embed"
	"io/fs"
)

// exampleConfig is the starter configuration shipped with Mango.
// Keeping the sample embedded lets the CLI initialize a project after the
// repository source tree is no longer available.
//
//go:embed mango.example.yaml
var exampleConfig string

//go:embed examples/api/main.go examples/one-task/main.go examples/tasks/emit.go examples/tasks/artifact/main.go
var exampleFiles embed.FS

// ExampleConfig returns the complete sample YAML configuration.
func ExampleConfig() string {
	return exampleConfig
}

// ExampleFiles returns the executable Go source files copied by mango init.
func ExampleFiles() fs.FS {
	return exampleFiles
}
