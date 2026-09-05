package mango

import _ "embed"

// exampleConfig is the complete sample configuration shipped with Mango.
// Keeping the sample embedded lets the CLI initialize a project after the
// repository source tree is no longer available.
//
//go:embed mango.example.yaml
var exampleConfig string

// ExampleConfig returns the complete sample YAML configuration.
func ExampleConfig() string {
	return exampleConfig
}
