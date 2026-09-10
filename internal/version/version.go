// Package version contains build metadata shared by the Mango binaries.
package version

import "fmt"

// These values are replaced by release builds through Go ldflags. Keeping
// useful defaults makes locally built binaries and tests self-describing.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// BuildInfo is safe to expose through health and diagnostic APIs. It contains
// no host paths or credentials.
type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

func Current() BuildInfo {
	return BuildInfo{Version: Version, Commit: Commit, BuildDate: BuildDate}
}

func String() string {
	return fmt.Sprintf("%s (commit=%s, build_date=%s)", Version, Commit, BuildDate)
}
