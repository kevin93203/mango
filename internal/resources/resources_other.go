//go:build !linux

package resources

import "fmt"

func applyPlatform(_ int, _ Policy) (*Handle, error) {
	return nil, fmt.Errorf("resource adapter is unsupported on this platform")
}

func cleanupPlatform(_ *Handle) error { return nil }
