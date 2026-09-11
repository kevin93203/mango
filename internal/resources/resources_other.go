//go:build !linux

package resources

import "fmt"

func preparePlatform(_ Policy) (*Handle, error) {
	return nil, fmt.Errorf("resource adapter is unsupported on this platform")
}

func attachPlatform(_ *Handle, _ int) error { return nil }

func cleanupPlatform(_ *Handle) error { return nil }
