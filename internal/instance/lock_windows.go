//go:build windows

package instance

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

type platformLock struct {
	overlapped windows.Overlapped
}

func acquirePlatformLock(file *os.File) (platformLock, error) {
	platform := platformLock{}
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&platform.overlapped,
	)
	return platform, err
}

func releasePlatformLock(file *os.File, platform platformLock) error {
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &platform.overlapped)
}

func isAlreadyRunningError(err error) bool {
	return errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}
