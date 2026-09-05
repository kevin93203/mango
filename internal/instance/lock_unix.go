//go:build !windows

package instance

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type platformLock struct{}

func acquirePlatformLock(file *os.File) (platformLock, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	return platformLock{}, err
}

func releasePlatformLock(file *os.File, _ platformLock) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}

func isAlreadyRunningError(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}
