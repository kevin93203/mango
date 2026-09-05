// Package instance provides process-level ownership for one Mango instance.
package instance

import (
	"errors"
	"os"
	"sync"
)

var ErrAlreadyRunning = errors.New("daemon is already running")

type Lock struct {
	file     *os.File
	platform platformLock
	once     sync.Once
	err      error
}

func Acquire(path string) (*Lock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	platform, err := acquirePlatformLock(file)
	if err != nil {
		_ = file.Close()
		if isAlreadyRunningError(err) {
			return nil, ErrAlreadyRunning
		}
		return nil, err
	}
	return &Lock{file: file, platform: platform}, nil
}

func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		if err := releasePlatformLock(l.file, l.platform); err != nil {
			l.err = err
		}
		if err := l.file.Close(); l.err == nil && err != nil {
			l.err = err
		}
	})
	return l.err
}
