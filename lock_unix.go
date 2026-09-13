//go:build linux || darwin || dragonfly || freebsd || illumos || netbsd || openbsd

package ingot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const dataDirLockFilename = "lock"

type dataDirLock struct {
	file *os.File
}

func acquireDataDirLock(dataDir string) (*dataDirLock, error) {
	path := filepath.Join(dataDir, dataDirLockFilename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("ingot: open data directory lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(fmt.Errorf("%w: %s", ErrDataDirLocked, dataDir), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("ingot: lock data directory: %w", err), closeErr)
	}
	return &dataDirLock{file: f}, nil
}

func (l *dataDirLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	return errors.Join(unlockErr, closeErr)
}
