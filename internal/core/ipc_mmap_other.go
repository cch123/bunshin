//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package core

import (
	"errors"
	"os"
)

func mmapFile(_ *os.File, _ int) ([]byte, error) {
	return nil, errors.New("bunshin ipc ring: mmap is not supported on this platform")
}

func munmapFile(_ []byte) error {
	return nil
}
