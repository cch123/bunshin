//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package bunshin

import (
	"os"
	"syscall"
)

func mmapFile(file *os.File, length int) ([]byte, error) {
	return syscall.Mmap(int(file.Fd()), 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func munmapFile(data []byte) error {
	return syscall.Munmap(data)
}
