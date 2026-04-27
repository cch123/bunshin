package ipc

import (
	"github.com/xargin/bunshin/internal/core"
)

type (
	IPCHandler      = core.IPCHandler
	IPCRing         = core.IPCRing
	IPCRingConfig   = core.IPCRingConfig
	IPCRingSnapshot = core.IPCRingSnapshot
)

var (
	ErrIPCRingClosed          = core.ErrIPCRingClosed
	ErrIPCRingCorrupt         = core.ErrIPCRingCorrupt
	ErrIPCRingEmpty           = core.ErrIPCRingEmpty
	ErrIPCRingFull            = core.ErrIPCRingFull
	ErrIPCRingMessageTooLarge = core.ErrIPCRingMessageTooLarge
)

func OpenIPCRing(cfg IPCRingConfig) (*IPCRing, error) {
	return core.OpenIPCRing(cfg)
}
