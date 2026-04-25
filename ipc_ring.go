package bunshin

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	ipcRingMagic           = "BSIP"
	ipcRingVersion         = 1
	ipcRingHeaderLen       = 64
	minIPCRingCapacity     = 64
	defaultIPCRingCapacity = 64 * 1024
	ipcRecordHeaderLen     = 4
	ipcRecordAlignment     = 8
	ipcRecordWrapMarker    = math.MaxUint32

	ipcRingMagicOffset    = 0
	ipcRingVersionOffset  = 4
	ipcRingHeaderLenOff   = 6
	ipcRingCapacityOffset = 8
	ipcRingReadOffset     = 16
	ipcRingWriteOffset    = 24
)

var (
	ErrIPCRingClosed          = errors.New("bunshin ipc ring: closed")
	ErrIPCRingFull            = errors.New("bunshin ipc ring: full")
	ErrIPCRingEmpty           = errors.New("bunshin ipc ring: empty")
	ErrIPCRingMessageTooLarge = errors.New("bunshin ipc ring: message too large")
	ErrIPCRingCorrupt         = errors.New("bunshin ipc ring: corrupt")
)

type IPCRingConfig struct {
	Path     string
	Capacity int
	Reset    bool
}

type IPCRing struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	data     []byte
	capacity int
	closed   bool
}

type IPCRingSnapshot struct {
	Path          string
	Capacity      int
	Used          int
	Free          int
	ReadPosition  uint64
	WritePosition uint64
}

type IPCHandler func([]byte) error

func OpenIPCRing(cfg IPCRingConfig) (*IPCRing, error) {
	if cfg.Path == "" {
		return nil, invalidConfigf("ipc ring path is required")
	}
	if cfg.Capacity < 0 {
		return nil, invalidConfigf("invalid ipc ring capacity: %d", cfg.Capacity)
	}

	dir := filepath.Dir(cfg.Path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	ring, err := openIPCRingFile(cfg, file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return ring, nil
}

func openIPCRingFile(cfg IPCRingConfig, file *os.File) (*IPCRing, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if cfg.Reset || info.Size() == 0 {
		capacity := cfg.Capacity
		if capacity == 0 {
			capacity = defaultIPCRingCapacity
		}
		if err := validateIPCRingCapacity(capacity); err != nil {
			return nil, err
		}
		length := ipcRingHeaderLen + capacity
		if err := file.Truncate(int64(length)); err != nil {
			return nil, err
		}
		data, err := mmapFile(file, length)
		if err != nil {
			return nil, err
		}
		initIPCRingHeader(data, capacity)
		return &IPCRing{
			path:     cfg.Path,
			file:     file,
			data:     data,
			capacity: capacity,
		}, nil
	}

	if info.Size() < ipcRingHeaderLen {
		return nil, fmt.Errorf("%w: file is shorter than the ipc ring header", ErrIPCRingCorrupt)
	}
	if info.Size() > int64(maxInt()) {
		return nil, fmt.Errorf("%w: ipc ring file is too large", ErrIPCRingCorrupt)
	}
	data, err := mmapFile(file, int(info.Size()))
	if err != nil {
		return nil, err
	}
	capacity, err := validateIPCRingHeader(data)
	if err != nil {
		return nil, errors.Join(err, munmapFile(data))
	}
	if cfg.Capacity != 0 && cfg.Capacity != capacity {
		return nil, errors.Join(
			invalidConfigf("ipc ring capacity mismatch: got %d, want %d", capacity, cfg.Capacity),
			munmapFile(data),
		)
	}
	return &IPCRing{
		path:     cfg.Path,
		file:     file,
		data:     data,
		capacity: capacity,
	}, nil
}

func (r *IPCRing) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	data := r.data
	file := r.file
	r.data = nil
	r.file = nil
	r.closed = true
	r.mu.Unlock()

	var err error
	if data != nil {
		err = errors.Join(err, munmapFile(data))
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	return err
}

func (r *IPCRing) Offer(payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.ensureOpenLocked(); err != nil {
		return err
	}
	return r.offerLocked(payload)
}

func (r *IPCRing) OfferContext(ctx context.Context, payload []byte, idle IdleStrategy) error {
	if ctx == nil {
		ctx = context.Background()
	}
	idle = ipcIdleStrategy(idle)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrIPCRingFull, ctx.Err())
		default:
		}

		err := r.Offer(payload)
		if err == nil {
			idle.Reset()
			return nil
		}
		if !errors.Is(err, ErrIPCRingFull) {
			return err
		}
		idle.Idle(0)
	}
}

func (r *IPCRing) Poll(handler IPCHandler) (int, error) {
	return r.PollN(1, handler)
}

func (r *IPCRing) PollN(limit int, handler IPCHandler) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	if handler == nil {
		return 0, invalidConfigf("ipc ring handler is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.ensureOpenLocked(); err != nil {
		return 0, err
	}

	handled := 0
	for handled < limit {
		polled, err := r.pollOneLocked(handler)
		if err != nil {
			if handled > 0 && errors.Is(err, ErrIPCRingEmpty) {
				return handled, nil
			}
			return handled, err
		}
		if !polled {
			if handled == 0 {
				return 0, ErrIPCRingEmpty
			}
			return handled, nil
		}
		handled++
	}
	return handled, nil
}

func (r *IPCRing) PollContext(ctx context.Context, handler IPCHandler, idle IdleStrategy) (int, error) {
	return r.PollNContext(ctx, 1, handler, idle)
}

func (r *IPCRing) PollNContext(ctx context.Context, limit int, handler IPCHandler, idle IdleStrategy) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idle = ipcIdleStrategy(idle)

	handled := 0
	for handled < limit {
		select {
		case <-ctx.Done():
			if handled > 0 {
				return handled, nil
			}
			return 0, fmt.Errorf("%w: %w", ErrIPCRingEmpty, ctx.Err())
		default:
		}

		n, err := r.PollN(limit-handled, handler)
		handled += n
		if err == nil {
			idle.Reset()
			return handled, nil
		}
		if !errors.Is(err, ErrIPCRingEmpty) {
			return handled, err
		}
		if handled > 0 {
			idle.Reset()
			return handled, nil
		}
		idle.Idle(0)
	}
	return handled, nil
}

func (r *IPCRing) Snapshot() (IPCRingSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.ensureOpenLocked(); err != nil {
		return IPCRingSnapshot{}, err
	}
	read := r.readPositionLocked()
	write := r.writePositionLocked()
	used, err := r.usedLocked(read, write)
	if err != nil {
		return IPCRingSnapshot{}, err
	}
	free := r.capacity - used
	return IPCRingSnapshot{
		Path:          r.path,
		Capacity:      r.capacity,
		Used:          used,
		Free:          free,
		ReadPosition:  read,
		WritePosition: write,
	}, nil
}

func (r *IPCRing) offerLocked(payload []byte) error {
	recordSize := align(ipcRecordHeaderLen+len(payload), ipcRecordAlignment)
	if recordSize > r.capacity {
		return fmt.Errorf("%w: %d bytes", ErrIPCRingMessageTooLarge, len(payload))
	}

	read := r.readPositionLocked()
	write := r.writePositionLocked()
	used, err := r.usedLocked(read, write)
	if err != nil {
		return err
	}

	writeOffset := int(write % uint64(r.capacity))
	tail := r.capacity - writeOffset
	required := recordSize
	if recordSize > tail {
		required += tail
	}
	if r.capacity-used < required {
		return ErrIPCRingFull
	}

	if recordSize > tail {
		r.writeWrapMarkerLocked(writeOffset, tail)
		write += uint64(tail)
		writeOffset = 0
	}

	record := r.recordDataLocked(writeOffset, recordSize)
	binary.LittleEndian.PutUint32(record[:ipcRecordHeaderLen], uint32(len(payload)))
	copy(record[ipcRecordHeaderLen:], payload)
	clear(record[ipcRecordHeaderLen+len(payload):])
	r.setWritePositionLocked(write + uint64(recordSize))
	return nil
}

func (r *IPCRing) pollOneLocked(handler IPCHandler) (bool, error) {
	for {
		read := r.readPositionLocked()
		write := r.writePositionLocked()
		if read == write {
			return false, ErrIPCRingEmpty
		}
		used, err := r.usedLocked(read, write)
		if err != nil {
			return false, err
		}

		readOffset := int(read % uint64(r.capacity))
		tail := r.capacity - readOffset
		if tail < ipcRecordHeaderLen {
			r.setReadPositionLocked(read + uint64(tail))
			continue
		}

		recordHeader := r.recordDataLocked(readOffset, ipcRecordHeaderLen)
		payloadLength := binary.LittleEndian.Uint32(recordHeader)
		if payloadLength == ipcRecordWrapMarker {
			r.setReadPositionLocked(read + uint64(tail))
			continue
		}
		if payloadLength > uint32(r.capacity-ipcRecordHeaderLen) {
			return false, fmt.Errorf("%w: invalid payload length %d", ErrIPCRingCorrupt, payloadLength)
		}

		recordSize := align(ipcRecordHeaderLen+int(payloadLength), ipcRecordAlignment)
		if recordSize > tail {
			return false, fmt.Errorf("%w: record crosses ring boundary", ErrIPCRingCorrupt)
		}
		if uint64(recordSize) > uint64(used) {
			return false, fmt.Errorf("%w: partial record", ErrIPCRingCorrupt)
		}

		record := r.recordDataLocked(readOffset, recordSize)
		payload := append([]byte(nil), record[ipcRecordHeaderLen:ipcRecordHeaderLen+int(payloadLength)]...)
		if err := handler(payload); err != nil {
			return false, err
		}
		r.setReadPositionLocked(read + uint64(recordSize))
		return true, nil
	}
}

func (r *IPCRing) writeWrapMarkerLocked(offset, tail int) {
	if tail <= 0 {
		return
	}
	segment := r.recordDataLocked(offset, tail)
	clear(segment)
	if tail >= ipcRecordHeaderLen {
		binary.LittleEndian.PutUint32(segment[:ipcRecordHeaderLen], ipcRecordWrapMarker)
	}
}

func (r *IPCRing) recordDataLocked(offset, length int) []byte {
	start := ipcRingHeaderLen + offset
	return r.data[start : start+length]
}

func (r *IPCRing) ensureOpenLocked() error {
	if r.closed {
		return ErrIPCRingClosed
	}
	return nil
}

func (r *IPCRing) readPositionLocked() uint64 {
	return binary.LittleEndian.Uint64(r.data[ipcRingReadOffset:])
}

func (r *IPCRing) writePositionLocked() uint64 {
	return binary.LittleEndian.Uint64(r.data[ipcRingWriteOffset:])
}

func (r *IPCRing) setReadPositionLocked(position uint64) {
	binary.LittleEndian.PutUint64(r.data[ipcRingReadOffset:], position)
}

func (r *IPCRing) setWritePositionLocked(position uint64) {
	binary.LittleEndian.PutUint64(r.data[ipcRingWriteOffset:], position)
}

func (r *IPCRing) usedLocked(read, write uint64) (int, error) {
	if write < read {
		return 0, fmt.Errorf("%w: write position is behind read position", ErrIPCRingCorrupt)
	}
	used := write - read
	if used > uint64(r.capacity) {
		return 0, fmt.Errorf("%w: used bytes exceed capacity", ErrIPCRingCorrupt)
	}
	return int(used), nil
}

func initIPCRingHeader(data []byte, capacity int) {
	clear(data)
	copy(data[ipcRingMagicOffset:], ipcRingMagic)
	data[ipcRingVersionOffset] = ipcRingVersion
	binary.LittleEndian.PutUint16(data[ipcRingHeaderLenOff:], ipcRingHeaderLen)
	binary.LittleEndian.PutUint64(data[ipcRingCapacityOffset:], uint64(capacity))
}

func validateIPCRingHeader(data []byte) (int, error) {
	if len(data) < ipcRingHeaderLen {
		return 0, fmt.Errorf("%w: file is shorter than the ipc ring header", ErrIPCRingCorrupt)
	}
	if string(data[ipcRingMagicOffset:ipcRingMagicOffset+len(ipcRingMagic)]) != ipcRingMagic {
		return 0, fmt.Errorf("%w: invalid magic", ErrIPCRingCorrupt)
	}
	if data[ipcRingVersionOffset] != ipcRingVersion {
		return 0, fmt.Errorf("%w: unsupported version %d", ErrIPCRingCorrupt, data[ipcRingVersionOffset])
	}
	headerLen := binary.LittleEndian.Uint16(data[ipcRingHeaderLenOff:])
	if headerLen != ipcRingHeaderLen {
		return 0, fmt.Errorf("%w: invalid header length %d", ErrIPCRingCorrupt, headerLen)
	}
	capacity := binary.LittleEndian.Uint64(data[ipcRingCapacityOffset:])
	if capacity > uint64(maxInt()) {
		return 0, fmt.Errorf("%w: capacity is too large", ErrIPCRingCorrupt)
	}
	if int(capacity) < minIPCRingCapacity || int(capacity) > maxInt()-ipcRingHeaderLen {
		return 0, fmt.Errorf("%w: invalid capacity %d", ErrIPCRingCorrupt, capacity)
	}
	if len(data) != ipcRingHeaderLen+int(capacity) {
		return 0, fmt.Errorf("%w: file length does not match capacity", ErrIPCRingCorrupt)
	}
	return int(capacity), nil
}

func validateIPCRingCapacity(capacity int) error {
	if capacity < minIPCRingCapacity {
		return invalidConfigf("invalid ipc ring capacity: %d", capacity)
	}
	if capacity > maxInt()-ipcRingHeaderLen {
		return invalidConfigf("invalid ipc ring capacity: %d", capacity)
	}
	return nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func ipcIdleStrategy(idle IdleStrategy) IdleStrategy {
	if idle != nil {
		return idle
	}
	return NewDefaultBackoffIdleStrategy()
}
