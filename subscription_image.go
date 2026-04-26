package bunshin

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	driverSubscriptionImageMagic           = "BSIM"
	driverSubscriptionImageVersion         = 1
	driverSubscriptionImageHeaderLen       = 64
	driverSubscriptionImageRecordHeaderLen = 4
	driverSubscriptionImageRecordAlignment = 8
	driverSubscriptionImageWrapMarker      = math.MaxUint32
	driverSubscriptionImageMessageHeader   = 48

	driverSubscriptionImageMagicOffset    = 0
	driverSubscriptionImageVersionOffset  = 4
	driverSubscriptionImageHeaderLenOff   = 6
	driverSubscriptionImageCapacityOffset = 8
	driverSubscriptionImageReadOffset     = 16
	driverSubscriptionImageWriteOffset    = 24
)

var minDriverSubscriptionImageRecordBytes = driverSubscriptionImageRecordBytes(Message{StreamID: defaultStreamID})

type DriverSubscriptionImageConfig struct {
	Path     string
	Capacity int
	Reset    bool
}

type DriverSubscriptionImage struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	data     []byte
	capacity int
	closed   bool
}

type DriverSubscriptionImageSnapshot struct {
	Path          string
	Capacity      int
	Used          int
	Free          int
	ReadPosition  uint64
	WritePosition uint64
}

func OpenDriverSubscriptionImage(cfg DriverSubscriptionImageConfig) (*DriverSubscriptionImage, error) {
	if cfg.Path == "" {
		return nil, invalidConfigf("driver subscription image path is required")
	}
	if cfg.Capacity < 0 {
		return nil, invalidConfigf("invalid driver subscription image capacity: %d", cfg.Capacity)
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
	image, err := openDriverSubscriptionImageFile(cfg, file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return image, nil
}

func openDriverSubscriptionImageFile(cfg DriverSubscriptionImageConfig, file *os.File) (*DriverSubscriptionImage, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if cfg.Reset || info.Size() == 0 {
		capacity := cfg.Capacity
		if capacity == 0 {
			capacity = defaultDriverIPCSubscriptionDataRing
		}
		if err := validateIPCRingCapacity(capacity); err != nil {
			return nil, err
		}
		length := driverSubscriptionImageHeaderLen + capacity
		if err := file.Truncate(int64(length)); err != nil {
			return nil, err
		}
		data, err := mmapFile(file, length)
		if err != nil {
			return nil, err
		}
		initDriverSubscriptionImageHeader(data, capacity)
		return &DriverSubscriptionImage{
			path:     cfg.Path,
			file:     file,
			data:     data,
			capacity: capacity,
		}, nil
	}

	if info.Size() < driverSubscriptionImageHeaderLen {
		return nil, fmt.Errorf("%w: file is shorter than the driver subscription image header", ErrIPCRingCorrupt)
	}
	if info.Size() > int64(maxInt()) {
		return nil, fmt.Errorf("%w: driver subscription image file is too large", ErrIPCRingCorrupt)
	}
	data, err := mmapFile(file, int(info.Size()))
	if err != nil {
		return nil, err
	}
	capacity, err := validateDriverSubscriptionImageHeader(data)
	if err != nil {
		return nil, errors.Join(err, munmapFile(data))
	}
	if cfg.Capacity != 0 && cfg.Capacity != capacity {
		return nil, errors.Join(
			invalidConfigf("driver subscription image capacity mismatch: got %d, want %d", capacity, cfg.Capacity),
			munmapFile(data),
		)
	}
	return &DriverSubscriptionImage{
		path:     cfg.Path,
		file:     file,
		data:     data,
		capacity: capacity,
	}, nil
}

func (i *DriverSubscriptionImage) Close() error {
	if i == nil {
		return nil
	}
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return nil
	}
	data := i.data
	file := i.file
	i.data = nil
	i.file = nil
	i.closed = true
	i.mu.Unlock()

	var err error
	if data != nil {
		err = errors.Join(err, munmapFile(data))
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	return err
}

func (i *DriverSubscriptionImage) OfferMessage(msg Message) error {
	if i == nil {
		return ErrIPCRingClosed
	}
	payload, err := encodeDriverSubscriptionImageMessage(msg)
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if err := i.ensureOpenLocked(); err != nil {
		return err
	}
	return i.offerLocked(payload)
}

func (i *DriverSubscriptionImage) PollN(limit int, handler func(Message) error) (int, error) {
	if i == nil {
		return 0, ErrIPCRingClosed
	}
	if limit <= 0 {
		return 0, nil
	}
	if handler == nil {
		return 0, invalidConfigf("driver subscription image handler is required")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if err := i.ensureOpenLocked(); err != nil {
		return 0, err
	}

	handled := 0
	for handled < limit {
		polled, err := i.pollOneLocked(handler)
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

func (i *DriverSubscriptionImage) Snapshot() (DriverSubscriptionImageSnapshot, error) {
	if i == nil {
		return DriverSubscriptionImageSnapshot{}, ErrIPCRingClosed
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	if err := i.ensureOpenLocked(); err != nil {
		return DriverSubscriptionImageSnapshot{}, err
	}
	read := i.readPositionLocked()
	write := i.writePositionLocked()
	used, err := i.usedLocked(read, write)
	if err != nil {
		return DriverSubscriptionImageSnapshot{}, err
	}
	return DriverSubscriptionImageSnapshot{
		Path:          i.path,
		Capacity:      i.capacity,
		Used:          used,
		Free:          i.capacity - used,
		ReadPosition:  read,
		WritePosition: write,
	}, nil
}

func (i *DriverSubscriptionImage) offerLocked(payload []byte) error {
	recordSize := align(driverSubscriptionImageRecordHeaderLen+len(payload), driverSubscriptionImageRecordAlignment)
	if recordSize > i.capacity {
		return fmt.Errorf("%w: %d bytes", ErrIPCRingMessageTooLarge, len(payload))
	}

	read := i.readPositionLocked()
	write := i.writePositionLocked()
	used, err := i.usedLocked(read, write)
	if err != nil {
		return err
	}

	writeOffset := int(write % uint64(i.capacity))
	tail := i.capacity - writeOffset
	required := recordSize
	if recordSize > tail {
		required += tail
	}
	if i.capacity-used < required {
		return ErrIPCRingFull
	}

	if recordSize > tail {
		i.writeWrapMarkerLocked(writeOffset, tail)
		write += uint64(tail)
		writeOffset = 0
	}

	record := i.recordDataLocked(writeOffset, recordSize)
	binary.LittleEndian.PutUint32(record[:driverSubscriptionImageRecordHeaderLen], uint32(len(payload)))
	copy(record[driverSubscriptionImageRecordHeaderLen:], payload)
	clear(record[driverSubscriptionImageRecordHeaderLen+len(payload):])
	i.setWritePositionLocked(write + uint64(recordSize))
	return nil
}

func (i *DriverSubscriptionImage) pollOneLocked(handler func(Message) error) (bool, error) {
	for {
		read := i.readPositionLocked()
		write := i.writePositionLocked()
		if read == write {
			return false, ErrIPCRingEmpty
		}
		used, err := i.usedLocked(read, write)
		if err != nil {
			return false, err
		}

		readOffset := int(read % uint64(i.capacity))
		tail := i.capacity - readOffset
		if tail < driverSubscriptionImageRecordHeaderLen {
			i.setReadPositionLocked(read + uint64(tail))
			continue
		}

		recordHeader := i.recordDataLocked(readOffset, driverSubscriptionImageRecordHeaderLen)
		payloadLength := binary.LittleEndian.Uint32(recordHeader)
		if payloadLength == driverSubscriptionImageWrapMarker {
			i.setReadPositionLocked(read + uint64(tail))
			continue
		}
		if payloadLength > uint32(i.capacity-driverSubscriptionImageRecordHeaderLen) {
			return false, fmt.Errorf("%w: invalid driver subscription image payload length %d", ErrIPCRingCorrupt, payloadLength)
		}

		recordSize := align(driverSubscriptionImageRecordHeaderLen+int(payloadLength), driverSubscriptionImageRecordAlignment)
		if recordSize > tail {
			return false, fmt.Errorf("%w: driver subscription image record crosses boundary", ErrIPCRingCorrupt)
		}
		if uint64(recordSize) > uint64(used) {
			return false, fmt.Errorf("%w: partial driver subscription image record", ErrIPCRingCorrupt)
		}

		record := i.recordDataLocked(readOffset, recordSize)
		msg, err := decodeDriverSubscriptionImageMessage(record[driverSubscriptionImageRecordHeaderLen : driverSubscriptionImageRecordHeaderLen+int(payloadLength)])
		if err != nil {
			return false, err
		}
		if err := handler(msg); err != nil {
			return false, err
		}
		i.setReadPositionLocked(read + uint64(recordSize))
		return true, nil
	}
}

func (i *DriverSubscriptionImage) writeWrapMarkerLocked(offset, tail int) {
	if tail <= 0 {
		return
	}
	segment := i.recordDataLocked(offset, tail)
	clear(segment)
	if tail >= driverSubscriptionImageRecordHeaderLen {
		binary.LittleEndian.PutUint32(segment[:driverSubscriptionImageRecordHeaderLen], driverSubscriptionImageWrapMarker)
	}
}

func (i *DriverSubscriptionImage) recordDataLocked(offset, length int) []byte {
	start := driverSubscriptionImageHeaderLen + offset
	return i.data[start : start+length]
}

func (i *DriverSubscriptionImage) ensureOpenLocked() error {
	if i == nil || i.closed {
		return ErrIPCRingClosed
	}
	return nil
}

func (i *DriverSubscriptionImage) readPositionLocked() uint64 {
	return binary.LittleEndian.Uint64(i.data[driverSubscriptionImageReadOffset:])
}

func (i *DriverSubscriptionImage) writePositionLocked() uint64 {
	return binary.LittleEndian.Uint64(i.data[driverSubscriptionImageWriteOffset:])
}

func (i *DriverSubscriptionImage) setReadPositionLocked(position uint64) {
	binary.LittleEndian.PutUint64(i.data[driverSubscriptionImageReadOffset:], position)
}

func (i *DriverSubscriptionImage) setWritePositionLocked(position uint64) {
	binary.LittleEndian.PutUint64(i.data[driverSubscriptionImageWriteOffset:], position)
}

func (i *DriverSubscriptionImage) usedLocked(read, write uint64) (int, error) {
	if write < read {
		return 0, fmt.Errorf("%w: driver subscription image write position is behind read position", ErrIPCRingCorrupt)
	}
	used := write - read
	if used > uint64(i.capacity) {
		return 0, fmt.Errorf("%w: driver subscription image used bytes exceed capacity", ErrIPCRingCorrupt)
	}
	return int(used), nil
}

func initDriverSubscriptionImageHeader(data []byte, capacity int) {
	clear(data)
	copy(data[driverSubscriptionImageMagicOffset:], driverSubscriptionImageMagic)
	data[driverSubscriptionImageVersionOffset] = driverSubscriptionImageVersion
	binary.LittleEndian.PutUint16(data[driverSubscriptionImageHeaderLenOff:], driverSubscriptionImageHeaderLen)
	binary.LittleEndian.PutUint64(data[driverSubscriptionImageCapacityOffset:], uint64(capacity))
}

func validateDriverSubscriptionImageHeader(data []byte) (int, error) {
	if len(data) < driverSubscriptionImageHeaderLen {
		return 0, fmt.Errorf("%w: file is shorter than the driver subscription image header", ErrIPCRingCorrupt)
	}
	if string(data[driverSubscriptionImageMagicOffset:driverSubscriptionImageMagicOffset+len(driverSubscriptionImageMagic)]) != driverSubscriptionImageMagic {
		return 0, fmt.Errorf("%w: invalid driver subscription image magic", ErrIPCRingCorrupt)
	}
	if data[driverSubscriptionImageVersionOffset] != driverSubscriptionImageVersion {
		return 0, fmt.Errorf("%w: unsupported driver subscription image version %d", ErrIPCRingCorrupt, data[driverSubscriptionImageVersionOffset])
	}
	headerLen := int(binary.LittleEndian.Uint16(data[driverSubscriptionImageHeaderLenOff:]))
	if headerLen != driverSubscriptionImageHeaderLen {
		return 0, fmt.Errorf("%w: invalid driver subscription image header length %d", ErrIPCRingCorrupt, headerLen)
	}
	capacity64 := binary.LittleEndian.Uint64(data[driverSubscriptionImageCapacityOffset:])
	if capacity64 > uint64(maxInt()) {
		return 0, fmt.Errorf("%w: driver subscription image capacity too large %d", ErrIPCRingCorrupt, capacity64)
	}
	capacity := int(capacity64)
	if err := validateIPCRingCapacity(capacity); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrIPCRingCorrupt, err)
	}
	if len(data) != driverSubscriptionImageHeaderLen+capacity {
		return 0, fmt.Errorf("%w: driver subscription image size mismatch", ErrIPCRingCorrupt)
	}
	return capacity, nil
}

func encodeDriverSubscriptionImageMessage(msg Message) ([]byte, error) {
	remote := []byte(nil)
	if msg.Remote != nil {
		remote = []byte(msg.Remote.String())
	}
	if len(remote) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: remote address too large: %d bytes", ErrDriverIPCProtocol, len(remote))
	}
	if len(msg.Payload) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: payload too large: %d bytes", ErrDriverIPCProtocol, len(msg.Payload))
	}
	buf := make([]byte, driverSubscriptionImageMessageHeader+len(remote)+len(msg.Payload))
	binary.LittleEndian.PutUint32(buf[0:4], msg.StreamID)
	binary.LittleEndian.PutUint32(buf[4:8], msg.SessionID)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(msg.TermID))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(msg.TermOffset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(msg.Position))
	binary.LittleEndian.PutUint64(buf[24:32], msg.Sequence)
	binary.LittleEndian.PutUint64(buf[32:40], msg.ReservedValue)
	binary.LittleEndian.PutUint32(buf[40:44], uint32(len(remote)))
	binary.LittleEndian.PutUint32(buf[44:48], uint32(len(msg.Payload)))
	copy(buf[driverSubscriptionImageMessageHeader:], remote)
	copy(buf[driverSubscriptionImageMessageHeader+len(remote):], msg.Payload)
	return buf, nil
}

func decodeDriverSubscriptionImageMessage(buf []byte) (Message, error) {
	if len(buf) < driverSubscriptionImageMessageHeader {
		return Message{}, fmt.Errorf("%w: driver subscription image message too short", ErrDriverIPCProtocol)
	}
	remoteLen := int(binary.LittleEndian.Uint32(buf[40:44]))
	payloadLen := int(binary.LittleEndian.Uint32(buf[44:48]))
	if remoteLen < 0 || payloadLen < 0 || len(buf) != driverSubscriptionImageMessageHeader+remoteLen+payloadLen {
		return Message{}, fmt.Errorf("%w: invalid driver subscription image message length", ErrDriverIPCProtocol)
	}
	msg := Message{
		StreamID:      binary.LittleEndian.Uint32(buf[0:4]),
		SessionID:     binary.LittleEndian.Uint32(buf[4:8]),
		TermID:        int32(binary.LittleEndian.Uint32(buf[8:12])),
		TermOffset:    int32(binary.LittleEndian.Uint32(buf[12:16])),
		Position:      int64(binary.LittleEndian.Uint64(buf[16:24])),
		Sequence:      binary.LittleEndian.Uint64(buf[24:32]),
		ReservedValue: binary.LittleEndian.Uint64(buf[32:40]),
	}
	payloadOffset := driverSubscriptionImageMessageHeader + remoteLen
	if remoteLen > 0 {
		msg.Remote = driverNetAddr(string(buf[driverSubscriptionImageMessageHeader:payloadOffset]))
	}
	msg.Payload = cloneBytes(buf[payloadOffset : payloadOffset+payloadLen])
	return msg, nil
}

func driverSubscriptionImageRecordBytes(msg Message) int {
	payload, err := encodeDriverSubscriptionImageMessage(msg)
	if err != nil {
		return minIPCRingCapacity
	}
	return align(driverSubscriptionImageRecordHeaderLen+len(payload), driverSubscriptionImageRecordAlignment)
}
