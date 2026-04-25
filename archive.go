package bunshin

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	archiveMagic            = "BSAR"
	archiveVersion    uint8 = 1
	archiveHeaderLen        = 64
	archiveMaxPayload       = uint64(^uint32(0))
)

var (
	ErrArchiveClosed   = errors.New("bunshin archive: closed")
	ErrArchiveCorrupt  = errors.New("bunshin archive: corrupt")
	ErrArchivePosition = errors.New("bunshin archive: invalid position")
)

type ArchiveConfig struct {
	Path string
	Sync bool
}

type Archive struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	sync   bool
	closed bool
}

type ArchiveRecord struct {
	Position     int64
	NextPosition int64
	RecordedAt   time.Time
	Message      Message
}

type ArchiveReplayConfig struct {
	FromPosition int64
	StreamID     uint32
	SessionID    uint32
}

type ArchiveIntegrityReport struct {
	Path            string
	Records         uint64
	Bytes           int64
	LastPosition    int64
	CorruptPosition int64
}

func OpenArchive(cfg ArchiveConfig) (*Archive, error) {
	if cfg.Path == "" {
		return nil, invalidConfigf("archive path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, fmt.Errorf("create archive directory: %w", err)
	}
	file, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}
	return &Archive{
		path: cfg.Path,
		file: file,
		sync: cfg.Sync,
	}, nil
}

func (a *Archive) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true
	return a.file.Close()
}

func (a *Archive) Record(msg Message) (ArchiveRecord, error) {
	if uint64(len(msg.Payload)) > archiveMaxPayload {
		return ArchiveRecord{}, fmt.Errorf("%w: payload too large: %d bytes", ErrInvalidConfig, len(msg.Payload))
	}

	recordedAt := time.Now().UTC()
	packet := encodeArchiveRecord(msg, recordedAt)

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveRecord{}, ErrArchiveClosed
	}
	position, err := a.file.Seek(0, io.SeekEnd)
	if err != nil {
		return ArchiveRecord{}, fmt.Errorf("seek archive end: %w", err)
	}
	if _, err := a.file.Write(packet); err != nil {
		return ArchiveRecord{}, fmt.Errorf("write archive record: %w", err)
	}
	if a.sync {
		if err := a.file.Sync(); err != nil {
			return ArchiveRecord{}, fmt.Errorf("sync archive: %w", err)
		}
	}

	return ArchiveRecord{
		Position:     position,
		NextPosition: position + int64(len(packet)),
		RecordedAt:   recordedAt,
		Message:      cloneMessage(msg),
	}, nil
}

func (a *Archive) RecordingHandler(next Handler) Handler {
	return func(ctx context.Context, msg Message) error {
		if _, err := a.Record(msg); err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		return next(ctx, msg)
	}
}

func (a *Archive) Replay(ctx context.Context, cfg ArchiveReplayConfig, handler Handler) error {
	if handler == nil {
		return errors.New("handler is required")
	}
	if cfg.FromPosition < 0 {
		return fmt.Errorf("%w: %d", ErrArchivePosition, cfg.FromPosition)
	}

	file, size, err := a.snapshotReader()
	if err != nil {
		return err
	}
	defer file.Close()

	if cfg.FromPosition > size {
		return fmt.Errorf("%w: %d", ErrArchivePosition, cfg.FromPosition)
	}
	for position := cfg.FromPosition; position < size; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record, err := readArchiveRecordAt(file, position, size)
		if err != nil {
			return err
		}
		position = record.NextPosition
		if !archiveReplayMatch(cfg, record.Message) {
			continue
		}
		if err := handler(ctx, record.Message); err != nil {
			return err
		}
	}
	return nil
}

func (a *Archive) IntegrityScan() (ArchiveIntegrityReport, error) {
	file, size, err := a.snapshotReader()
	if err != nil {
		return ArchiveIntegrityReport{}, err
	}
	defer file.Close()

	report := ArchiveIntegrityReport{
		Path:            a.path,
		Bytes:           size,
		CorruptPosition: -1,
	}
	for position := int64(0); position < size; {
		record, err := readArchiveRecordAt(file, position, size)
		if err != nil {
			report.CorruptPosition = position
			return report, err
		}
		report.Records++
		report.LastPosition = record.Position
		position = record.NextPosition
	}
	return report, nil
}

func (a *Archive) Truncate(position int64) error {
	if position < 0 {
		return fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ErrArchiveClosed
	}
	size, err := a.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek archive end: %w", err)
	}
	if position > size {
		return fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}
	if err := a.validateBoundaryLocked(position, size); err != nil {
		return err
	}
	if err := a.file.Truncate(position); err != nil {
		return fmt.Errorf("truncate archive: %w", err)
	}
	if _, err := a.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek archive end: %w", err)
	}
	if a.sync {
		if err := a.file.Sync(); err != nil {
			return fmt.Errorf("sync archive: %w", err)
		}
	}
	return nil
}

func (a *Archive) Purge() error {
	return a.Truncate(0)
}

func (a *Archive) snapshotReader() (*os.File, int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, 0, ErrArchiveClosed
	}
	if a.sync {
		if err := a.file.Sync(); err != nil {
			return nil, 0, fmt.Errorf("sync archive: %w", err)
		}
	}
	info, err := a.file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat archive: %w", err)
	}
	file, err := os.Open(a.path)
	if err != nil {
		return nil, 0, fmt.Errorf("open archive reader: %w", err)
	}
	return file, info.Size(), nil
}

func (a *Archive) validateBoundaryLocked(position, size int64) error {
	if position == 0 || position == size {
		return nil
	}
	for offset := int64(0); offset < size; {
		record, err := readArchiveRecordAt(a.file, offset, size)
		if err != nil {
			return err
		}
		if record.NextPosition == position {
			return nil
		}
		if record.NextPosition > position {
			break
		}
		offset = record.NextPosition
	}
	return fmt.Errorf("%w: %d", ErrArchivePosition, position)
}

func encodeArchiveRecord(msg Message, recordedAt time.Time) []byte {
	payload := append([]byte(nil), msg.Payload...)
	recordLen := archiveHeaderLen + len(payload)
	buf := make([]byte, recordLen)

	copy(buf[0:4], archiveMagic)
	buf[4] = archiveVersion
	frameByteOrder.PutUint16(buf[6:8], uint16(archiveHeaderLen))
	frameByteOrder.PutUint64(buf[8:16], uint64(recordLen))
	frameByteOrder.PutUint32(buf[16:20], msg.StreamID)
	frameByteOrder.PutUint32(buf[20:24], msg.SessionID)
	frameByteOrder.PutUint32(buf[24:28], uint32(msg.TermID))
	frameByteOrder.PutUint32(buf[28:32], uint32(msg.TermOffset))
	frameByteOrder.PutUint64(buf[32:40], msg.Sequence)
	frameByteOrder.PutUint64(buf[40:48], msg.ReservedValue)
	frameByteOrder.PutUint64(buf[48:56], uint64(recordedAt.UnixNano()))
	frameByteOrder.PutUint32(buf[56:60], uint32(len(payload)))
	copy(buf[archiveHeaderLen:], payload)
	frameByteOrder.PutUint32(buf[60:64], archiveRecordCRC(buf))
	return buf
}

func readArchiveRecordAt(reader io.ReaderAt, position, limit int64) (ArchiveRecord, error) {
	if position < 0 || position > limit {
		return ArchiveRecord{}, fmt.Errorf("%w: %d", ErrArchivePosition, position)
	}
	if limit-position < archiveHeaderLen {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: incomplete header", ErrArchiveCorrupt, position)
	}

	header := make([]byte, archiveHeaderLen)
	if _, err := reader.ReadAt(header, position); err != nil {
		return ArchiveRecord{}, fmt.Errorf("read archive header: %w", err)
	}
	if string(header[0:4]) != archiveMagic {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: invalid magic", ErrArchiveCorrupt, position)
	}
	if header[4] != archiveVersion {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: unsupported version %d", ErrArchiveCorrupt, position, header[4])
	}
	if headerLen := int(frameByteOrder.Uint16(header[6:8])); headerLen != archiveHeaderLen {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: invalid header length %d", ErrArchiveCorrupt, position, headerLen)
	}
	recordLen := int64(frameByteOrder.Uint64(header[8:16]))
	payloadLen := int64(frameByteOrder.Uint32(header[56:60]))
	if recordLen != archiveHeaderLen+payloadLen || recordLen < archiveHeaderLen {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: invalid record length %d", ErrArchiveCorrupt, position, recordLen)
	}
	if position+recordLen > limit {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: incomplete record", ErrArchiveCorrupt, position)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := reader.ReadAt(payload, position+archiveHeaderLen); err != nil {
			return ArchiveRecord{}, fmt.Errorf("read archive payload: %w", err)
		}
	}

	packet := make([]byte, archiveHeaderLen+len(payload))
	copy(packet, header)
	copy(packet[archiveHeaderLen:], payload)
	wantCRC := frameByteOrder.Uint32(header[60:64])
	if gotCRC := archiveRecordCRC(packet); gotCRC != wantCRC {
		return ArchiveRecord{}, fmt.Errorf("%w at position %d: checksum mismatch", ErrArchiveCorrupt, position)
	}

	return ArchiveRecord{
		Position:     position,
		NextPosition: position + recordLen,
		RecordedAt:   time.Unix(0, int64(frameByteOrder.Uint64(header[48:56]))).UTC(),
		Message: Message{
			StreamID:      frameByteOrder.Uint32(header[16:20]),
			SessionID:     frameByteOrder.Uint32(header[20:24]),
			TermID:        int32(frameByteOrder.Uint32(header[24:28])),
			TermOffset:    int32(frameByteOrder.Uint32(header[28:32])),
			Sequence:      frameByteOrder.Uint64(header[32:40]),
			ReservedValue: frameByteOrder.Uint64(header[40:48]),
			Payload:       payload,
		},
	}, nil
}

func archiveRecordCRC(packet []byte) uint32 {
	crc := crc32.NewIEEE()
	_, _ = crc.Write(packet[:60])
	if len(packet) > archiveHeaderLen {
		_, _ = crc.Write(packet[archiveHeaderLen:])
	}
	return crc.Sum32()
}

func archiveReplayMatch(cfg ArchiveReplayConfig, msg Message) bool {
	if cfg.StreamID != 0 && cfg.StreamID != msg.StreamID {
		return false
	}
	if cfg.SessionID != 0 && cfg.SessionID != msg.SessionID {
		return false
	}
	return true
}

func cloneMessage(msg Message) Message {
	msg.Payload = append([]byte(nil), msg.Payload...)
	return msg
}
