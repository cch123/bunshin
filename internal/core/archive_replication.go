package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var ErrArchiveReplicationActiveRecording = errors.New("bunshin archive replication: recording is active")

var archiveCopyBufferPool = sync.Pool{
	New: func() any {
		buffer := make([]byte, 32*1024)
		return &buffer
	},
}

type ArchiveReplicationConfig struct {
	RecordingID int64
}

type ArchiveLiveReplicationConfig struct {
	RecordingID int64
	EventBuffer int
}

type ArchiveReplicationReport struct {
	SourceRecordingID      int64  `json:"source_recording_id"`
	DestinationRecordingID int64  `json:"destination_recording_id"`
	Segments               uint64 `json:"segments"`
	Bytes                  int64  `json:"bytes"`
	RecordingBytes         int64  `json:"recording_bytes"`
	StopPosition           int64  `json:"stop_position"`
	Live                   bool   `json:"live"`
	Complete               bool   `json:"complete"`
}

type archiveReplicationSegment struct {
	base int64
	path string
	size int64
}

func ReplicateArchive(ctx context.Context, src, dst *Archive, cfg ArchiveReplicationConfig) (ArchiveReplicationReport, error) {
	if err := validateArchiveReplicationArchives(src, dst); err != nil {
		return ArchiveReplicationReport{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	desc, segments, err := src.replicationSnapshot(cfg.RecordingID)
	if err != nil {
		return ArchiveReplicationReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArchiveReplicationReport{}, err
	}
	return dst.importReplicatedRecording(ctx, desc, segments)
}

func ReplicateArchiveLive(ctx context.Context, src, dst *Archive, cfg ArchiveLiveReplicationConfig) (ArchiveReplicationReport, error) {
	if err := validateArchiveReplicationArchives(src, dst); err != nil {
		return ArchiveReplicationReport{}, err
	}
	if cfg.EventBuffer < 0 {
		return ArchiveReplicationReport{}, invalidConfigf("invalid archive live replication event buffer: %d", cfg.EventBuffer)
	}
	if cfg.EventBuffer == 0 {
		cfg.EventBuffer = defaultArchiveControlProtocolEvents
	}
	if ctx == nil {
		ctx = context.Background()
	}

	events, unsubscribe, err := src.SubscribeRecordingEvents(cfg.EventBuffer)
	if err != nil {
		return ArchiveReplicationReport{}, err
	}
	defer unsubscribe()

	desc, segments, err := src.liveReplicationSnapshot(cfg.RecordingID)
	if err != nil {
		return ArchiveReplicationReport{}, err
	}
	report, err := dst.importReplicatedRecordingWithMode(ctx, desc, segments, desc.StoppedAt == nil)
	if err != nil {
		return report, err
	}
	report.Live = desc.StoppedAt == nil
	report.Complete = desc.StoppedAt != nil
	report.StopPosition = desc.StopPosition
	if desc.StoppedAt != nil {
		return report, nil
	}

	currentPosition := desc.StopPosition
	for {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case event, ok := <-events:
			if !ok {
				return report, ErrArchiveClosed
			}
			if event.RecordingID != desc.RecordingID {
				continue
			}
			switch event.Signal {
			case ArchiveRecordingSignalProgress, ArchiveRecordingSignalExtend, ArchiveRecordingSignalStop:
				if event.NextPosition > currentPosition {
					incremental, err := dst.appendReplicatedRange(ctx, src, event.Descriptor, report.DestinationRecordingID, currentPosition, event.NextPosition)
					if err != nil {
						return report, err
					}
					report.Segments += incremental.Segments
					report.Bytes += incremental.Bytes
					currentPosition = event.NextPosition
					report.RecordingBytes = currentPosition - desc.StartPosition
					report.StopPosition = currentPosition
				}
				if event.Signal == ArchiveRecordingSignalStop {
					if err := dst.finishReplicatedRecording(report.DestinationRecordingID, event.Descriptor); err != nil {
						return report, err
					}
					report.Complete = true
					report.Live = false
					return report, nil
				}
			case ArchiveRecordingSignalTruncate, ArchiveRecordingSignalPurge:
				return report, fmt.Errorf("%w: source recording changed during live replication: %s", ErrArchivePosition, event.Signal)
			}
		}
	}
}

func validateArchiveReplicationArchives(src, dst *Archive) error {
	if src == nil {
		return invalidConfigf("source archive is required")
	}
	if dst == nil {
		return invalidConfigf("destination archive is required")
	}
	if src == dst {
		return invalidConfigf("source and destination archive must be different")
	}
	srcPath, _ := filepath.Abs(src.dir)
	dstPath, _ := filepath.Abs(dst.dir)
	if srcPath == dstPath {
		return invalidConfigf("source and destination archive must be different")
	}
	return nil
}

func (a *Archive) ReplicateTo(ctx context.Context, dst *Archive, cfg ArchiveReplicationConfig) (ArchiveReplicationReport, error) {
	return ReplicateArchive(ctx, a, dst, cfg)
}

func (a *Archive) ReplicateLiveTo(ctx context.Context, dst *Archive, cfg ArchiveLiveReplicationConfig) (ArchiveReplicationReport, error) {
	return ReplicateArchiveLive(ctx, a, dst, cfg)
}

func (a *Archive) replicationSnapshot(recordingID int64) (ArchiveRecordingDescriptor, []archiveReplicationSegment, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveRecordingDescriptor{}, nil, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return ArchiveRecordingDescriptor{}, nil, err
	}
	desc := a.catalog.Recordings[index]
	if desc.StoppedAt == nil {
		return ArchiveRecordingDescriptor{}, nil, ErrArchiveReplicationActiveRecording
	}
	return a.replicationSegmentsLocked(desc)
}

func (a *Archive) liveReplicationSnapshot(recordingID int64) (ArchiveRecordingDescriptor, []archiveReplicationSegment, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveRecordingDescriptor{}, nil, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return ArchiveRecordingDescriptor{}, nil, err
	}
	return a.replicationSegmentsLocked(a.catalog.Recordings[index])
}

func (a *Archive) replicationSegmentsLocked(desc ArchiveRecordingDescriptor) (ArchiveRecordingDescriptor, []archiveReplicationSegment, error) {
	var segments []archiveReplicationSegment
	for base := desc.StartPosition; base < desc.StopPosition; base += desc.SegmentLength {
		segment := a.segmentDescriptorLocked(desc, base)
		if segment.State != ArchiveSegmentAttached {
			return ArchiveRecordingDescriptor{}, nil, fmt.Errorf("%w: segment %d is %s", ErrArchiveCorrupt, base, segment.State)
		}
		segments = append(segments, archiveReplicationSegment{
			base: base,
			path: segment.Path,
			size: segment.Size,
		})
	}
	return desc, segments, nil
}

func (a *Archive) importReplicatedRecording(ctx context.Context, srcDesc ArchiveRecordingDescriptor, srcSegments []archiveReplicationSegment) (ArchiveReplicationReport, error) {
	return a.importReplicatedRecordingWithMode(ctx, srcDesc, srcSegments, false)
}

func (a *Archive) importReplicatedRecordingWithMode(ctx context.Context, srcDesc ArchiveRecordingDescriptor, srcSegments []archiveReplicationSegment, activate bool) (ArchiveReplicationReport, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveReplicationReport{}, ErrArchiveClosed
	}
	if activate && a.activeRecordingID != 0 {
		return ArchiveReplicationReport{}, ErrArchiveRecordingActive
	}

	dstRecordingID := a.catalog.NextRecordingID
	if dstRecordingID <= 0 {
		dstRecordingID = 1
	}
	dstDesc := srcDesc
	dstDesc.RecordingID = dstRecordingID

	report := ArchiveReplicationReport{
		SourceRecordingID:      srcDesc.RecordingID,
		DestinationRecordingID: dstRecordingID,
		RecordingBytes:         srcDesc.StopPosition - srcDesc.StartPosition,
		StopPosition:           srcDesc.StopPosition,
		Complete:               srcDesc.StoppedAt != nil,
	}
	copied := make([]string, 0, len(srcSegments))
	for _, segment := range srcSegments {
		if err := ctx.Err(); err != nil {
			removeArchiveFiles(copied)
			return report, err
		}
		dstPath := a.segmentPath(dstRecordingID, segment.base)
		if _, err := os.Stat(dstPath); err == nil {
			removeArchiveFiles(copied)
			return report, fmt.Errorf("%w: destination segment exists: %s", ErrArchivePosition, dstPath)
		} else if !errors.Is(err, os.ErrNotExist) {
			removeArchiveFiles(copied)
			return report, fmt.Errorf("stat archive replication destination segment: %w", err)
		}
		bytes, err := copyArchiveFile(ctx, segment.path, dstPath)
		if err != nil {
			removeArchiveFiles(copied)
			return report, err
		}
		copied = append(copied, dstPath)
		report.Segments++
		report.Bytes += bytes
	}

	oldNextRecordingID := a.catalog.NextRecordingID
	oldRecordingsLen := len(a.catalog.Recordings)
	oldActiveRecordingID := a.activeRecordingID
	oldExtensionSignalPending := a.extensionSignalPending
	a.catalog.NextRecordingID = dstRecordingID + 1
	a.catalog.Recordings = append(a.catalog.Recordings, dstDesc)
	sortArchiveRecordings(a.catalog.Recordings)
	if activate {
		a.activeRecordingID = dstRecordingID
		a.extensionSignalPending = false
	}
	if err := a.saveCatalogLocked(); err != nil {
		a.catalog.NextRecordingID = oldNextRecordingID
		a.catalog.Recordings = a.catalog.Recordings[:oldRecordingsLen]
		a.activeRecordingID = oldActiveRecordingID
		a.extensionSignalPending = oldExtensionSignalPending
		removeArchiveFiles(copied)
		return report, err
	}
	return report, nil
}

func (a *Archive) appendReplicatedRange(ctx context.Context, src *Archive, srcDesc ArchiveRecordingDescriptor, dstRecordingID, fromPosition, toPosition int64) (ArchiveReplicationReport, error) {
	if fromPosition > toPosition {
		return ArchiveReplicationReport{}, fmt.Errorf("%w: invalid replication range %d-%d", ErrArchivePosition, fromPosition, toPosition)
	}
	report := ArchiveReplicationReport{
		SourceRecordingID:      srcDesc.RecordingID,
		DestinationRecordingID: dstRecordingID,
		RecordingBytes:         toPosition - fromPosition,
		StopPosition:           toPosition,
		Live:                   srcDesc.StoppedAt == nil,
		Complete:               srcDesc.StoppedAt != nil,
	}
	if fromPosition == toPosition {
		return report, nil
	}

	type replicatedPacket struct {
		position int64
		next     int64
		packet   []byte
	}
	var packets []replicatedPacket
	segments := make(map[int64]struct{})
	for position := fromPosition; position < toPosition; {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		packet, entry, err := src.readArchivePacket(srcDesc, position)
		if err != nil {
			return report, err
		}
		if entry.record.NextPosition > toPosition {
			return report, fmt.Errorf("%w: replication range ends inside record at %d", ErrArchivePosition, position)
		}
		if len(packet) > 0 {
			packets = append(packets, replicatedPacket{
				position: position,
				next:     entry.record.NextPosition,
				packet:   packet,
			})
			segments[archiveSegmentBase(position, srcDesc.SegmentLength)] = struct{}{}
		}
		position = entry.record.NextPosition
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return report, ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(dstRecordingID)
	if err != nil {
		return report, err
	}
	desc := &a.catalog.Recordings[index]
	if desc.StopPosition != fromPosition {
		return report, fmt.Errorf("%w: destination position %d does not match source position %d", ErrArchivePosition, desc.StopPosition, fromPosition)
	}
	for _, packet := range packets {
		segmentBase := archiveSegmentBase(packet.position, desc.SegmentLength)
		segmentOffset := packet.position - segmentBase
		if err := a.writeSegmentLocked(desc.RecordingID, segmentBase, segmentOffset, packet.packet); err != nil {
			return report, err
		}
		desc.StopPosition = packet.next
		report.Bytes += int64(len(packet.packet))
	}
	if len(packets) == 0 {
		desc.StopPosition = toPosition
	}
	desc.UpdatedAt = srcDesc.UpdatedAt
	if srcDesc.StoppedAt != nil {
		desc.StoppedAt = srcDesc.StoppedAt
		if a.activeRecordingID == desc.RecordingID {
			a.activeRecordingID = 0
			a.extensionSignalPending = false
		}
	}
	if err := a.saveCatalogLocked(); err != nil {
		return report, err
	}
	report.Segments = uint64(len(segments))
	return report, nil
}

func (a *Archive) finishReplicatedRecording(recordingID int64, srcDesc ArchiveRecordingDescriptor) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ErrArchiveClosed
	}
	index, err := a.recordingIndexLocked(recordingID)
	if err != nil {
		return err
	}
	desc := &a.catalog.Recordings[index]
	desc.StopPosition = srcDesc.StopPosition
	desc.UpdatedAt = srcDesc.UpdatedAt
	desc.StoppedAt = srcDesc.StoppedAt
	if a.activeRecordingID == desc.RecordingID {
		a.activeRecordingID = 0
		a.extensionSignalPending = false
	}
	return a.saveCatalogLocked()
}

func (a *Archive) readArchivePacket(desc ArchiveRecordingDescriptor, position int64) ([]byte, archiveEntry, error) {
	entry, err := a.readArchiveEntry(desc, position)
	if err != nil {
		return nil, archiveEntry{}, err
	}
	segmentBase := archiveSegmentBase(position, desc.SegmentLength)
	segmentOffset := position - segmentBase
	tail := desc.SegmentLength - segmentOffset
	if entry.padding && tail < archiveHeaderLen {
		return nil, entry, nil
	}
	packetLen := entry.record.NextPosition - entry.record.Position
	if packetLen < 0 || packetLen > desc.SegmentLength {
		return nil, archiveEntry{}, fmt.Errorf("%w: invalid packet length %d", ErrArchiveCorrupt, packetLen)
	}
	packet := make([]byte, int(packetLen))
	file, err := os.Open(a.segmentPath(desc.RecordingID, segmentBase))
	if err != nil {
		return nil, archiveEntry{}, fmt.Errorf("%w: open segment packet at position %d: %w", ErrArchiveCorrupt, position, err)
	}
	defer file.Close()
	if _, err := file.ReadAt(packet, segmentOffset); err != nil {
		return nil, archiveEntry{}, fmt.Errorf("%w: read segment packet at position %d: %w", ErrArchiveCorrupt, position, err)
	}
	return packet, entry, nil
}

func copyArchiveFile(ctx context.Context, src, dst string) (int64, error) {
	info, err := os.Stat(src)
	if err != nil {
		return 0, fmt.Errorf("stat archive file: %w", err)
	}
	in, err := os.Open(src)
	if err != nil {
		return 0, fmt.Errorf("open archive file: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return 0, fmt.Errorf("create archive file: %w", err)
	}
	written, copyErr := copyArchiveFileWithContext(ctx, out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(dst)
		return written, fmt.Errorf("copy archive file: %w", copyErr)
	}
	if syncErr != nil {
		_ = os.Remove(dst)
		return written, fmt.Errorf("sync archive file: %w", syncErr)
	}
	if closeErr != nil {
		_ = os.Remove(dst)
		return written, fmt.Errorf("close archive file: %w", closeErr)
	}
	return written, nil
}

func copyArchiveFileWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	bufp := archiveCopyBufferPool.Get().(*[]byte)
	defer archiveCopyBufferPool.Put(bufp)

	buf := *bufp
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			written += int64(nw)
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}

func removeArchiveFiles(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}
