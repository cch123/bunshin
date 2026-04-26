package bunshin

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

type ArchiveReplicationReport struct {
	SourceRecordingID      int64  `json:"source_recording_id"`
	DestinationRecordingID int64  `json:"destination_recording_id"`
	Segments               uint64 `json:"segments"`
	Bytes                  int64  `json:"bytes"`
	RecordingBytes         int64  `json:"recording_bytes"`
}

type archiveReplicationSegment struct {
	base int64
	path string
	size int64
}

func ReplicateArchive(ctx context.Context, src, dst *Archive, cfg ArchiveReplicationConfig) (ArchiveReplicationReport, error) {
	if src == nil {
		return ArchiveReplicationReport{}, invalidConfigf("source archive is required")
	}
	if dst == nil {
		return ArchiveReplicationReport{}, invalidConfigf("destination archive is required")
	}
	if src == dst {
		return ArchiveReplicationReport{}, invalidConfigf("source and destination archive must be different")
	}
	srcPath, _ := filepath.Abs(src.dir)
	dstPath, _ := filepath.Abs(dst.dir)
	if srcPath == dstPath {
		return ArchiveReplicationReport{}, invalidConfigf("source and destination archive must be different")
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

func (a *Archive) ReplicateTo(ctx context.Context, dst *Archive, cfg ArchiveReplicationConfig) (ArchiveReplicationReport, error) {
	return ReplicateArchive(ctx, a, dst, cfg)
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
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ArchiveReplicationReport{}, ErrArchiveClosed
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
	a.catalog.NextRecordingID = dstRecordingID + 1
	a.catalog.Recordings = append(a.catalog.Recordings, dstDesc)
	sortArchiveRecordings(a.catalog.Recordings)
	if err := a.saveCatalogLocked(); err != nil {
		a.catalog.NextRecordingID = oldNextRecordingID
		a.catalog.Recordings = a.catalog.Recordings[:oldRecordingsLen]
		removeArchiveFiles(copied)
		return report, err
	}
	return report, nil
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
