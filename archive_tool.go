package bunshin

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type ArchiveDescription struct {
	Path             string                       `json:"path"`
	Recordings       []ArchiveRecordingDescriptor `json:"recordings"`
	Segments         []ArchiveSegmentDescriptor   `json:"segments"`
	RecordingCount   int                          `json:"recording_count"`
	SegmentCount     int                          `json:"segment_count"`
	AttachedSegments int                          `json:"attached_segments"`
	DetachedSegments int                          `json:"detached_segments"`
	MissingSegments  int                          `json:"missing_segments"`
	Bytes            int64                        `json:"bytes"`
}

type ArchiveVerificationReport struct {
	Description      ArchiveDescription         `json:"description"`
	Integrity        ArchiveIntegrityReport     `json:"integrity"`
	DetachedSegments []ArchiveSegmentDescriptor `json:"detached_segments"`
	MissingSegments  []ArchiveSegmentDescriptor `json:"missing_segments"`
	OK               bool                       `json:"ok"`
}

type ArchiveMigrationReport struct {
	SourcePath      string                    `json:"source_path"`
	DestinationPath string                    `json:"destination_path"`
	Files           uint64                    `json:"files"`
	Bytes           int64                     `json:"bytes"`
	Verification    ArchiveVerificationReport `json:"verification"`
}

func DescribeArchive(path string) (ArchiveDescription, error) {
	archive, err := openArchiveTool(path)
	if err != nil {
		return ArchiveDescription{}, err
	}
	defer archive.Close()

	recordings, err := archive.ListRecordings()
	if err != nil {
		return ArchiveDescription{}, err
	}
	description := ArchiveDescription{
		Path:           path,
		Recordings:     recordings,
		RecordingCount: len(recordings),
	}
	for _, recording := range recordings {
		description.Bytes += recording.StopPosition - recording.StartPosition
		segments, err := archive.ListRecordingSegments(recording.RecordingID)
		if err != nil {
			return ArchiveDescription{}, err
		}
		description.Segments = append(description.Segments, segments...)
	}
	description.SegmentCount = len(description.Segments)
	for _, segment := range description.Segments {
		switch segment.State {
		case ArchiveSegmentAttached:
			description.AttachedSegments++
		case ArchiveSegmentDetached:
			description.DetachedSegments++
		case ArchiveSegmentMissing:
			description.MissingSegments++
		}
	}
	return description, nil
}

func VerifyArchive(path string) (ArchiveVerificationReport, error) {
	description, err := DescribeArchive(path)
	if err != nil {
		return ArchiveVerificationReport{}, err
	}
	report := ArchiveVerificationReport{
		Description: description,
		OK:          true,
	}
	for _, segment := range description.Segments {
		switch segment.State {
		case ArchiveSegmentDetached:
			report.DetachedSegments = append(report.DetachedSegments, segment)
		case ArchiveSegmentMissing:
			report.MissingSegments = append(report.MissingSegments, segment)
		}
	}

	archive, err := openArchiveTool(path)
	if err != nil {
		report.OK = false
		return report, err
	}
	defer archive.Close()

	integrity, scanErr := archive.IntegrityScan()
	report.Integrity = integrity
	if scanErr != nil || len(report.DetachedSegments) > 0 || len(report.MissingSegments) > 0 {
		report.OK = false
	}
	return report, scanErr
}

func MigrateArchive(srcPath, dstPath string) (ArchiveMigrationReport, error) {
	if dstPath == "" {
		return ArchiveMigrationReport{}, invalidConfigf("archive migration destination is required")
	}
	if _, err := VerifyArchive(srcPath); err != nil {
		return ArchiveMigrationReport{}, err
	}
	if err := prepareArchiveMigrationDestination(dstPath); err != nil {
		return ArchiveMigrationReport{}, err
	}

	report := ArchiveMigrationReport{
		SourcePath:      srcPath,
		DestinationPath: dstPath,
	}
	files, err := archiveToolFiles(srcPath)
	if err != nil {
		return report, err
	}
	for _, name := range files {
		bytes, err := copyArchiveToolFile(filepath.Join(srcPath, name), filepath.Join(dstPath, name))
		if err != nil {
			return report, err
		}
		report.Files++
		report.Bytes += bytes
	}

	verification, err := VerifyArchive(dstPath)
	report.Verification = verification
	if err != nil {
		return report, err
	}
	return report, nil
}

func openArchiveTool(path string) (*Archive, error) {
	if path == "" {
		return nil, invalidConfigf("archive path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat archive directory: %w", err)
	}
	if !info.IsDir() {
		return nil, invalidConfigf("archive path must be a directory: %s", path)
	}
	return OpenArchive(ArchiveConfig{Path: path})
}

func prepareArchiveMigrationDestination(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return fmt.Errorf("stat archive migration destination: %w", err)
	}
	if !info.IsDir() {
		return invalidConfigf("archive migration destination must be a directory: %s", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read archive migration destination: %w", err)
	}
	if len(entries) > 0 {
		return invalidConfigf("archive migration destination is not empty: %s", path)
	}
	return nil
}

func archiveToolFiles(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read archive directory: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == archiveCatalogFile || strings.HasSuffix(name, ".rec") || strings.HasSuffix(name, archiveDetachedSegmentExt) {
			files = append(files, name)
		}
	}
	return files, nil
}

func copyArchiveToolFile(src, dst string) (int64, error) {
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
	written, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return written, fmt.Errorf("copy archive file: %w", copyErr)
	}
	if syncErr != nil {
		return written, fmt.Errorf("sync archive file: %w", syncErr)
	}
	if closeErr != nil {
		return written, fmt.Errorf("close archive file: %w", closeErr)
	}
	return written, nil
}
