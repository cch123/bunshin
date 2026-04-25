package bunshin

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	driverDirectoryLayoutVersion = 1

	driverMarkFileName       = "driver.mark"
	driverCommandRingName    = "command.ring"
	driverEventRingName      = "events.ring"
	driverCountersReportName = "counters.json"
	driverLossReportName     = "loss-report.json"
	driverErrorReportName    = "error-report.json"
)

var ErrDriverDirectoryUnavailable = errors.New("bunshin driver directory: unavailable")

type DriverDirectoryStatus string

const (
	DriverDirectoryStatusActive DriverDirectoryStatus = "active"
	DriverDirectoryStatusClosed DriverDirectoryStatus = "closed"
)

type DriverDirectoryLayout struct {
	Directory              string `json:"directory"`
	MarkFile               string `json:"mark_file"`
	CommandRingFile        string `json:"command_ring_file"`
	EventRingFile          string `json:"event_ring_file"`
	ReportsDirectory       string `json:"reports_directory"`
	CountersFile           string `json:"counters_file"`
	LossReportFile         string `json:"loss_report_file"`
	ErrorReportFile        string `json:"error_report_file"`
	ClientsDirectory       string `json:"clients_directory"`
	PublicationsDirectory  string `json:"publications_directory"`
	SubscriptionsDirectory string `json:"subscriptions_directory"`
	BuffersDirectory       string `json:"buffers_directory"`
}

type DriverMarkFile struct {
	Version         int                   `json:"version"`
	PID             int                   `json:"pid"`
	Status          DriverDirectoryStatus `json:"status"`
	StartedAt       time.Time             `json:"started_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	ClosedAt        *time.Time            `json:"closed_at,omitempty"`
	CommandBuffer   int                   `json:"command_buffer"`
	ClientTimeout   string                `json:"client_timeout"`
	CleanupInterval string                `json:"cleanup_interval"`
}

type DriverCountersFile struct {
	UpdatedAt     time.Time       `json:"updated_at"`
	Counters      DriverCounters  `json:"counters"`
	Metrics       MetricsSnapshot `json:"metrics"`
	Clients       int             `json:"clients"`
	Publications  int             `json:"publications"`
	Subscriptions int             `json:"subscriptions"`
}

type DriverLossReportFile struct {
	UpdatedAt time.Time                  `json:"updated_at"`
	Reports   []DriverLossReportSnapshot `json:"reports"`
}

type DriverErrorReport struct {
	Time      time.Time      `json:"time"`
	Level     LogLevel       `json:"level"`
	Component string         `json:"component"`
	Operation string         `json:"operation"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
	Error     string         `json:"error"`
}

type DriverErrorReportFile struct {
	UpdatedAt time.Time           `json:"updated_at"`
	Reports   []DriverErrorReport `json:"reports"`
}

type DriverDirectoryReport struct {
	Layout   DriverDirectoryLayout `json:"layout"`
	Mark     DriverMarkFile        `json:"mark"`
	Counters DriverCountersFile    `json:"counters"`
	Loss     DriverLossReportFile  `json:"loss"`
	Errors   DriverErrorReportFile `json:"errors"`
}

type driverDirectory struct {
	layout DriverDirectoryLayout
	mark   DriverMarkFile
}

func ResolveDriverDirectoryLayout(directory string) (DriverDirectoryLayout, error) {
	if directory == "" {
		return DriverDirectoryLayout{}, invalidConfigf("driver directory is required")
	}
	root, err := filepath.Abs(directory)
	if err != nil {
		return DriverDirectoryLayout{}, fmt.Errorf("resolve driver directory: %w", err)
	}
	reportsDir := filepath.Join(root, "reports")
	return DriverDirectoryLayout{
		Directory:              root,
		MarkFile:               filepath.Join(root, driverMarkFileName),
		CommandRingFile:        filepath.Join(root, driverCommandRingName),
		EventRingFile:          filepath.Join(root, driverEventRingName),
		ReportsDirectory:       reportsDir,
		CountersFile:           filepath.Join(reportsDir, driverCountersReportName),
		LossReportFile:         filepath.Join(reportsDir, driverLossReportName),
		ErrorReportFile:        filepath.Join(reportsDir, driverErrorReportName),
		ClientsDirectory:       filepath.Join(root, "clients"),
		PublicationsDirectory:  filepath.Join(root, "publications"),
		SubscriptionsDirectory: filepath.Join(root, "subscriptions"),
		BuffersDirectory:       filepath.Join(root, "buffers"),
	}, nil
}

func openDriverDirectory(cfg DriverConfig) (*driverDirectory, error) {
	if cfg.Directory == "" {
		return nil, nil
	}
	layout, err := ResolveDriverDirectoryLayout(cfg.Directory)
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{
		layout.Directory,
		layout.ReportsDirectory,
		layout.ClientsDirectory,
		layout.PublicationsDirectory,
		layout.SubscriptionsDirectory,
		layout.BuffersDirectory,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create driver directory %s: %w", dir, err)
		}
	}

	now := time.Now().UTC()
	directory := &driverDirectory{
		layout: layout,
		mark: DriverMarkFile{
			Version:         driverDirectoryLayoutVersion,
			PID:             os.Getpid(),
			Status:          DriverDirectoryStatusActive,
			StartedAt:       now,
			UpdatedAt:       now,
			CommandBuffer:   cfg.CommandBuffer,
			ClientTimeout:   cfg.ClientTimeout.String(),
			CleanupInterval: cfg.CleanupInterval.String(),
		},
	}
	report := DriverDirectoryReport{
		Layout: directory.layout,
		Mark:   directory.mark,
		Counters: DriverCountersFile{
			UpdatedAt: now,
			Metrics:   cfg.Metrics.Snapshot(),
		},
		Loss: DriverLossReportFile{
			UpdatedAt: now,
		},
		Errors: DriverErrorReportFile{
			UpdatedAt: now,
		},
	}
	return directory, directory.writeReport(report)
}

func (state *driverState) flushDriverDirectory(now time.Time, status DriverDirectoryStatus) (DriverDirectoryReport, error) {
	if state == nil || state.directory == nil {
		return DriverDirectoryReport{}, ErrDriverDirectoryUnavailable
	}
	now = now.UTC()
	snapshot := state.snapshot()
	state.directory.mark.Status = status
	state.directory.mark.UpdatedAt = now
	if status == DriverDirectoryStatusClosed {
		closedAt := now
		state.directory.mark.ClosedAt = &closedAt
	}
	report := DriverDirectoryReport{
		Layout: state.directory.layout,
		Mark:   state.directory.mark,
		Counters: DriverCountersFile{
			UpdatedAt:     now,
			Counters:      snapshot.Counters,
			Metrics:       state.metrics.Snapshot(),
			Clients:       len(snapshot.Clients),
			Publications:  len(snapshot.Publications),
			Subscriptions: len(snapshot.Subscriptions),
		},
		Loss: DriverLossReportFile{
			UpdatedAt: now,
			Reports:   append([]DriverLossReportSnapshot(nil), snapshot.LossReports...),
		},
		Errors: DriverErrorReportFile{
			UpdatedAt: now,
			Reports:   cloneDriverErrorReports(state.errorReports),
		},
	}
	if err := state.directory.writeReport(report); err != nil {
		return DriverDirectoryReport{}, err
	}
	return report, nil
}

func (d *driverDirectory) writeReport(report DriverDirectoryReport) error {
	if d == nil {
		return ErrDriverDirectoryUnavailable
	}
	if err := writeDriverJSONFile(d.layout.MarkFile, report.Mark); err != nil {
		return fmt.Errorf("write driver mark file: %w", err)
	}
	if err := writeDriverJSONFile(d.layout.CountersFile, report.Counters); err != nil {
		return fmt.Errorf("write driver counters file: %w", err)
	}
	if err := writeDriverJSONFile(d.layout.LossReportFile, report.Loss); err != nil {
		return fmt.Errorf("write driver loss report file: %w", err)
	}
	if err := writeDriverJSONFile(d.layout.ErrorReportFile, report.Errors); err != nil {
		return fmt.Errorf("write driver error report file: %w", err)
	}
	return nil
}

func writeDriverJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode JSON: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}
	return nil
}

func (state *driverState) recordDriverError(level LogLevel, operation, message string, fields map[string]any, err error) {
	if state == nil || err == nil {
		return
	}
	state.errorReports = append(state.errorReports, DriverErrorReport{
		Time:      time.Now().UTC(),
		Level:     level,
		Component: "media_driver",
		Operation: operation,
		Message:   message,
		Fields:    cloneDriverFields(fields),
		Error:     err.Error(),
	})
}

func cloneDriverErrorReports(reports []DriverErrorReport) []DriverErrorReport {
	cloned := make([]DriverErrorReport, len(reports))
	for i, report := range reports {
		cloned[i] = report
		cloned[i].Fields = cloneDriverFields(report.Fields)
	}
	return cloned
}

func cloneDriverFields(fields map[string]any) map[string]any {
	if fields == nil {
		return nil
	}
	cloned := make(map[string]any, len(fields))
	for key, value := range fields {
		cloned[key] = value
	}
	return cloned
}
