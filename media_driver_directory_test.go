package bunshin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMediaDriverDirectoryWritesMarkCountersAndReports(t *testing.T) {
	root := t.TempDir()
	metrics := &Metrics{}
	driver, err := StartMediaDriver(DriverConfig{
		Directory:       root,
		Metrics:         metrics,
		ClientTimeout:   time.Second,
		CleanupInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	layout, ok := driver.Directory()
	if !ok {
		t.Fatal("driver directory unavailable")
	}
	if layout.Directory != root && layout.Directory != filepath.Clean(root) {
		t.Fatalf("layout directory = %q, want %q", layout.Directory, root)
	}
	assertDriverFileExists(t, layout.MarkFile)
	assertDriverFileExists(t, layout.CountersFile)
	assertDriverFileExists(t, layout.LossReportFile)
	assertDriverFileExists(t, layout.ErrorReportFile)
	assertDriverDirectoryExists(t, layout.ClientsDirectory)
	assertDriverDirectoryExists(t, layout.PublicationsDirectory)
	assertDriverDirectoryExists(t, layout.SubscriptionsDirectory)
	assertDriverDirectoryExists(t, layout.BuffersDirectory)

	mark := readDriverJSON[DriverMarkFile](t, layout.MarkFile)
	if mark.Version != driverDirectoryLayoutVersion || mark.Status != DriverDirectoryStatusActive ||
		mark.PID == 0 || mark.ClientTimeout != time.Second.String() {
		t.Fatalf("unexpected mark file: %#v", mark)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := driver.NewClient(ctx, "directory-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ClosePublication(ctx, 99); !errors.Is(err, ErrDriverResourceNotFound) {
		t.Fatalf("ClosePublication() err = %v, want %v", err, ErrDriverResourceNotFound)
	}

	report, err := driver.FlushReports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Counters.Counters.ClientsRegistered != 1 || report.Counters.Counters.CommandsFailed != 1 ||
		report.Counters.Clients != 1 {
		t.Fatalf("unexpected flushed counters: %#v", report.Counters)
	}
	if len(report.Errors.Reports) != 1 || report.Errors.Reports[0].Operation != "command" {
		t.Fatalf("unexpected error reports: %#v", report.Errors.Reports)
	}

	counters := readDriverJSON[DriverCountersFile](t, layout.CountersFile)
	if counters.Counters.ClientsRegistered != 1 || counters.Counters.CommandsFailed != 1 ||
		counters.Clients != 1 {
		t.Fatalf("unexpected counters file: %#v", counters)
	}
	loss := readDriverJSON[DriverLossReportFile](t, layout.LossReportFile)
	if len(loss.Reports) != 0 {
		t.Fatalf("unexpected loss reports: %#v", loss.Reports)
	}
	errorReport := readDriverJSON[DriverErrorReportFile](t, layout.ErrorReportFile)
	if len(errorReport.Reports) != 1 || errorReport.Reports[0].Error == "" {
		t.Fatalf("unexpected error report file: %#v", errorReport)
	}

	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	mark = readDriverJSON[DriverMarkFile](t, layout.MarkFile)
	if mark.Status != DriverDirectoryStatusClosed || mark.ClosedAt == nil {
		t.Fatalf("unexpected closed mark file: %#v", mark)
	}
}

func TestMediaDriverDirectoryRejectsFilePath(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := StartMediaDriver(DriverConfig{Directory: path}); err == nil {
		t.Fatal("StartMediaDriver() err = nil, want invalid directory error")
	}
}

func assertDriverFileExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() {
		t.Fatalf("%s is a directory, want file", path)
	}
}

func assertDriverDirectoryExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", path)
	}
}

func readDriverJSON[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
