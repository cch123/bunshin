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
	assertDriverFileExists(t, layout.RingsReportFile)
	assertDriverDirectoryExists(t, layout.ClientsDirectory)
	assertDriverDirectoryExists(t, layout.PublicationsDirectory)
	assertDriverDirectoryExists(t, layout.SubscriptionsDirectory)
	assertDriverDirectoryExists(t, layout.BuffersDirectory)

	mark := readDriverJSON[DriverMarkFile](t, layout.MarkFile)
	if mark.Version != driverDirectoryLayoutVersion || mark.Status != DriverDirectoryStatusActive ||
		mark.PID == 0 || mark.ClientTimeout != time.Second.String() ||
		mark.StallThreshold != defaultDriverStallThreshold.String() {
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
	if report.Counters.StatusCounters.ActiveClients != 1 || report.Counters.StatusCounters.ActivePublications != 0 ||
		report.Counters.StatusCounters.ActiveSubscriptions != 0 {
		t.Fatalf("unexpected status counters: %#v", report.Counters.StatusCounters)
	}
	assertCounterSnapshot(t, report.Counters.CounterSnapshots, CounterDriverClientsRegistered, CounterScopeDriver, "driver_clients_registered", 1)
	assertCounterSnapshot(t, report.Counters.CounterSnapshots, CounterDriverCommandsFailed, CounterScopeDriver, "driver_commands_failed", 1)
	assertCounterSnapshot(t, report.Counters.CounterSnapshots, CounterDriverActiveClients, CounterScopeDriver, "driver_active_clients", 1)
	assertCounterSnapshot(t, report.Counters.CounterSnapshots, CounterMetricsMessagesSent, CounterScopeMetrics, "messages_sent", 0)
	if len(report.Errors.Reports) != 1 || report.Errors.Reports[0].Operation != "command" {
		t.Fatalf("unexpected error reports: %#v", report.Errors.Reports)
	}
	if len(report.Rings.Subscriptions) != 0 {
		t.Fatalf("unexpected rings report: %#v", report.Rings)
	}

	counters := readDriverJSON[DriverCountersFile](t, layout.CountersFile)
	if counters.Counters.ClientsRegistered != 1 || counters.Counters.CommandsFailed != 1 ||
		counters.Clients != 1 {
		t.Fatalf("unexpected counters file: %#v", counters)
	}
	if counters.StatusCounters.ActiveClients != 1 {
		t.Fatalf("unexpected counters file status counters: %#v", counters.StatusCounters)
	}
	assertCounterSnapshot(t, counters.CounterSnapshots, CounterDriverClientsRegistered, CounterScopeDriver, "driver_clients_registered", 1)
	assertCounterSnapshot(t, counters.CounterSnapshots, CounterDriverCommandsFailed, CounterScopeDriver, "driver_commands_failed", 1)
	assertCounterSnapshot(t, counters.CounterSnapshots, CounterDriverActiveClients, CounterScopeDriver, "driver_active_clients", 1)
	loss := readDriverJSON[DriverLossReportFile](t, layout.LossReportFile)
	if len(loss.Reports) != 0 {
		t.Fatalf("unexpected loss reports: %#v", loss.Reports)
	}
	errorReport := readDriverJSON[DriverErrorReportFile](t, layout.ErrorReportFile)
	if len(errorReport.Reports) != 1 || errorReport.Reports[0].Error == "" {
		t.Fatalf("unexpected error report file: %#v", errorReport)
	}
	rings := readDriverJSON[DriverRingsReportFile](t, layout.RingsReportFile)
	if len(rings.Subscriptions) != 0 {
		t.Fatalf("unexpected rings report file: %#v", rings)
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
