package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/xargin/bunshin"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var (
		value any
		err   error
	)
	switch os.Args[1] {
	case "run":
		err = runDriver(os.Args[2:])
	case "status":
		value, err = statusDriver(os.Args[2:])
	case "counters":
		value, err = countersDriver(os.Args[2:])
	case "errors":
		value, err = errorsDriver(os.Args[2:])
	case "loss":
		value, err = lossDriver(os.Args[2:])
	case "streams":
		value, err = streamsDriver(os.Args[2:])
	case "rings":
		value, err = ringsDriver(os.Args[2:])
	case "flush":
		value, err = flushDriver(os.Args[2:])
	case "terminate":
		value, err = terminateDriver(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}

	if value != nil {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if encodeErr := encoder.Encode(value); encodeErr != nil {
			fmt.Fprintln(os.Stderr, encodeErr)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, context.Canceled) {
			return
		}
		os.Exit(1)
	}
}

func runDriver(args []string) error {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	heartbeat := flags.Duration("heartbeat", time.Second, "driver heartbeat/report interval")
	clientTimeout := flags.Duration("client-timeout", 30*time.Second, "stale client timeout")
	cleanupInterval := flags.Duration("cleanup-interval", time.Second, "stale client cleanup interval")
	staleTimeout := flags.Duration("stale-timeout", 5*time.Second, "active driver directory stale timeout")
	commandBuffer := flags.Int("command-buffer", 64, "in-process driver command buffer")
	commandRingCapacity := flags.Int("command-ring-capacity", 64*1024, "driver IPC command ring capacity")
	eventRingCapacity := flags.Int("event-ring-capacity", 64*1024, "driver IPC event ring capacity")
	pollLimit := flags.Int("poll-limit", 64, "max IPC commands to poll per driver loop")
	subscriptionPumpLimit := flags.Int("subscription-pump-limit", 64, "max external subscription messages to pump per driver loop")
	disableSubscriptionPump := flags.Bool("disable-subscription-pump", false, "disable background pumping of external subscriptions into shared images")
	preserveIPC := flags.Bool("preserve-ipc", false, "preserve existing IPC ring contents")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("driver directory is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return bunshin.RunMediaDriverProcess(ctx, bunshin.DriverProcessConfig{
		Directory: *dir,
		Driver: bunshin.DriverConfig{
			Directory:             *dir,
			CommandBuffer:         *commandBuffer,
			ClientTimeout:         *clientTimeout,
			CleanupInterval:       *cleanupInterval,
			DirectoryStaleTimeout: *staleTimeout,
		},
		CommandRingCapacity:     *commandRingCapacity,
		EventRingCapacity:       *eventRingCapacity,
		ResetIPC:                !*preserveIPC,
		HeartbeatInterval:       *heartbeat,
		StaleTimeout:            *staleTimeout,
		PollLimit:               *pollLimit,
		SubscriptionPumpLimit:   *subscriptionPumpLimit,
		DisableSubscriptionPump: *disableSubscriptionPump,
	})
}

func statusDriver(args []string) (bunshin.DriverProcessStatus, error) {
	flags := flag.NewFlagSet("status", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	staleTimeout := flags.Duration("stale-timeout", 5*time.Second, "heartbeat age after which an active driver is stale")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverProcessStatus{}, err
	}
	if *dir == "" {
		return bunshin.DriverProcessStatus{}, errors.New("driver directory is required")
	}
	return bunshin.CheckDriverProcess(*dir, *staleTimeout)
}

func countersDriver(args []string) (bunshin.DriverCountersFile, error) {
	flags := flag.NewFlagSet("counters", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverCountersFile{}, err
	}
	if *dir == "" {
		return bunshin.DriverCountersFile{}, errors.New("driver directory is required")
	}
	return readDriverReport[bunshin.DriverCountersFile](*dir, func(layout bunshin.DriverDirectoryLayout) string {
		return layout.CountersFile
	})
}

func errorsDriver(args []string) (bunshin.DriverErrorReportFile, error) {
	flags := flag.NewFlagSet("errors", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverErrorReportFile{}, err
	}
	if *dir == "" {
		return bunshin.DriverErrorReportFile{}, errors.New("driver directory is required")
	}
	return readDriverReport[bunshin.DriverErrorReportFile](*dir, func(layout bunshin.DriverDirectoryLayout) string {
		return layout.ErrorReportFile
	})
}

func lossDriver(args []string) (bunshin.DriverLossReportFile, error) {
	flags := flag.NewFlagSet("loss", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverLossReportFile{}, err
	}
	if *dir == "" {
		return bunshin.DriverLossReportFile{}, errors.New("driver directory is required")
	}
	return readDriverReport[bunshin.DriverLossReportFile](*dir, func(layout bunshin.DriverDirectoryLayout) string {
		return layout.LossReportFile
	})
}

func streamsDriver(args []string) (bunshin.DriverSnapshot, error) {
	flags := flag.NewFlagSet("streams", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	timeout := flags.Duration("timeout", 5*time.Second, "snapshot timeout")
	report := flags.Bool("report", false, "read the persisted streams report instead of querying the live driver")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverSnapshot{}, err
	}
	if *dir == "" {
		return bunshin.DriverSnapshot{}, errors.New("driver directory is required")
	}
	if *report {
		streams, err := readDriverReport[bunshin.DriverStreamsReportFile](*dir, func(layout bunshin.DriverDirectoryLayout) string {
			return layout.StreamsReportFile
		})
		if err != nil {
			return bunshin.DriverSnapshot{}, err
		}
		return streams.Snapshot, nil
	}
	return driverSnapshot(*dir, *timeout)
}

func driverSnapshot(dir string, timeout time.Duration) (bunshin.DriverSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	event, err := sendDriverIPCCommand(ctx, dir, bunshin.DriverIPCCommand{
		Type: bunshin.DriverIPCCommandSnapshot,
	})
	if err != nil {
		return bunshin.DriverSnapshot{}, err
	}
	if event.Snapshot == nil {
		return bunshin.DriverSnapshot{}, errors.New("driver returned no snapshot")
	}
	return *event.Snapshot, nil
}

func ringsDriver(args []string) (bunshin.DriverRingsReportFile, error) {
	flags := flag.NewFlagSet("rings", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	timeout := flags.Duration("timeout", 5*time.Second, "snapshot timeout")
	report := flags.Bool("report", false, "read the persisted rings report instead of querying the live driver")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverRingsReportFile{}, err
	}
	if *dir == "" {
		return bunshin.DriverRingsReportFile{}, errors.New("driver directory is required")
	}
	if *report {
		return readDriverReport[bunshin.DriverRingsReportFile](*dir, func(layout bunshin.DriverDirectoryLayout) string {
			return layout.RingsReportFile
		})
	}
	snapshot, err := driverSnapshot(*dir, *timeout)
	if err != nil {
		return bunshin.DriverRingsReportFile{}, err
	}
	return bunshin.BuildDriverRingsReport(snapshot, time.Now()), nil
}

func flushDriver(args []string) (bunshin.DriverDirectoryReport, error) {
	flags := flag.NewFlagSet("flush", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	timeout := flags.Duration("timeout", 5*time.Second, "flush timeout")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverDirectoryReport{}, err
	}
	if *dir == "" {
		return bunshin.DriverDirectoryReport{}, errors.New("driver directory is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	event, err := sendDriverIPCCommand(ctx, *dir, bunshin.DriverIPCCommand{
		Type: bunshin.DriverIPCCommandFlushReports,
	})
	if err != nil {
		return bunshin.DriverDirectoryReport{}, err
	}
	if event.Directory == nil {
		return bunshin.DriverDirectoryReport{}, errors.New("driver returned no directory report")
	}
	return *event.Directory, nil
}

func terminateDriver(args []string) (bunshin.DriverIPCEvent, error) {
	flags := flag.NewFlagSet("terminate", flag.ExitOnError)
	dir := flags.String("dir", "", "driver directory")
	timeout := flags.Duration("timeout", 5*time.Second, "termination timeout")
	if err := flags.Parse(args); err != nil {
		return bunshin.DriverIPCEvent{}, err
	}
	if *dir == "" {
		return bunshin.DriverIPCEvent{}, errors.New("driver directory is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return bunshin.TerminateDriverProcess(ctx, *dir)
}

func readDriverReport[T any](dir string, path func(bunshin.DriverDirectoryLayout) string) (T, error) {
	var zero T
	layout, err := bunshin.ResolveDriverDirectoryLayout(dir)
	if err != nil {
		return zero, err
	}
	data, err := os.ReadFile(path(layout))
	if err != nil {
		return zero, err
	}
	var report T
	if err := json.Unmarshal(data, &report); err != nil {
		return zero, err
	}
	return report, nil
}

func sendDriverIPCCommand(ctx context.Context, dir string, command bunshin.DriverIPCCommand) (bunshin.DriverIPCEvent, error) {
	layout, err := bunshin.ResolveDriverDirectoryLayout(dir)
	if err != nil {
		return bunshin.DriverIPCEvent{}, err
	}
	responseRingPath := fmt.Sprintf("%s/tool-events-%d-%d.ring", layout.ClientsDirectory, os.Getpid(), time.Now().UnixNano())
	ipc, err := bunshin.OpenDriverIPC(bunshin.DriverIPCConfig{
		Directory:     dir,
		EventRingPath: responseRingPath,
	})
	if err != nil {
		return bunshin.DriverIPCEvent{}, err
	}
	defer ipc.Close()
	defer os.Remove(responseRingPath)

	correlationID, err := ipc.SendCommand(ctx, command, nil)
	if err != nil {
		return bunshin.DriverIPCEvent{}, err
	}
	idle := bunshin.NewDefaultBackoffIdleStrategy()
	for {
		select {
		case <-ctx.Done():
			return bunshin.DriverIPCEvent{}, ctx.Err()
		default:
		}
		var matched bunshin.DriverIPCEvent
		polled, err := ipc.PollEvents(1, func(event bunshin.DriverIPCEvent) error {
			if event.CorrelationID == correlationID {
				matched = event
			}
			return nil
		})
		if err != nil {
			if !errors.Is(err, bunshin.ErrIPCRingEmpty) {
				return bunshin.DriverIPCEvent{}, err
			}
			idle.Idle(0)
			continue
		}
		if matched.Type != "" {
			if matched.Type == bunshin.DriverIPCEventCommandError {
				return bunshin.DriverIPCEvent{}, errors.New(matched.Error)
			}
			return matched, nil
		}
		if polled > 0 {
			idle.Reset()
			continue
		}
		idle.Idle(0)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bunshin-driver run -dir PATH [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver status -dir PATH [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver counters -dir PATH")
	fmt.Fprintln(os.Stderr, "       bunshin-driver errors -dir PATH")
	fmt.Fprintln(os.Stderr, "       bunshin-driver loss -dir PATH")
	fmt.Fprintln(os.Stderr, "       bunshin-driver streams -dir PATH [-report] [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver rings -dir PATH [-report] [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver flush -dir PATH [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver terminate -dir PATH [options]")
}
