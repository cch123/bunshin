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
	commandBuffer := flags.Int("command-buffer", 64, "in-process driver command buffer")
	commandRingCapacity := flags.Int("command-ring-capacity", 64*1024, "driver IPC command ring capacity")
	eventRingCapacity := flags.Int("event-ring-capacity", 64*1024, "driver IPC event ring capacity")
	pollLimit := flags.Int("poll-limit", 64, "max IPC commands to poll per driver loop")
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
			Directory:       *dir,
			CommandBuffer:   *commandBuffer,
			ClientTimeout:   *clientTimeout,
			CleanupInterval: *cleanupInterval,
		},
		CommandRingCapacity: *commandRingCapacity,
		EventRingCapacity:   *eventRingCapacity,
		ResetIPC:            !*preserveIPC,
		HeartbeatInterval:   *heartbeat,
		PollLimit:           *pollLimit,
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

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bunshin-driver run -dir PATH [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver status -dir PATH [options]")
	fmt.Fprintln(os.Stderr, "       bunshin-driver terminate -dir PATH [options]")
}
