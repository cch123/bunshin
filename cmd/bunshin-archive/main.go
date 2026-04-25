package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/xargin/bunshin"
)

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(2)
	}

	var (
		value any
		err   error
	)
	switch os.Args[1] {
	case "describe":
		if len(os.Args) != 3 {
			usage()
			os.Exit(2)
		}
		value, err = bunshin.DescribeArchive(os.Args[2])
	case "verify":
		if len(os.Args) != 3 {
			usage()
			os.Exit(2)
		}
		value, err = bunshin.VerifyArchive(os.Args[2])
	case "migrate":
		if len(os.Args) != 4 {
			usage()
			os.Exit(2)
		}
		value, err = bunshin.MigrateArchive(os.Args[2], os.Args[3])
	case "replicate":
		if len(os.Args) != 5 {
			usage()
			os.Exit(2)
		}
		recordingID, parseErr := strconv.ParseInt(os.Args[4], 10, 64)
		if parseErr != nil {
			fmt.Fprintln(os.Stderr, parseErr)
			os.Exit(2)
		}
		value, err = replicateArchive(os.Args[2], os.Args[3], recordingID)
	default:
		usage()
		os.Exit(2)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(value); encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bunshin-archive describe PATH")
	fmt.Fprintln(os.Stderr, "       bunshin-archive verify PATH")
	fmt.Fprintln(os.Stderr, "       bunshin-archive migrate SRC_PATH DST_PATH")
	fmt.Fprintln(os.Stderr, "       bunshin-archive replicate SRC_PATH DST_PATH RECORDING_ID")
}

func replicateArchive(srcPath, dstPath string, recordingID int64) (bunshin.ArchiveReplicationReport, error) {
	src, err := bunshin.OpenArchive(bunshin.ArchiveConfig{Path: srcPath})
	if err != nil {
		return bunshin.ArchiveReplicationReport{}, err
	}
	defer src.Close()

	dst, err := bunshin.OpenArchive(bunshin.ArchiveConfig{Path: dstPath})
	if err != nil {
		return bunshin.ArchiveReplicationReport{}, err
	}
	defer dst.Close()

	return bunshin.ReplicateArchive(context.Background(), src, dst, bunshin.ArchiveReplicationConfig{
		RecordingID: recordingID,
	})
}
