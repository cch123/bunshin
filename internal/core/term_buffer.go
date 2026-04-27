package core

import (
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
)

const (
	termPartitionCount = 3
	minTermLength      = 64 * 1024
	maxTermLength      = 1024 * 1024 * 1024
	termFrameAlignment = 32
)

var (
	errInvalidTermLength = errors.New("invalid term length")
	errTermRecordTooLong = errors.New("term record too long")
)

type termState uint8

const (
	termClean termState = iota
	termActive
	termDirty
)

type termLog struct {
	mu                  sync.Mutex
	termLength          int
	positionBitsToShift uint
	initialTermID       int32
	activeTermCount     int64
	terms               [termPartitionCount]termBuffer
	files               []*os.File
	paths               []string
	mapped              bool
}

type termBuffer struct {
	termID int32
	tail   int
	state  termState
	data   []byte
	path   string
}

type termAppend struct {
	TermID               int32
	TermOffset           int32
	Position             int64
	FrameLength          int
	AlignedLength        int
	ActiveTermCount      int64
	ActivePartitionIndex int

	buffer []byte
}

func newTermLog(termLength int, initialTermID int32) (*termLog, error) {
	if err := validateTermLength(termLength); err != nil {
		return nil, err
	}

	l := &termLog{
		termLength:          termLength,
		positionBitsToShift: termPositionBitsToShift(termLength),
		initialTermID:       initialTermID,
	}
	for i := range l.terms {
		l.terms[i] = termBuffer{
			termID: initialTermID + int32(i),
			state:  termClean,
			data:   make([]byte, termLength),
		}
	}
	l.terms[0].state = termActive
	return l, nil
}

func newMappedTermLog(termLength int, initialTermID int32, directory string) (*termLog, error) {
	if directory == "" {
		return newTermLog(termLength, initialTermID)
	}
	if err := validateTermLength(termLength); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create term buffer directory: %w", err)
	}

	l := &termLog{
		termLength:          termLength,
		positionBitsToShift: termPositionBitsToShift(termLength),
		initialTermID:       initialTermID,
		files:               make([]*os.File, 0, termPartitionCount),
		paths:               make([]string, 0, termPartitionCount),
		mapped:              true,
	}
	for i := range l.terms {
		path := filepath.Join(directory, fmt.Sprintf("term-%d.buf", i))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
		if err != nil {
			_ = l.close()
			return nil, fmt.Errorf("open term buffer file: %w", err)
		}
		l.files = append(l.files, file)
		l.paths = append(l.paths, path)
		if err := file.Truncate(int64(termLength)); err != nil {
			_ = l.close()
			return nil, fmt.Errorf("truncate term buffer file: %w", err)
		}
		data, err := mmapFile(file, termLength)
		if err != nil {
			_ = l.close()
			return nil, fmt.Errorf("mmap term buffer file: %w", err)
		}
		l.terms[i] = termBuffer{
			termID: initialTermID + int32(i),
			state:  termClean,
			data:   data,
			path:   path,
		}
	}
	l.terms[0].state = termActive
	return l, nil
}

func validateTermLength(termLength int) error {
	if termLength < minTermLength || termLength > maxTermLength || !isPowerOfTwo(termLength) {
		return fmt.Errorf("%w: %d", errInvalidTermLength, termLength)
	}
	return nil
}

func isPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}

func (l *termLog) append(frameLength int, write func(termAppend) error) (termAppend, error) {
	if frameLength <= 0 {
		return termAppend{}, fmt.Errorf("%w: %d", errTermRecordTooLong, frameLength)
	}
	if write == nil {
		return termAppend{}, errors.New("term append write function is required")
	}

	alignedLength := align(frameLength, termFrameAlignment)
	if alignedLength > l.termLength {
		return termAppend{}, fmt.Errorf("%w: %d", errTermRecordTooLong, frameLength)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	index := int(l.activeTermCount % termPartitionCount)
	active := &l.terms[index]
	if l.termLength-active.tail < alignedLength {
		l.rotate()
		index = int(l.activeTermCount % termPartitionCount)
		active = &l.terms[index]
	}

	termOffset := active.tail
	active.tail += alignedLength
	position := computeTermPosition(active.termID, active.tail, l.positionBitsToShift, l.initialTermID)

	appendResult := termAppend{
		TermID:               active.termID,
		TermOffset:           int32(termOffset),
		Position:             position,
		FrameLength:          frameLength,
		AlignedLength:        alignedLength,
		ActiveTermCount:      l.activeTermCount,
		ActivePartitionIndex: index,
		buffer:               active.data[termOffset : termOffset+frameLength],
	}
	if err := write(appendResult); err != nil {
		return termAppend{}, err
	}
	return appendResult, nil
}

func (l *termLog) rotate() {
	index := int(l.activeTermCount % termPartitionCount)
	l.terms[index].state = termDirty

	l.activeTermCount++
	nextIndex := int(l.activeTermCount % termPartitionCount)
	next := &l.terms[nextIndex]
	clear(next.data)
	next.termID = l.initialTermID + int32(l.activeTermCount)
	next.tail = 0
	next.state = termActive
}

func (a termAppend) Bytes() []byte {
	return a.buffer
}

func (l *termLog) close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var err error
	for i := range l.terms {
		if l.mapped && l.terms[i].data != nil {
			err = errors.Join(err, munmapFile(l.terms[i].data))
		}
		l.terms[i].data = nil
	}
	for _, file := range l.files {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	l.files = nil
	return err
}

func (l *termLog) mappedFiles() []string {
	if l == nil || !l.mapped {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.paths...)
}

func (l *termLog) isMapped() bool {
	return l != nil && l.mapped
}

func (l *termLog) position(termID, termOffset int32) (int64, error) {
	if termOffset < 0 || int(termOffset) > l.termLength {
		return 0, fmt.Errorf("invalid term offset: %d", termOffset)
	}
	if termID < l.initialTermID {
		return 0, fmt.Errorf("invalid term id: %d", termID)
	}
	return computeTermPosition(termID, int(termOffset), l.positionBitsToShift, l.initialTermID), nil
}

func align(value, alignment int) int {
	return (value + (alignment - 1)) & ^(alignment - 1)
}

func computeTermPosition(termID int32, termOffset int, positionBitsToShift uint, initialTermID int32) int64 {
	termCount := int64(termID - initialTermID)
	return (termCount << positionBitsToShift) + int64(termOffset)
}

func termPositionBitsToShift(termLength int) uint {
	return uint(bits.TrailingZeros(uint(termLength)))
}
