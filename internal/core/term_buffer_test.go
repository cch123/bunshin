package core

import (
	"errors"
	"testing"
)

func TestTermLogClaimComputesPosition(t *testing.T) {
	log, err := newTermLog(minTermLength, 7)
	if err != nil {
		t.Fatal(err)
	}

	first, err := log.append(44, func(appendResult termAppend) error {
		copy(appendResult.Bytes(), []byte("frame"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if first.TermID != 7 || first.TermOffset != 0 || first.Position != 64 {
		t.Fatalf("first append = %#v", first)
	}
	if first.AlignedLength != 64 || first.ActivePartitionIndex != 0 || first.ActiveTermCount != 0 {
		t.Fatalf("first append metadata = %#v", first)
	}
	if got := string(log.terms[0].data[:5]); got != "frame" {
		t.Fatalf("term bytes = %q, want frame", got)
	}

	second, err := log.append(65, func(termAppend) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.TermID != 7 || second.TermOffset != 64 || second.Position != 160 {
		t.Fatalf("second append = %#v", second)
	}
	if second.AlignedLength != 96 {
		t.Fatalf("second aligned length = %d, want 96", second.AlignedLength)
	}
}

func TestTermLogRotatesWhenActiveTermCannotFitRecord(t *testing.T) {
	log, err := newTermLog(minTermLength, 10)
	if err != nil {
		t.Fatal(err)
	}

	first, err := log.append(40*1024, func(termAppend) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.TermID != 10 || first.TermOffset != 0 {
		t.Fatalf("first append = %#v", first)
	}

	second, err := log.append(30*1024, func(termAppend) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.TermID != 11 || second.TermOffset != 0 || second.ActivePartitionIndex != 1 || second.ActiveTermCount != 1 {
		t.Fatalf("second append after rotation = %#v", second)
	}
	if second.Position != minTermLength+30*1024 {
		t.Fatalf("second position = %d, want %d", second.Position, minTermLength+30*1024)
	}
	if log.terms[0].state != termDirty || log.terms[1].state != termActive {
		t.Fatalf("states after rotation: term0=%d term1=%d", log.terms[0].state, log.terms[1].state)
	}
}

func TestNewTermLogRejectsInvalidTermLength(t *testing.T) {
	for _, termLength := range []int{minTermLength - 1, minTermLength + 1, maxTermLength * 2} {
		if _, err := newTermLog(termLength, 1); !errors.Is(err, errInvalidTermLength) {
			t.Fatalf("newTermLog(%d) err = %v, want %v", termLength, err, errInvalidTermLength)
		}
	}
}

func TestTermLogRejectsOversizedRecord(t *testing.T) {
	log, err := newTermLog(minTermLength, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.append(minTermLength+1, func(termAppend) error {
		return nil
	}); !errors.Is(err, errTermRecordTooLong) {
		t.Fatalf("claim err = %v, want %v", err, errTermRecordTooLong)
	}
}
