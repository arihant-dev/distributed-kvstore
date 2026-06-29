package store

import (
	"os"
	"reflect"
	"testing"
)

func TestWAL(t *testing.T) {
	// 1. Create a temporary file for our test WAL
	tmpFile := "test_wal.log"
	defer os.Remove(tmpFile) // clean up after test

	// 2. Open the WAL
	w, err := NewWAL(tmpFile)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}

	// 3. Create some entries
	entries := []LogEntry{
		{Index: 1, Op: OpPut, Key: "user1", Value: []byte("alice")},
		{Index: 2, Op: OpPut, Key: "user2", Value: []byte("bob")},
		{Index: 3, Op: OpDelete, Key: "user1", Value: []byte{}},
	}

	// 4. Append them to the log
	for _, entry := range entries {
		if err := w.Append(entry); err != nil {
			t.Fatalf("failed to append entry: %v", err)
		}
	}
	w.Close()

	// 5. Open a NEW WAL instance pointing to the same file (simulating a crash & reboot)
	w2, err := NewWAL(tmpFile)
	if err != nil {
		t.Fatalf("failed to open wal for replay: %v", err)
	}
	defer w2.Close()

	// 6. Replay the file
	recovered, err := w2.Replay()
	if err != nil {
		t.Fatalf("failed to replay wal: %v", err)
	}

	// 7. Verify the recovered entries perfectly match the original entries
	if !reflect.DeepEqual(entries, recovered) {
		t.Fatalf("recovered entries do not match!\nExpected: %+v\nGot: %+v", entries, recovered)
	}
}

// TestWAL_TornWrite simulates a chaos-kill mid-write (a torn write).
// The last entry is written partially and the process "crashes". Recovery must
// succeed gracefully, returning all complete entries and silently dropping the
// partial one.
func TestWAL_TornWrite(t *testing.T) {
	tmpFile := "test_wal_torn.log"
	defer os.Remove(tmpFile)

	w, err := NewWAL(tmpFile)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}

	goodEntries := []LogEntry{
		{Index: 1, Op: OpPut, Key: "key1", Value: []byte("val1")},
		{Index: 2, Op: OpPut, Key: "key2", Value: []byte("val2")},
	}
	for _, e := range goodEntries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Get file size after good writes
	info, err := w.file.Stat()
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	sizeAfterGood := info.Size()
	w.Close()

	// Append a few garbage bytes to simulate a partial (torn) write at end of file
	f, err := os.OpenFile(tmpFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for corruption: %v", err)
	}
	f.Write([]byte{0xDE, 0xAD, 0xBE}) // 3 bytes of a torn 13-byte header
	f.Close()

	// Verify file is larger than after good writes
	info2, _ := os.Stat(tmpFile)
	if info2.Size() <= sizeAfterGood {
		t.Fatal("expected file to be larger after corruption injection")
	}

	// Replay should succeed, returning only the good entries
	w2, err := NewWAL(tmpFile)
	if err != nil {
		t.Fatalf("failed to open wal for replay: %v", err)
	}
	defer w2.Close()

	recovered, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay must not return an error on a torn write (crash recovery), got: %v", err)
	}

	if !reflect.DeepEqual(goodEntries, recovered) {
		t.Fatalf("expected only good entries\nExpected: %+v\nGot:      %+v", goodEntries, recovered)
	}
}
