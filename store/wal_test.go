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
