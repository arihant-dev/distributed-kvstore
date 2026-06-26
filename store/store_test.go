package store

import (
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	tmpFile := "test_store_wal.log"
	defer func() {
		err := os.Remove(tmpFile)
		if err != nil {
			t.Fatalf("unable to remove the file: %v", err)
		}
	}()

	// 1. Initialize empty store
	s, err := NewStore(tmpFile)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// 2. Perform operations
	err = s.Put("hero", []byte("batman"))
	if err != nil {
		t.Fatalf("unable to put a value to the key: %v", err)
	}
	err = s.Put("villain", []byte("joker"))
	if err != nil {
		t.Fatalf("unable to put a value to the key: %v", err)
	}
	err = s.Delete("villain")
	if err != nil {
		t.Fatalf("unable to delete the key: %v", err)
	}

	val, ok := s.Get("hero")
	if !ok || string(val) != "batman" {
		t.Fatalf("expected batman, got %v", string(val))
	}

	_, ok = s.Get("villain")
	if ok {
		t.Fatalf("expected villain to be deleted")
	}

	expectedIndex := s.GetIndex()
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close initial store: %v", err)
	}

	// 3. Simulate Crash & Reboot
	s2, err := NewStore(tmpFile)
	if err != nil {
		t.Fatalf("failed to recover store: %v", err)
	}
	defer func() {
		err := s2.Close()
		if err != nil {
			t.Fatalf("failed to close recovered store: %v", err)
		}
	}()

	// 4. Verify state was perfectly restored
	val2, ok := s2.Get("hero")
	if !ok || string(val2) != "batman" {
		t.Fatalf("recovery failed, expected batman, got %v", string(val2))
	}

	_, ok = s2.Get("villain")
	if ok {
		t.Fatalf("recovery failed, expected villain to be deleted")
	}

	if s2.GetIndex() != expectedIndex {
		t.Fatalf("index mismatch after recovery. Expected %d, got %d", expectedIndex, s2.GetIndex())
	}
}
