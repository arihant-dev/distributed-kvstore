package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStore_BasicOperations(t *testing.T) {
	// Use a temporary directory to avoid conflicts
	tmpDir, err := os.MkdirTemp("", "store_test_basic")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test_store.log")
	s, err := NewStore(walPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	// 1. Test Get on missing key
	if _, ok := s.Get("missing"); ok {
		t.Error("expected key 'missing' to not exist")
	}

	// 2. Test Put and Get
	key := "foo"
	val := []byte("bar")
	if err := s.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	gotVal, ok := s.Get(key)
	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}
	if !bytes.Equal(gotVal, val) {
		t.Errorf("expected value %q, got %q", val, gotVal)
	}

	// 3. Test Delete
	if err := s.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, ok := s.Get(key); ok {
		t.Errorf("expected key %q to be deleted", key)
	}
}

func TestStore_WALRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "store_test_recovery")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test_recovery.log")
	s, err := NewStore(walPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Write some keys
	kvs := map[string][]byte{
		"key1": []byte("val1"),
		"key2": []byte("val2"),
		"key3": []byte("val3"),
	}

	for k, v := range kvs {
		if err := s.Put(k, v); err != nil {
			t.Fatalf("failed to Put key %q: %v", k, err)
		}
	}

	// Delete one key
	if err := s.Delete("key2"); err != nil {
		t.Fatalf("failed to Delete key2: %v", err)
	}

	// Save final index
	expectedIndex := s.GetIndex()

	// Close store
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// Recreate store from same WAL file
	s2, err := NewStore(walPath)
	if err != nil {
		t.Fatalf("failed to recreate store: %v", err)
	}
	defer s2.Close()

	// Verify loaded keys
	val1, ok := s2.Get("key1")
	if !ok || !bytes.Equal(val1, []byte("val1")) {
		t.Errorf("expected key1 to be %q, got %q (ok=%t)", "val1", val1, ok)
	}

	if _, ok := s2.Get("key2"); ok {
		t.Error("expected key2 to be deleted in recovered store")
	}

	val3, ok := s2.Get("key3")
	if !ok || !bytes.Equal(val3, []byte("val3")) {
		t.Errorf("expected key3 to be %q, got %q (ok=%t)", "val3", val3, ok)
	}

	if s2.GetIndex() != expectedIndex {
		t.Errorf("expected index %d, got %d", expectedIndex, s2.GetIndex())
	}
}
