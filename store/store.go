package store

import (
	"fmt"
	"sort"
	"sync"
)

// Store represents our in-memory key-value store, backed by a Write-Ahead Log.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	wal  *WAL

	// index keeps track of the latest operation number we've processed.
	// This will be crucial for replication later, so followers know if they are behind.
	index uint64

	// log is an in-memory copy of every committed LogEntry, in index order.
	// This lets GetEntriesAfter serve sync requests without replaying the WAL from disk.
	log []LogEntry
}

// NewStore initializes a new Store and recovers any past state from the WAL.
func NewStore(walPath string) (*Store, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %v", err)
	}

	store := &Store{
		data: make(map[string][]byte),
		wal:  wal,
	}

	// Crash Recovery: Replay the log immediately upon startup
	entries, err := wal.Replay()
	if err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %v", err)
	}

	for _, entry := range entries {
		// Apply each historical entry to our in-memory map
		if entry.Op == OpPut {
			store.data[entry.Key] = entry.Value
		} else if entry.Op == OpDelete {
			delete(store.data, entry.Key)
		}

		// Update our latest index
		if entry.Index > store.index {
			store.index = entry.Index
		}

		// Rebuild the in-memory log
		store.log = append(store.log, entry)
	}

	return store, nil
}

// Put adds or updates a key in the store.
func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.index++ // Increment index for the new operation

	// 1. Write to Disk FIRST (The "Ahead" in Write-Ahead Log)
	entry := LogEntry{
		Index: s.index,
		Op:    OpPut,
		Key:   key,
		Value: value,
	}

	if err := s.wal.Append(entry); err != nil {
		s.index-- // roll back the increment
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// 2. Only after disk acknowledges it, apply to Memory and in-memory log
	s.data[key] = value
	s.log = append(s.log, entry)
	return nil
}

// Get retrieves a key from the store. Note this only reads from memory!
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock() // RLock allows multiple concurrent readers
	defer s.mu.RUnlock()

	val, exists := s.data[key]
	return val, exists
}

// Delete removes a key from the store.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.index++

	// 1. Write the delete operation to Disk FIRST
	entry := LogEntry{
		Index: s.index,
		Op:    OpDelete,
		Key:   key,
		Value: []byte{},
	}

	if err := s.wal.Append(entry); err != nil {
		s.index-- // roll back the increment
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// 2. Delete from Memory and append to in-memory log
	delete(s.data, key)
	s.log = append(s.log, entry)
	return nil
}

// Close safely shuts down the store and its WAL.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}

// GetIndex returns the current log index of the store.
func (s *Store) GetIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index
}

// PrepareEntry atomically reserves the next log index and returns a LogEntry
// ready for replication. It does NOT write to the WAL or apply to memory.
// Call ApplyEntry after receiving quorum acknowledgement.
func (s *Store) PrepareEntry(op OpType, key string, value []byte) LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index++
	return LogEntry{
		Index: s.index,
		Op:    op,
		Key:   key,
		Value: value,
	}
}

// RollbackIndex decrements the log index by 1. Used to undo a PrepareEntry
// reservation if replication failed and the entry should never be committed.
// Must only be called by the leader when no follower has acked the entry.
func (s *Store) RollbackIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index > 0 {
		s.index--
	}
}

// GetEntriesAfter returns all log entries with an index greater than afterIndex.
// It serves from the in-memory log (O(log n) binary search), so it does not
// hold the WAL mutex or perform any disk I/O. This makes it safe to call
// concurrently with ongoing writes.
func (s *Store) GetEntriesAfter(afterIndex uint64) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.log) == 0 || afterIndex >= s.index {
		return nil, nil
	}

	// Binary search for the first entry with Index > afterIndex
	pos := sort.Search(len(s.log), func(i int) bool {
		return s.log[i].Index > afterIndex
	})

	if pos >= len(s.log) {
		return nil, nil
	}

	// Return a copy to avoid callers mutating our internal slice
	result := make([]LogEntry, len(s.log)-pos)
	copy(result, s.log[pos:])
	return result, nil
}

// ApplyEntry is used by followers to apply a replicated entry from the leader.
// Unlike Put/Delete, it preserves the leader's original index instead of
// generating its own. This keeps WAL indices consistent across the cluster.
func (s *Store) ApplyEntry(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip if we already have this entry
	if entry.Index <= s.index {
		return nil
	}

	// Write to WAL first
	if err := s.wal.Append(entry); err != nil {
		return fmt.Errorf("failed to write replicated entry to WAL: %v", err)
	}

	// Apply to memory and in-memory log
	if entry.Op == OpPut {
		s.data[entry.Key] = entry.Value
	} else if entry.Op == OpDelete {
		delete(s.data, entry.Key)
	}

	s.index = entry.Index
	s.log = append(s.log, entry)
	return nil
}
