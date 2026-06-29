package store

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Store represents our in-memory key-value store, backed by a Write-Ahead Log.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	wal  *WAL

	// nextIndex is the next index to reserve via PrepareEntry.
	// It advances ahead of committedIndex when a write is in flight.
	nextIndex uint64

	// committedIndex is the last index that has been durably written to WAL and
	// applied to memory. This is what GetIndex() returns and what heartbeats use
	// for PrevLogIndex. Crucially, it stays behind nextIndex during a write, so
	// followers never see a "gap" in the committed log.
	committedIndex uint64

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

		// Track both committed and next index (equal on startup, no writes in-flight)
		if entry.Index > store.committedIndex {
			store.committedIndex = entry.Index
		}

		// Rebuild the in-memory log
		store.log = append(store.log, entry)
	}
	store.nextIndex = store.committedIndex

	// P1 Fix #10: Group commit — sync WAL to disk every 10ms instead of on every write.
	// This gives ~10ms durability window while removing per-write fsync latency.
	go store.syncLoop()

	return store, nil
}

// syncLoop calls wal.Sync() every 10ms to flush buffered writes to disk.
// This replaces the O_SYNC flag that was previously fsyncing after every write.
func (s *Store) syncLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		_ = s.wal.Sync()
	}
}

// Put adds or updates a key in the store.
// Used for single-node writes and by tests. Commits immediately (no 2-phase).
func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextIndex++ // Increment index for the new operation

	// 1. Write to Disk FIRST (The "Ahead" in Write-Ahead Log)
	entry := LogEntry{
		Index: s.nextIndex,
		Op:    OpPut,
		Key:   key,
		Value: value,
	}

	if err := s.wal.Append(entry); err != nil {
		s.nextIndex-- // roll back the increment
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// 2. Only after disk acknowledges it, apply to Memory and in-memory log
	s.data[key] = value
	s.log = append(s.log, entry)
	s.committedIndex = s.nextIndex
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
// Used for single-node writes and by tests. Commits immediately (no 2-phase).
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextIndex++

	// 1. Write the delete operation to Disk FIRST
	entry := LogEntry{
		Index: s.nextIndex,
		Op:    OpDelete,
		Key:   key,
		Value: []byte{},
	}

	if err := s.wal.Append(entry); err != nil {
		s.nextIndex-- // roll back the increment
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// 2. Delete from Memory and append to in-memory log
	delete(s.data, key)
	s.log = append(s.log, entry)
	s.committedIndex = s.nextIndex
	return nil
}

// Close safely shuts down the store and its WAL.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.wal.Sync() // flush any pending writes before closing
	return s.wal.Close()
}

// GetIndex returns the last *committed* log index.
// This is used by heartbeats and followers for consistency checks.
// It tracks only entries that have been durably written to WAL, never the
// in-flight reserved index from PrepareEntry.
func (s *Store) GetIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.committedIndex
}

// PrepareEntry atomically reserves the next log index and returns a LogEntry
// ready for replication. It does NOT write to the WAL or apply to memory.
// Call ApplyEntry after receiving quorum acknowledgement.
// The committedIndex is NOT advanced here — it moves only in ApplyEntry.
func (s *Store) PrepareEntry(op OpType, key string, value []byte) LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIndex++
	return LogEntry{
		Index: s.nextIndex,
		Op:    op,
		Key:   key,
		Value: value,
	}
}

// GetEntriesAfter returns all *committed* log entries with an index greater than
// afterIndex. It serves from the in-memory log (O(log n) binary search), so it
// does not hold the WAL mutex or perform any disk I/O.
func (s *Store) GetEntriesAfter(afterIndex uint64) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.log) == 0 || afterIndex >= s.committedIndex {
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

// ApplyEntry is used by both followers (applying replicated entries from the leader)
// and by the leader itself (committing after quorum). It preserves the exact index
// from the entry rather than generating one locally.
//
// Key invariant: ApplyEntry checks entry.Index against committedIndex (not nextIndex),
// so a PrepareEntry reservation at nextIndex=N doesn't cause ApplyEntry({Index:N})
// to be incorrectly skipped.
func (s *Store) ApplyEntry(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip if we already have this entry (idempotent)
	if entry.Index <= s.committedIndex {
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

	s.committedIndex = entry.Index
	// Keep nextIndex >= committedIndex (a follower receiving entries has no in-flight reservations)
	if entry.Index > s.nextIndex {
		s.nextIndex = entry.Index
	}
	s.log = append(s.log, entry)
	return nil
}
