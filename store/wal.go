package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// OpType identifies the type of operation in the log
type OpType byte

const (
	OpPut    OpType = 0
	OpDelete OpType = 1
)

// LogEntry represents a single operation in our system.
type LogEntry struct {
	Index uint64
	Op    OpType
	Key   string
	Value []byte
}

// WAL (Write-Ahead Log) manages our append-only persistence.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// NewWAL opens or creates a WAL file.
func NewWAL(filepath string) (*WAL, error) {
	// The flags here are the secret sauce of a WAL:
	// os.O_APPEND: Any write is forced to the end of the file.
	// os.O_SYNC: When we call Write(), it flushes directly to the physical disk (bypassing the OS cache),
	// guaranteeing that if we return success, the data survives a power outage.
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND|os.O_SYNC, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

// Append writes a new entry to the end of the log in a raw binary format.
func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	/*
		Binary Serialization Nitty-Gritty:
		Instead of JSON (which is slow and bulky), databases write packed binary formats.
		Our row looks like this:
		[Index (8 bytes)][Op (1 byte)][KeyLen (4 bytes)][Key (KeyLen bytes)][ValLen (4 bytes)][Value (ValLen bytes)]
	*/

	// 1. Calculate the exact size we need for this row
	size := 8 + 1 + 4 + len(entry.Key) + 4 + len(entry.Value)
	buf := make([]byte, size)

	// 2. Encode the metadata (Index, Op, KeyLen) using Little Endian byte order
	binary.LittleEndian.PutUint64(buf[0:8], entry.Index)
	buf[8] = byte(entry.Op)
	
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(entry.Key)))
	
	// 3. Copy the actual Key string into the buffer
	offset := 13
	copy(buf[offset:], entry.Key)
	offset += len(entry.Key)
	
	// 4. Encode the Value Length and Value itself
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(entry.Value)))
	offset += 4
	copy(buf[offset:], entry.Value)

	// 5. Append to the disk!
	_, err := w.file.Write(buf)
	return err
}

// Replay reads the entire file sequentially to return all past operations.
func (w *WAL) Replay() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to the very beginning of the file
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var entries []LogEntry
	
	for {
		// Read the fixed-size header first: (Index + OpType + KeyLen) = 8 + 1 + 4 = 13 bytes
		header := make([]byte, 13)
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF {
			break // Reached the end of the file, we successfully recovered everything!
		}
		if err != nil {
			return nil, fmt.Errorf("corrupt wal reading header: %v", err)
		}

		index := binary.LittleEndian.Uint64(header[0:8])
		op := OpType(header[8])
		keyLen := binary.LittleEndian.Uint32(header[9:13])

		// Knowing the KeyLen, we can read exactly that many bytes for the key
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(w.file, keyBuf); err != nil {
			return nil, fmt.Errorf("corrupt wal reading key: %v", err)
		}

		// Read the Value Length (4 bytes)
		valLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(w.file, valLenBuf); err != nil {
			return nil, fmt.Errorf("corrupt wal reading val len: %v", err)
		}
		valLen := binary.LittleEndian.Uint32(valLenBuf)

		// Read the Value itself
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(w.file, valBuf); err != nil {
			return nil, fmt.Errorf("corrupt wal reading value: %v", err)
		}

		entries = append(entries, LogEntry{
			Index: index,
			Op:    op,
			Key:   string(keyBuf),
			Value: valBuf,
		})
	}

	return entries, nil
}

// Close closes the file safely.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
