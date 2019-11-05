package desync

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/folbricht/tempfile"
)

var _ WriteStore = LocalStore{}

// LocalStore casync store
type LocalStore struct {
	Base string

	// When accessing chunks, should mtime be updated? Useful when this is
	// a cache. Old chunks can be identified and removed from the store that way
	UpdateTimes bool

	opt StoreOptions
}

// NewLocalStore creates an instance of a local castore, it only checks presence
// of the store
func NewLocalStore(dir string, opt StoreOptions) (LocalStore, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return LocalStore{}, err
	}
	if !info.IsDir() {
		return LocalStore{}, fmt.Errorf("%s is not a directory", dir)
	}
	return LocalStore{Base: dir, opt: opt}, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s LocalStore) GetChunk(id ChunkID) (*Chunk, error) {
	_, p := s.nameFromID(id)
	b, err := ioutil.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, ChunkMissing{id}
	}
	if s.opt.Uncompressed {
		return NewChunkWithID(id, b, nil, s.opt.SkipVerify)
	}
	return NewChunkWithID(id, nil, b, s.opt.SkipVerify)
}

func (s LocalStore) getChunkForceValidate(id ChunkID) (*Chunk, error) {
	chunk, err := s.GetChunk(id)
	if err != nil {
		return chunk, err
	}
	if s.opt.SkipVerify {
		chunk.idCalculated = false
		sum := chunk.ID()
		if sum != id {
			return nil, ChunkInvalid{ID: id, Sum: sum}
		}
	}
	return chunk, err
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s LocalStore) RemoveChunk(id ChunkID) error {
	_, p := s.nameFromID(id)
	if _, err := os.Stat(p); err != nil {
		return ChunkMissing{id}
	}
	return os.Remove(p)
}

// StoreChunk adds a new chunk to the store
func (s LocalStore) StoreChunk(chunk *Chunk) error {
	d, p := s.nameFromID(chunk.ID())
	var (
		b   []byte
		err error
	)
	_, err = os.Stat(p)
	if os.IsExist(err) {
		// Someone beat us to it, no more work to be done
		return nil
	}

	if s.opt.Uncompressed {
		b, err = chunk.Uncompressed()
	} else {
		b, err = chunk.Compressed()
	}
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d, 0755); err != nil {
		return err
	}
	tmp, err := tempfile.NewSuffixAndMode(d, "cacnk", ".tmp", 0644)
	if err != nil {
		return err
	}
	if _, err = tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmp.Name()) // clean up
		return err
	}
	tmp.Close() // Windows can't rename open files, close explicitly

	// Windows might be blocked by virus scanners etc preventing file rename
	// Also make sure if the file already exists, use it if valid or remove and retry if invalid
	err = os.Rename(tmp.Name(), p)
	retriesLeft := 10
	for err != nil {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			fmt.Printf("Failed to rename file from `%s` to `%s`, error: %s\n", tmp.Name(), p, err)
		} else {
			// File already exists, validate if it is correct
			_, err := s.getChunkForceValidate(chunk.ID())
			switch err.(type) {
			case ChunkInvalid: // bad chunk, remove it so we can try again
				if err = os.Remove(p); err != nil {
					if os.IsNotExist(err) {
						// Someone else removed the invalid chunk
						break
					}
					fmt.Printf("Failed to remove invalid chunk `%s`, error %s\n", p, err)
				}
			case nil:
				if err = os.Remove(tmp.Name()); err != nil {
					fmt.Printf("Valid chunk already exists, failed to remove temp file `%s`, error: %s\n", tmp.Name(), err)
				} else {
					// All good, we have cleaned up after ourselves
					return nil
				}
			default: // unexpected
				return err
			}
		}
		if retriesLeft == 0 {
			return err
		}
		// If the chunk file or our tmp file is locked by anti-virus or some other process wait a little before retrying
		time.Sleep(200 * time.Millisecond)
		err = os.Rename(tmp.Name(), p)
		retriesLeft--
	}
	return err
}

// Verify all chunks in the store. If repair is set true, bad chunks are deleted.
// n determines the number of concurrent operations. w is used to write any messages
// intended for the user, typically os.Stderr.
func (s LocalStore) Verify(ctx context.Context, n int, repair bool, w io.Writer) error {
	var wg sync.WaitGroup
	ids := make(chan ChunkID)

	// Start the workers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for id := range ids {
				_, err := s.GetChunk(id)
				switch err.(type) {
				case ChunkInvalid: // bad chunk, report and delete (if repair=true)
					msg := err.Error()
					if repair {
						if err = s.RemoveChunk(id); err != nil {
							msg = msg + ":" + err.Error()
						} else {
							msg = msg + ": removed"
						}
					}
					fmt.Fprintln(w, msg)
				case nil:
				default: // unexpected, print the error and carry on
					fmt.Fprintln(w, err)
				}
			}
			wg.Done()
		}()
	}

	// Go trough all chunks underneath Base, filtering out other files, then feed
	// the IDs to the workers
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil { // failed to walk? => fail
			return err
		}
		if info.IsDir() { // Skip dirs
			return nil
		}
		// Skip compressed chunks if this is running in uncompressed mode and vice-versa
		var sID string
		if s.opt.Uncompressed {
			if !strings.HasSuffix(path, UncompressedChunkExt) {
				return nil
			}
			sID = strings.TrimSuffix(filepath.Base(path), UncompressedChunkExt)
		} else {
			if !strings.HasSuffix(path, CompressedChunkExt) {
				return nil
			}
			sID = strings.TrimSuffix(filepath.Base(path), CompressedChunkExt)
		}
		// Convert the name into a checksum, if that fails we're probably not looking
		// at a chunk file and should skip it.
		id, err := ChunkIDFromString(sID)
		if err != nil {
			return nil
		}
		// Feed the workers
		ids <- id
		return nil
	})
	close(ids)
	wg.Wait()
	return err
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s LocalStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	// Go trough all chunks underneath Base, filtering out other directories and files
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil { // failed to walk? => fail
			return err
		}
		if info.IsDir() { // Skip dirs
			return nil
		}
		// Skip compressed chunks if this is running in uncompressed mode and vice-versa
		var sID string
		if s.opt.Uncompressed {
			if !strings.HasSuffix(path, UncompressedChunkExt) {
				return nil
			}
			sID = strings.TrimSuffix(filepath.Base(path), UncompressedChunkExt)
		} else {
			if !strings.HasSuffix(path, CompressedChunkExt) {
				return nil
			}
			sID = strings.TrimSuffix(filepath.Base(path), CompressedChunkExt)
		}
		// Convert the name into a checksum, if that fails we're probably not looking
		// at a chunk file and should skip it.
		id, err := ChunkIDFromString(sID)
		if err != nil {
			return nil
		}
		// See if the chunk we're looking at is in the list we want to keep, if not
		// remove it.
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// HasChunk returns true if the chunk is in the store
func (s LocalStore) HasChunk(id ChunkID) (bool, error) {
	_, p := s.nameFromID(id)
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s LocalStore) String() string {
	return s.Base
}

// Close the store. NOP opertation, needed to implement Store interface.
func (s LocalStore) Close() error { return nil }

func (s LocalStore) nameFromID(id ChunkID) (dir, name string) {
	sID := id.String()
	dir = filepath.Join(s.Base, sID[0:4])
	name = filepath.Join(dir, sID)
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return
}
