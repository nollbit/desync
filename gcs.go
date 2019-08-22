package desync

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

var _ WriteStore = GCSStore{}

// GCSStoreBase is the base object for all chunk and index stores with GCS backing
type GCSStoreBase struct {
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle
	prefix   string
	opt      StoreOptions
}

// GCSStore is a read-write store with GCS backing
type GCSStore struct {
	GCSStoreBase
}

// NewGCSStoreBase initializes a base object used for chunk or index stores backed by GCS.
func NewGCSStoreBase(u *url.URL, opt StoreOptions) (GCSStoreBase, error) {
	var err error
	s := GCSStoreBase{Location: u.String(), opt: opt}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	s.prefix = u.Path[1:] // strip initial slash

	if s.prefix != "" {
		s.prefix += "/"
	}

	ctx := context.Background()
	s.client, err = storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	s.bucket = s.client.Bucket(bucketName)

	return s, nil
}

func (s GCSStoreBase) String() string {
	return s.Location
}

func (s GCSStoreBase) putObjectBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	objHandle := s.bucket.Object(key)
	objWriter := objHandle.NewWriter(ctx)

	_, err := objWriter.Write(blob)
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	err = objWriter.Close()
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	return nil
}

// Close the GCS base store. NOP opertation but needed to implement the store interface.
func (s GCSStoreBase) Close() error { return nil }

// NewGCSStore creates a chunk store with Google Cloud Storage backing. The URL
// should be provided like this: gs://bucket/prefix
// Credentials can be passed using the methods outlined at
// https://cloud.google.com/docs/authentication/production
func NewGCSStore(location *url.URL, opt StoreOptions) (s GCSStore, e error) {
	b, err := NewGCSStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return GCSStore{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s GCSStore) GetChunk(id ChunkID) (*Chunk, error) {
	// it would be great if we could be passed the ctx of an incoming req here
	ctx := context.Background()

	name := s.nameFromID(id)

	objHandle := s.bucket.Object(name)

	obj, err := objHandle.NewReader(ctx)

	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, err
	}
	if s.opt.Uncompressed {
		return NewChunkWithID(id, b, nil, s.opt.SkipVerify)
	}
	return NewChunkWithID(id, nil, b, s.opt.SkipVerify)
}

// StoreChunk adds a new chunk to the store
func (s GCSStore) StoreChunk(chunk *Chunk) error {
	// it would be great if we could be passed the ctx of an incoming req here
	ctx := context.Background()

	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())

	var (
		b   []byte
		err error
	)
	if s.opt.Uncompressed {
		b, err = chunk.Uncompressed()
	} else {
		b, err = chunk.Compressed()
	}
	if err != nil {
		return err
	}

	objHandle := s.bucket.Object(name)
	objWriter := objHandle.NewWriter(ctx)

	_, err = objWriter.Write(b)
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	err = objWriter.Close()
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	return nil
}

// HasChunk returns true if the chunk is in the store
func (s GCSStore) HasChunk(id ChunkID) (bool, error) {
	// it would be great if we could be passed the ctx of an incoming req here
	ctx := context.Background()

	name := s.nameFromID(id)
	_, err := s.bucket.Object(name).Attrs(ctx)
	return err == nil, nil
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s GCSStore) RemoveChunk(id ChunkID) error {
	// it would be great if we could be passed the ctx of an incoming req here
	ctx := context.Background()

	name := s.nameFromID(id)
	return s.bucket.Object(name).Delete(ctx)
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s GCSStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {

	it := s.bucket.Objects(ctx, nil)

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return err
		}

		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}

		id, err := s.idFromName(objAttrs.Name)
		if err != nil {
			continue
		}

		// Drop the chunk if it's not on the list
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s GCSStore) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}

func (s GCSStore) idFromName(name string) (ChunkID, error) {
	var n string
	if s.opt.Uncompressed {
		if !strings.HasSuffix(name, UncompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), UncompressedChunkExt)
	} else {
		if !strings.HasSuffix(name, CompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), CompressedChunkExt)
	}
	fragments := strings.Split(n, "/")
	if len(fragments) != 2 {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	idx := fragments[0]
	sid := fragments[1]
	if !strings.HasPrefix(sid, idx) {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	return ChunkIDFromString(sid)
}
