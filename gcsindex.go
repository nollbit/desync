package desync

import (
	"context"
	"io"

	"path"

	"net/url"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

// GCSIndexStore is a read-write index store with GCS backing
type GCSIndexStore struct {
	GCSStoreBase
}

// NewGCSIndexStore creates an index store with GCS backing. The URL
// should be provided like this: gs://bucket/prefix
// Credentials can be passed using the methods outlined at
// https://cloud.google.com/docs/authentication/production
func NewGCSIndexStore(location *url.URL, opt StoreOptions) (s GCSIndexStore, e error) {
	b, err := NewGCSStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return GCSIndexStore{b}, nil
}

// GetIndexReader returns a reader for an index from an GCS bucket. Fails if the specified index
// file does not exist.
func (s GCSIndexStore) GetIndexReader(name string) (io.ReadCloser, error) {
	// it would be great if we could be passed the ctx of an incoming req here
	ctx := context.Background()

	objHandle := s.bucket.Object(s.prefix + name)

	obj, err := objHandle.NewReader(ctx)

	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}

	return obj, nil
}

// GetIndex returns an Index structure from the store
func (s GCSIndexStore) GetIndex(name string) (i Index, e error) {
	obj, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer obj.Close()
	return IndexFromReader(obj)
}

// StoreIndex writes the index file to the S3 store
func (s GCSIndexStore) StoreIndex(name string, idx Index) error {
	// it would be great if we could be passed the ctx of an incoming req here
	ctx := context.Background()

	contentType := "application/octet-stream"

	objHandle := s.bucket.Object(s.prefix + name)
	w := objHandle.NewWriter(ctx)

	_, err := idx.WriteTo(w)
	if err != nil {
		return errors.Wrap(err, path.Base(s.Location))
	}

	err = w.Close()
	if err != nil {
		return errors.Wrap(err, path.Base(s.Location))
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	return nil
}
