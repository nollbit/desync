package desync

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
)

// MultiStoreWithCache is used to parse store and cache locations given in the
// command line.
// cacheLocation - Place of the local store used for caching, can be blank
// storeLocation - URLs or paths to remote or local stores that should be queried in order
func MultiStoreWithCache(cmdOpt CmdStoreOptions, cacheLocation string, storeLocations ...string) (Store, error) {
	// Combine all stores into one router
	store, err := MultiStoreWithRouter(cmdOpt, storeLocations...)
	if err != nil {
		return nil, err
	}

	// See if we want to use a writable store as cache, if so, attach a cache to
	// the router
	if cacheLocation != "" {
		cache, err := WritableStore(cacheLocation, cmdOpt)
		if err != nil {
			return store, err
		}

		if ls, ok := cache.(LocalStore); ok {
			ls.UpdateTimes = true
		}
		store = NewCache(store, cache)
	}
	return store, nil
}

// MultiStoreWithRouter is used to parse store locations, and return a store
// router instance containing them all for reading, in the order they're given
func MultiStoreWithRouter(cmdOpt CmdStoreOptions, storeLocations ...string) (Store, error) {
	var stores []Store
	for _, location := range storeLocations {
		s, err := storeGroup(location, cmdOpt)
		if err != nil {
			return nil, err
		}
		stores = append(stores, s)
	}

	return NewStoreRouter(stores...), nil
}

// storeGroup parses a store-location string and if it finds a "|" in the string initializes
// each store in the group individually before wrapping them into a FailoverGroup. If there's
// no "|" in the string, this is a nop.
func storeGroup(location string, cmdOpt CmdStoreOptions) (Store, error) {
	if !strings.ContainsAny(location, "|") {
		return StoreFromLocation(location, cmdOpt)
	}
	var stores []Store
	members := strings.Split(location, "|")
	for _, m := range members {
		s, err := StoreFromLocation(m, cmdOpt)
		if err != nil {
			return nil, err
		}
		stores = append(stores, s)
	}
	return NewFailoverGroup(stores...), nil
}

// WritableStore is used to parse a store location from the command line for
// commands that expect to write chunks, such as make or tar. It determines
// which type of writable store is needed, instantiates and returns a
// single WriteStore.
func WritableStore(location string, cmdOpt CmdStoreOptions) (WriteStore, error) {
	s, err := StoreFromLocation(location, cmdOpt)
	if err != nil {
		return nil, err
	}
	store, ok := s.(WriteStore)
	if !ok {
		return nil, fmt.Errorf("store '%s' does not support writing", location)
	}
	return store, nil
}

// Parse a single store URL or path and return an initialized instance of it
func StoreFromLocation(location string, cmdOpt CmdStoreOptions) (Store, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse store location %s : %s", location, err)
	}

	// Get any store options from the config if present and overwrite with settings from
	// the command line
	opt := cmdOpt.MergedWith(Cfg.GetStoreOptionsFor(location))

	var s Store
	switch loc.Scheme {
	case "ssh":
		s, err = NewRemoteSSHStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "sftp":
		s, err = NewSFTPStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "http", "https":
		// This is for backwards compatibility only, to support http-timeout and
		// http-error-retry in the config file for a bit longer. If those are
		// set, and the options for the store aren't, then use the old values.
		// TODO: Remove this code and drop these config options in the future.
		if opt.Timeout == 0 && Cfg.HTTPTimeout > 0 {
			opt.Timeout = Cfg.HTTPTimeout
		}
		if opt.ErrorRetry == 0 && Cfg.HTTPErrorRetry > 0 {
			opt.ErrorRetry = Cfg.HTTPErrorRetry
		}
		s, err = NewRemoteHTTPStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "s3+http", "s3+https":
		s3Creds, region := Cfg.GetS3CredentialsFor(loc)
		lookup := minio.BucketLookupAuto
		ls := loc.Query().Get("lookup")
		switch ls {
		case "dns":
			lookup = minio.BucketLookupDNS
		case "path":
			lookup = minio.BucketLookupPath
		case "", "auto":
		default:
			return nil, fmt.Errorf("unknown S3 bucket lookup type: %q", s)
		}
		s, err = NewS3Store(loc, s3Creds, region, opt, lookup)
		if err != nil {
			return nil, err
		}
	case "gs":
		s, err = NewGCSStore(loc, opt)
		if err != nil {
			return nil, err
		}
	default:
		s, err = NewLocalStore(location, opt)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func ReadCaibxFile(location string, cmdOpt CmdStoreOptions) (c Index, err error) {
	is, indexName, err := IndexStoreFromLocation(location, cmdOpt)
	if err != nil {
		return c, err
	}
	defer is.Close()
	idx, err := is.GetIndex(indexName)
	return idx, errors.Wrap(err, location)
}

func StoreCaibxFile(idx Index, location string, cmdOpt CmdStoreOptions) error {
	is, indexName, err := WritableIndexStore(location, cmdOpt)
	if err != nil {
		return err
	}
	defer is.Close()
	return is.StoreIndex(indexName, idx)
}

// WritableIndexStore is used to parse a store location from the command line for
// commands that expect to write indexes, such as make or tar. It determines
// which type of writable store is needed, instantiates and returns a
// single IndexWriteStore.
func WritableIndexStore(location string, cmdOpt CmdStoreOptions) (IndexWriteStore, string, error) {
	s, indexName, err := IndexStoreFromLocation(location, cmdOpt)
	if err != nil {
		return nil, indexName, err
	}
	store, ok := s.(IndexWriteStore)
	if !ok {
		return nil, indexName, fmt.Errorf("index store '%s' does not support writing", location)
	}
	return store, indexName, nil
}

// Parse a single store URL or path and return an initialized instance of it
func IndexStoreFromLocation(location string, cmdOpt CmdStoreOptions) (IndexStore, string, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return nil, "", fmt.Errorf("Unable to parse store location %s : %s", location, err)
	}

	indexName := path.Base(loc.Path)
	// Remove file name from url path
	p := *loc
	p.Path = path.Dir(p.Path)

	// Get any store options from the config if present and overwrite with settings from
	// the command line. To do that it's necessary to get the base string so it can be looked
	// up in the config. We could be dealing with Unix-style paths or URLs that use / or with
	// Windows paths that could be using \.
	var base string
	switch {
	case strings.Contains(location, "/"):
		base = location[:strings.LastIndex(location, "/")]
	case strings.Contains(location, "\\"):
		base = location[:strings.LastIndex(location, "\\")]
	}
	opt := cmdOpt.MergedWith(Cfg.GetStoreOptionsFor(base))

	var s IndexStore
	switch loc.Scheme {
	case "ssh":
		return nil, "", errors.New("Index storage is not supported by ssh remote stores")
	case "sftp":
		s, err = NewSFTPIndexStore(&p, opt)
		if err != nil {
			return nil, "", err
		}
	case "http", "https":
		// This is for backwards compatibility only, to support http-timeout and
		// http-error-retry in the config file for a bit longer. If those are
		// set, and the options for the store aren't, then use the old values.
		// TODO: Remove this code and drop these config options in the future.
		if opt.Timeout == 0 && Cfg.HTTPTimeout > 0 {
			opt.Timeout = Cfg.HTTPTimeout
		}
		if opt.ErrorRetry == 0 && Cfg.HTTPErrorRetry > 0 {
			opt.ErrorRetry = Cfg.HTTPErrorRetry
		}
		s, err = NewRemoteHTTPIndexStore(&p, opt)
		if err != nil {
			return nil, "", err
		}
	case "s3+http", "s3+https":
		s3Creds, region := Cfg.GetS3CredentialsFor(&p)
		lookup := minio.BucketLookupAuto
		ls := loc.Query().Get("lookup")
		switch ls {
		case "dns":
			lookup = minio.BucketLookupDNS
		case "path":
			lookup = minio.BucketLookupPath
		case "", "auto":
		default:
			return nil, "", fmt.Errorf("unknown S3 bucket lookup type: %q", s)
		}
		s, err = NewS3IndexStore(&p, s3Creds, region, opt, lookup)
		if err != nil {
			return nil, "", err
		}
	case "gs":
		s, err = NewGCSIndexStore(&p, opt)
		if err != nil {
			return nil, "", err
		}
	default:
		if location == "-" {
			s, _ = NewConsoleIndexStore()
		} else {
			s, err = NewLocalIndexStore(filepath.Dir(location))
			if err != nil {
				return nil, "", err
			}
			indexName = filepath.Base(location)
		}
	}
	return s, indexName, nil
}

// storeFile defines the structure of a file that can be used to pass in the stores
// not by command line arguments, but a file instead. This allows the configuration
// to be reloaded for long-running processes on-the-fly without restarting the process.
type storeFile struct {
	Stores []string `json:"stores"`
	Cache  string   `json:"cache"`
}

func ReadStoreFile(name string) ([]string, string, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	c := new(storeFile)
	err = json.NewDecoder(f).Decode(&c)
	return c.Stores, c.Cache, err
}
