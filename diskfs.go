package desync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	"github.com/pkg/xattr"
)

// DiskFS uses the local filesystem for tar/untar operations.
type DiskFS struct {
	Root string
}

var _ UntarFilesystem = DiskFS{}

// NewDiskFS initializes a new instance of a local filesystem that
// can be used for tar/untar operations.
func NewDiskFS(root string) DiskFS {
	return DiskFS{root}
}

func (fs DiskFS) CreateDir(n NodeDirectory, opts UntarOptions) error {
	dst := filepath.Join(fs.Root, n.Name)

	// Let's see if there is a dir with the same name already
	if info, err := os.Stat(dst); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", dst)
		}
	} else {
		// Stat error'ed out, presumably because the dir doesn't exist. Create it.
		if err := os.Mkdir(dst, 0777); err != nil {
			return err
		}
	}
	// The dir exists now, fix the UID/GID if needed
	if !opts.NoSameOwner {
		if err := os.Chown(dst, n.UID, n.GID); err != nil {
			return err
		}

		if n.Xattrs != nil {
			for key, value := range n.Xattrs {
				if err := xattr.LSet(dst, key, []byte(value)); err != nil {
					return err
				}
			}
		}
	}
	if !opts.NoSamePermissions {
		if err := syscall.Chmod(dst, uint32(n.Mode)); err != nil {
			return err
		}
	}
	return os.Chtimes(dst, n.MTime, n.MTime)
}

func (fs DiskFS) CreateFile(n NodeFile, opts UntarOptions) error {
	dst := filepath.Join(fs.Root, n.Name)

	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(f, n.Data); err != nil {
		return err
	}
	if !opts.NoSameOwner {
		if err = f.Chown(n.UID, n.GID); err != nil {
			return err
		}

		if n.Xattrs != nil {
			for key, value := range n.Xattrs {
				if err := xattr.LSet(dst, key, []byte(value)); err != nil {
					return err
				}
			}
		}
	}
	if !opts.NoSamePermissions {
		if err := syscall.Chmod(dst, uint32(n.Mode)); err != nil {
			return err
		}
	}
	return os.Chtimes(dst, n.MTime, n.MTime)
}

func (fs DiskFS) CreateSymlink(n NodeSymlink, opts UntarOptions) error {
	dst := filepath.Join(fs.Root, n.Name)

	if err := syscall.Unlink(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Symlink(n.Target, dst); err != nil {
		return err
	}
	// TODO: On Linux, the permissions of the link don't matter so we don't
	// set them here. But they do matter somewhat on Mac, so should probably
	// add some Mac-specific logic for that here.
	// fchmodat() with flag AT_SYMLINK_NOFOLLOW
	if !opts.NoSameOwner {
		if err := os.Lchown(dst, n.UID, n.GID); err != nil {
			return err
		}

		if n.Xattrs != nil {
			for key, value := range n.Xattrs {
				if err := xattr.LSet(dst, key, []byte(value)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (fs DiskFS) CreateDevice(n NodeDevice, opts UntarOptions) error {
	dst := filepath.Join(fs.Root, n.Name)

	if err := syscall.Unlink(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syscall.Mknod(dst, uint32(n.Mode)|0666, int(mkdev(n.Major, n.Minor))); err != nil {
		return errors.Wrapf(err, "mknod %s", dst)
	}
	if !opts.NoSameOwner {
		if err := os.Chown(dst, n.UID, n.GID); err != nil {
			return err
		}

		if n.Xattrs != nil {
			for key, value := range n.Xattrs {
				if err := xattr.LSet(dst, key, []byte(value)); err != nil {
					return err
				}
			}
		}
	}
	if !opts.NoSamePermissions {
		if err := syscall.Chmod(dst, uint32(n.Mode)); err != nil {
			return errors.Wrapf(err, "chmod %s", dst)
		}
	}
	return os.Chtimes(dst, n.MTime, n.MTime)
}

func mkdev(major, minor uint64) uint64 {
	dev := (major & 0x00000fff) << 8
	dev |= (major & 0xfffff000) << 32
	dev |= (minor & 0x000000ff) << 0
	dev |= (minor & 0xffffff00) << 12
	return dev
}
