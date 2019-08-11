package desync

import (
	gnutar "archive/tar"
	"io"
)

// TarWriteFS uses a GNU tar archive for tar/untar operations of a catar archive.
type TarWriteFS struct {
	w      *gnutar.Writer
	format gnutar.Format
}

var _ UntarFilesystem = TarWriteFS{}

// NewTarFS initializes a new instance of a GNU tar archive that can be used
// for catar archive tar/untar operations.
func NewTarWriteFS(w io.Writer) TarWriteFS {
	return TarWriteFS{gnutar.NewWriter(w), gnutar.FormatGNU}
}

func (fs TarWriteFS) CreateDir(n NodeDirectory, opts UntarOptions) error {
	hdr := &gnutar.Header{
		Typeflag: gnutar.TypeDir,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Xattrs:   n.Xattrs,
		Format:   fs.format,
	}
	return fs.w.WriteHeader(hdr)
}

func (fs TarWriteFS) CreateFile(n NodeFile, opts UntarOptions) error {
	hdr := &gnutar.Header{
		Typeflag: gnutar.TypeReg,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Size:     int64(n.Size),
		Xattrs:   n.Xattrs,
		Format:   fs.format,
	}
	if err := fs.w.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := io.Copy(fs.w, n.Data)
	return err
}

func (fs TarWriteFS) CreateSymlink(n NodeSymlink, opts UntarOptions) error {
	hdr := &gnutar.Header{
		Typeflag: gnutar.TypeSymlink,
		Linkname: n.Target,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Xattrs:   n.Xattrs,
		Format:   fs.format,
	}
	return fs.w.WriteHeader(hdr)
}

// We're not using os.Filemode here but the low-level system modes where the mode bits
// are in the lower half. Can't use os.ModeCharDevice here.
const modeChar = 0x4000

func (fs TarWriteFS) CreateDevice(n NodeDevice, opts UntarOptions) error {
	var typ byte = gnutar.TypeBlock
	if n.Mode&modeChar != 0 {
		typ = gnutar.TypeChar
	}
	hdr := &gnutar.Header{
		Typeflag: typ,
		Name:     n.Name,
		Uid:      n.UID,
		Gid:      n.GID,
		Mode:     int64(n.Mode),
		ModTime:  n.MTime,
		Xattrs:   n.Xattrs,
		Devmajor: int64(n.Major),
		Devminor: int64(n.Minor),
	}
	return fs.w.WriteHeader(hdr)
}

func (fs TarWriteFS) Close() error {
	return fs.w.Close()
}
