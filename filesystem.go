package desync

import "os"

func isSymlink(m os.FileMode) bool {
	return m&os.ModeSymlink != 0
}

func isDevice(m os.FileMode) bool {
	return m&os.ModeDevice != 0
}

// UntarFilesystem is a filesystem implementation that supports untar'ing
// a catar archive to.
type UntarFilesystem interface {
	CreateDir(n NodeDirectory, opts UntarOptions) error
	CreateFile(n NodeFile, opts UntarOptions) error
	CreateSymlink(n NodeSymlink, opts UntarOptions) error
	CreateDevice(n NodeDevice, opts UntarOptions) error
}
