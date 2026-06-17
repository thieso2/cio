package fuse

import (
	"os"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// This file is the single home for FUSE node attribute defaults. Every
// directory node returned the same Getattr (0755, the mounting user's
// uid/gid, Nlink=2), and every generated file node returned the same shape
// (mode, content size, mtime=now, Nlink=1). Those two blocks were copy-pasted
// across ~30 Getattr methods; now each method is one helper call, so the
// permission/ownership policy lives in one place.

// fillDirAttr writes the standard directory attributes into out.
func fillDirAttr(out *fuse.AttrOut) {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
}

// fillFileAttr writes the standard regular-file attributes for content of the
// given size, using mode (e.g. 0644 or 0444|fuse.S_IFREG) and mtime=now.
func fillFileAttr(out *fuse.AttrOut, mode uint32, size int) {
	out.Mode = mode
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(size)
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1
}
