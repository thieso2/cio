package fuse

import (
	"context"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	storagepkg "github.com/thieso2/cio/internal/storage"
	"google.golang.org/api/iterator"
)

// listGCSBuckets lists all buckets in a GCP project
func listGCSBuckets(ctx context.Context, projectID string) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	buckets, err := storagepkg.ListBuckets(ctx, projectID)
	logGCS("ListBuckets", start, projectID, len(buckets), "buckets")

	if err != nil {
		return nil, MapGCPError(err)
	}

	entries := make([]fuse.DirEntry, 0, len(buckets))
	for _, bucket := range buckets {
		entries = append(entries, fuse.DirEntry{
			Name: bucket.Name,
			Mode: fuse.S_IFDIR,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// BucketNode represents a GCS bucket directory
type BucketNode struct {
	fs.Inode
	projectID  string
	bucketName string
	prefix     string // For subdirectories within a bucket
}

var _ fs.NodeReaddirer = (*BucketNode)(nil)
var _ fs.NodeGetattrer = (*BucketNode)(nil)
var _ fs.NodeLookuper = (*BucketNode)(nil)

// Readdir lists objects and prefixes in the bucket
func (n *BucketNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	client, err := storagepkg.GetClient(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	bucket := client.Bucket(n.bucketName)
	query := &storage.Query{
		Prefix:    n.prefix,
		Delimiter: "/",
	}

	it := bucket.Objects(ctx, query)

	entries := []fuse.DirEntry{
		{Name: ".meta", Mode: fuse.S_IFDIR},
	}

	seen := make(map[string]bool)
	seen[".meta"] = true

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, MapGCPError(err)
		}

		// Handle directory prefixes
		if attrs.Prefix != "" {
			// Extract the directory name from the prefix
			dirName := strings.TrimPrefix(attrs.Prefix, n.prefix)
			dirName = strings.TrimSuffix(dirName, "/")
			if dirName != "" && !seen[dirName] {
				entries = append(entries, fuse.DirEntry{
					Name: dirName,
					Mode: fuse.S_IFDIR,
				})
				seen[dirName] = true
			}
			continue
		}

		// Handle objects (files)
		if attrs.Name != "" {
			// Extract just the filename from the full object name
			objectName := strings.TrimPrefix(attrs.Name, n.prefix)
			if objectName != "" && !strings.Contains(objectName, "/") && !seen[objectName] {
				entries = append(entries, fuse.DirEntry{
					Name: objectName,
					Mode: fuse.S_IFREG,
				})
				seen[objectName] = true
			}
		}
	}

	logGCS("ListObjects", start, n.bucketName, n.prefix, len(entries)-1, "objects") // -1 for .meta dir
	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the bucket directory
func (n *BucketNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a child node by name (object or prefix)
func (n *BucketNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	start := time.Now()

	// Handle .meta directory
	if name == ".meta" {
		stable := fs.StableAttr{
			Mode: fuse.S_IFDIR,
		}
		child := n.NewInode(ctx, &MetaDirectoryNode{
			bucketName: n.bucketName,
			prefix:     n.prefix,
		}, stable)
		logGCS("Lookup", start, n.bucketName, n.prefix+name, "-> .meta dir")
		return child, 0
	}

	client, err := storagepkg.GetClient(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	// Check if it's an object (file)
	objectName := n.prefix + name
	bucket := client.Bucket(n.bucketName)
	attrs, err := bucket.Object(objectName).Attrs(ctx)
	if err == nil {
		// It's a file
		logGCS("Lookup", start, n.bucketName, objectName, "-> object")
		stable := fs.StableAttr{
			Mode: fuse.S_IFREG,
		}
		child := n.NewInode(ctx, &ObjectNode{
			bucketName: n.bucketName,
			objectName: objectName,
			attrs:      attrs,
		}, stable)
		return child, 0
	}

	// Check if it's a prefix (directory)
	prefixPath := n.prefix + name + "/"
	query := &storage.Query{
		Prefix: prefixPath,
	}
	it := bucket.Objects(ctx, query)
	_, err = it.Next()
	if err != iterator.Done {
		// It's a directory (has contents)
		logGCS("Lookup", start, n.bucketName, prefixPath, "-> prefix")
		stable := fs.StableAttr{
			Mode: fuse.S_IFDIR,
		}
		child := n.NewInode(ctx, &BucketNode{
			projectID:  n.projectID,
			bucketName: n.bucketName,
			prefix:     prefixPath,
		}, stable)
		return child, 0
	}

	logGCS("Lookup", start, n.bucketName, n.prefix+name, "-> ENOENT")
	return nil, syscall.ENOENT
}

// ObjectNode represents a GCS object (file)
type ObjectNode struct {
	fs.Inode
	bucketName string
	objectName string
	attrs      *storage.ObjectAttrs
}

var _ fs.NodeOpener = (*ObjectNode)(nil)
var _ fs.NodeGetattrer = (*ObjectNode)(nil)
var _ fs.NodeReader = (*ObjectNode)(nil)

// Open opens the object for reading
func (n *ObjectNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Read-only access
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}

	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Getattr returns attributes for the object
func (n *ObjectNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Refresh attrs if not set
	if n.attrs == nil {
		client, err := storagepkg.GetClient(ctx)
		if err != nil {
			return MapGCPError(err)
		}
		attrs, err := client.Bucket(n.bucketName).Object(n.objectName).Attrs(ctx)
		if err != nil {
			return MapGCPError(err)
		}
		n.attrs = attrs
	}

	out.Mode = 0644
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(n.attrs.Size)
	out.Mtime = uint64(n.attrs.Updated.Unix())
	out.Mtimensec = uint32(n.attrs.Updated.Nanosecond())
	out.Atime = out.Mtime
	out.Atimensec = out.Mtimensec
	out.Ctime = uint64(n.attrs.Created.Unix())
	out.Ctimensec = uint32(n.attrs.Created.Nanosecond())
	out.Nlink = 1

	return 0
}

// Read reads data from the object
func (n *ObjectNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	start := time.Now()
	client, err := storagepkg.GetClient(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	reader, err := client.Bucket(n.bucketName).Object(n.objectName).NewRangeReader(ctx, off, int64(len(dest)))
	if err != nil {
		return nil, MapGCPError(err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil && err != io.EOF {
		return nil, MapGCPError(err)
	}

	logGCS("ReadObject", start, n.bucketName, n.objectName, "offset", off, "requested", len(dest), "read", len(data), "bytes")
	return fuse.ReadResultData(data), 0
}
