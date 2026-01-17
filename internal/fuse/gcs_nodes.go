package fuse

import (
	"context"
	"os"
	"strings"
	"sync"
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
	logGC("ListBuckets", start, projectID, len(buckets), "buckets")

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

// ObjectNode represents a GCS object (file)
type ObjectNode struct {
	fs.Inode
	bucketName   string
	objectName   string
	attrs        *storage.ObjectAttrs
	readAhead    *ReadAheadBuffer
	readAheadMu  sync.Mutex
}

var _ fs.NodeReaddirer = (*BucketNode)(nil)
var _ fs.NodeGetattrer = (*BucketNode)(nil)
var _ fs.NodeLookuper = (*BucketNode)(nil)
var _ fs.NodeSetattrer = (*BucketNode)(nil)

// Readdir lists objects and prefixes in the bucket using concurrent API calls
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

	// Use concurrent listing for better performance
	allAttrs, err := listObjectsConcurrent(ctx, bucket, query)
	if err != nil {
		return nil, MapGCPError(err)
	}

	entries := []fuse.DirEntry{
		{Name: ".meta", Mode: fuse.S_IFDIR},
	}

	seen := make(map[string]bool)
	seen[".meta"] = true

	// Process all results
	for _, attrs := range allAttrs {
		// Handle directory prefixes
		if attrs.Prefix != "" {
			// Extract the directory name from the prefix
			dirName := strings.TrimPrefix(attrs.Prefix, n.prefix)
			dirName = strings.TrimSuffix(dirName, "/")
			// Skip dot directories
			if dirName != "" && !strings.HasPrefix(dirName, ".") && !seen[dirName] {
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
			// Skip dot files (except those already handled like .meta)
			if objectName != "" && !strings.Contains(objectName, "/") && !strings.HasPrefix(objectName, ".") && !seen[objectName] {
				entries = append(entries, fuse.DirEntry{
					Name: objectName,
					Mode: fuse.S_IFREG,
				})
				seen[objectName] = true
			}
		}
	}

	logGC("ListObjects", start, n.bucketName, n.prefix, len(entries)-1, "objects") // -1 for .meta dir
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

// Setattr handles attribute changes (used for cache invalidation via `touch .`)
func (n *BucketNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Detect `touch .` by checking if mtime is being set
	if in.Valid&fuse.FATTR_MTIME != 0 {
		// Invalidate metadata cache for this bucket
		cache := GetMetadataCache()
		cache.InvalidateBucket(n.bucketName)
		logGC("CacheInvalidate", time.Now(), n.bucketName, n.prefix, "cache cleared via touch")
	}

	// Return current attributes (read-only filesystem)
	return n.Getattr(ctx, f, out)
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
		logGC("Lookup", start, n.bucketName, n.prefix+name, "-> .meta dir")
		return child, 0
	}

	// Return ENOENT for all other dot files (like .DS_Store, .config, etc.)
	if strings.HasPrefix(name, ".") {
		logGC("Lookup", start, n.bucketName, n.prefix+name, "-> ENOENT (dot file)")
		return nil, syscall.ENOENT
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
		logGC("Lookup", start, n.bucketName, objectName, "-> object")
		stable := fs.StableAttr{
			Mode: fuse.S_IFREG,
		}
		node := &ObjectNode{
			bucketName: n.bucketName,
			objectName: objectName,
			attrs:      attrs,
		}
		child := n.NewInode(ctx, node, stable)

		// Populate entry attributes so file size is known on first access
		var attrOut fuse.AttrOut
		node.Getattr(ctx, nil, &attrOut)
		out.Attr = attrOut.Attr

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
		logGC("Lookup", start, n.bucketName, prefixPath, "-> prefix")
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

	logGC("Lookup", start, n.bucketName, n.prefix+name, "-> ENOENT")
	return nil, syscall.ENOENT
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

// Read reads data from the object with read-ahead buffering
func (n *ObjectNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	start := time.Now()
	client, err := storagepkg.GetClient(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	// Initialize read-ahead buffer on first read
	n.readAheadMu.Lock()
	if n.readAhead == nil {
		n.readAhead = NewReadAheadBuffer(n.bucketName, n.objectName)
	}
	buffer := n.readAhead
	n.readAheadMu.Unlock()

	// Try to read from buffer with read-ahead
	data, err := buffer.Read(ctx, client.Bucket(n.bucketName), off, dest)
	if err != nil {
		logGC("ReadObject", start, n.bucketName, n.objectName, "offset", off, "requested", len(dest), "ERROR", err)
		return nil, MapGCPError(err)
	}

	logGC("ReadObject", start, n.bucketName, n.objectName, "offset", off, "requested", len(dest), "read", len(data), "bytes")
	return fuse.ReadResultData(data), 0
}
