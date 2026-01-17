package fuse

import (
	"context"
	"encoding/json"
	"fmt"
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

// MetaDirectoryNode represents the .meta/ directory in a bucket
type MetaDirectoryNode struct {
	fs.Inode
	bucketName string
	prefix     string
}

var _ fs.NodeReaddirer = (*MetaDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*MetaDirectoryNode)(nil)
var _ fs.NodeLookuper = (*MetaDirectoryNode)(nil)

// Readdir lists metadata files for all objects in the directory
func (n *MetaDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
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
		{Name: "_bucket.json", Mode: fuse.S_IFREG},
	}

	seen := make(map[string]bool)
	seen["_bucket.json"] = true

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, MapGCPError(err)
		}

		// Skip prefixes (directories)
		if attrs.Prefix != "" {
			continue
		}

		// Add metadata file for each object
		if attrs.Name != "" {
			objectName := strings.TrimPrefix(attrs.Name, n.prefix)
			if objectName != "" && !strings.Contains(objectName, "/") {
				metaName := objectName + ".json"
				if !seen[metaName] {
					entries = append(entries, fuse.DirEntry{
						Name: metaName,
						Mode: fuse.S_IFREG,
					})
					seen[metaName] = true
				}
			}
		}
	}

	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the .meta directory
func (n *MetaDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a metadata file by name
func (n *MetaDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Handle bucket metadata
	if name == "_bucket.json" {
		stable := fs.StableAttr{
			Mode: fuse.S_IFREG,
		}
		node := &BucketMetaFileNode{
			bucketName: n.bucketName,
		}
		child := n.NewInode(ctx, node, stable)

		// Populate entry attributes so file size is known on first access
		var attrOut fuse.AttrOut
		if errno := node.Getattr(ctx, nil, &attrOut); errno != 0 {
			return nil, errno
		}
		out.Attr = attrOut.Attr

		return child, 0
	}

	// Handle object metadata files
	if strings.HasSuffix(name, ".json") {
		objectName := n.prefix + strings.TrimSuffix(name, ".json")
		stable := fs.StableAttr{
			Mode: fuse.S_IFREG,
		}
		node := &ObjectMetaFileNode{
			bucketName: n.bucketName,
			objectName: objectName,
		}
		child := n.NewInode(ctx, node, stable)

		// Populate entry attributes so file size is known on first access
		var attrOut fuse.AttrOut
		if errno := node.Getattr(ctx, nil, &attrOut); errno != 0 {
			return nil, errno
		}
		out.Attr = attrOut.Attr

		return child, 0
	}

	return nil, syscall.ENOENT
}

// BucketMetaFileNode represents the _bucket.json metadata file
type BucketMetaFileNode struct {
	fs.Inode
	bucketName string
}

var _ fs.NodeOpener = (*BucketMetaFileNode)(nil)
var _ fs.NodeGetattrer = (*BucketMetaFileNode)(nil)
var _ fs.NodeReader = (*BucketMetaFileNode)(nil)

// Open opens the bucket metadata file for reading
func (n *BucketMetaFileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Getattr returns attributes for the bucket metadata file
func (n *BucketMetaFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content, err := n.generateMetadata(ctx)
	if err != nil {
		return MapGCPError(err)
	}

	out.Mode = 0644
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(len(content))
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1

	return 0
}

// Read reads the bucket metadata
func (n *BucketMetaFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	content, err := n.generateMetadata(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	if off >= int64(len(content)) {
		return fuse.ReadResultData(nil), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(content)) {
		end = int64(len(content))
	}

	return fuse.ReadResultData(content[off:end]), 0
}

// generateMetadata generates JSON metadata for the bucket (with caching)
func (n *BucketMetaFileNode) generateMetadata(ctx context.Context) ([]byte, error) {
	cache := GetMetadataCache()

	return cache.GetBucketMetadata(ctx, n.bucketName, func() ([]byte, error) {
		start := time.Now()
		client, err := storagepkg.GetClient(ctx)
		if err != nil {
			return nil, err
		}

		attrs, err := client.Bucket(n.bucketName).Attrs(ctx)
		logGCS("GetBucketAttrs", start, n.bucketName)
		if err != nil {
			return nil, err
		}

		metadata := map[string]interface{}{
			"version":            "1.0",
			"type":               "bucket",
			"name":               attrs.Name,
			"location":           attrs.Location,
			"storage_class":      attrs.StorageClass,
			"created":            attrs.Created.Format(time.RFC3339),
			"versioning_enabled": attrs.VersioningEnabled,
			"labels":             attrs.Labels,
		}

		return json.MarshalIndent(metadata, "", "  ")
	})
}

// ObjectMetaFileNode represents a <name>.json metadata file for an object
type ObjectMetaFileNode struct {
	fs.Inode
	bucketName string
	objectName string
}

var _ fs.NodeOpener = (*ObjectMetaFileNode)(nil)
var _ fs.NodeGetattrer = (*ObjectMetaFileNode)(nil)
var _ fs.NodeReader = (*ObjectMetaFileNode)(nil)

// Open opens the object metadata file for reading
func (n *ObjectMetaFileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Getattr returns attributes for the object metadata file
func (n *ObjectMetaFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content, err := n.generateMetadata(ctx)
	if err != nil {
		return MapGCPError(err)
	}

	out.Mode = 0644
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(len(content))
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1

	return 0
}

// Read reads the object metadata
func (n *ObjectMetaFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	content, err := n.generateMetadata(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	if off >= int64(len(content)) {
		return fuse.ReadResultData(nil), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(content)) {
		end = int64(len(content))
	}

	return fuse.ReadResultData(content[off:end]), 0
}

// generateMetadata generates JSON metadata for the object (with caching)
func (n *ObjectMetaFileNode) generateMetadata(ctx context.Context) ([]byte, error) {
	cache := GetMetadataCache()

	return cache.GetObjectMetadata(ctx, n.bucketName, n.objectName, func() ([]byte, error) {
		start := time.Now()
		client, err := storagepkg.GetClient(ctx)
		if err != nil {
			return nil, err
		}

		attrs, err := client.Bucket(n.bucketName).Object(n.objectName).Attrs(ctx)
		logGCS("GetObjectAttrs", start, n.bucketName, n.objectName)
		if err != nil {
			return nil, err
		}

		metadata := map[string]interface{}{
			"version":        "1.0",
			"type":           "object",
			"bucket":         n.bucketName,
			"name":           attrs.Name,
			"content_type":   attrs.ContentType,
			"size":           attrs.Size,
			"md5":            fmt.Sprintf("%x", attrs.MD5),
			"crc32c":         fmt.Sprintf("%x", attrs.CRC32C),
			"created":        attrs.Created.Format(time.RFC3339),
			"updated":        attrs.Updated.Format(time.RFC3339),
			"generation":     attrs.Generation,
			"metageneration": attrs.Metageneration,
			"storage_class":  attrs.StorageClass,
			"metadata":       attrs.Metadata,
		}

		return json.MarshalIndent(metadata, "", "  ")
	})
}
