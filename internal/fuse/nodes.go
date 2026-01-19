package fuse

import (
	"context"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// RootNode represents the root directory of the FUSE filesystem (e.g., /mnt/gcp/)
// It contains service directories (storage, bigquery, pubsub) directly.
type RootNode struct {
	fs.Inode
	projectID string
}

var _ fs.NodeReaddirer = (*RootNode)(nil)
var _ fs.NodeGetattrer = (*RootNode)(nil)
var _ fs.NodeLookuper = (*RootNode)(nil)

// Readdir lists the service directories under the root
func (n *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "storage", Mode: fuse.S_IFDIR},
		{Name: "bigquery", Mode: fuse.S_IFDIR},
		{Name: "pubsub", Mode: fuse.S_IFDIR},
	}
	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the root directory
func (n *RootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a child node by name (service directory)
func (n *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Only allow known service names
	if name != "storage" && name != "bigquery" && name != "pubsub" {
		return nil, syscall.ENOENT
	}

	// Create or retrieve the service node
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
	}
	child := n.NewInode(ctx, &ServiceNode{
		projectID:   n.projectID,
		serviceName: name,
	}, stable)
	return child, 0
}

// ServiceNode represents a GCP service directory (e.g., /mnt/gcp/storage/)
// Phase 0: Returns empty directory listing
// Phase 1+: Will be updated to list actual resources
type ServiceNode struct {
	fs.Inode
	projectID   string
	serviceName string
}

var _ fs.NodeReaddirer = (*ServiceNode)(nil)
var _ fs.NodeGetattrer = (*ServiceNode)(nil)
var _ fs.NodeLookuper = (*ServiceNode)(nil)

// Readdir lists resources under the service
// For storage service, lists all buckets
// For bigquery service, lists all datasets
// For other services, returns empty list
func (n *ServiceNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.serviceName == "storage" {
		// Delegate to GCS bucket listing
		return listGCSBuckets(ctx, n.projectID)
	}

	if n.serviceName == "bigquery" {
		// Delegate to BigQuery dataset listing
		return listBQDatasets(ctx, n.projectID)
	}

	// For other services, return empty
	entries := []fuse.DirEntry{}
	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the service directory
func (n *ServiceNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a child node by name
// For storage service, looks up bucket by name
// For bigquery service, looks up dataset by name
// For other services, returns ENOENT
func (n *ServiceNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Ignore files starting with "." (like .DS_Store)
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	if n.serviceName == "storage" {
		// Create a BucketNode for the requested bucket
		stable := fs.StableAttr{
			Mode: fuse.S_IFDIR,
		}
		child := n.NewInode(ctx, &BucketNode{
			projectID:  n.projectID,
			bucketName: name,
		}, stable)
		return child, 0
	}

	if n.serviceName == "bigquery" {
		// Create a DatasetNode for the requested dataset
		stable := fs.StableAttr{
			Mode: fuse.S_IFDIR,
		}
		child := n.NewInode(ctx, &DatasetNode{
			projectID: n.projectID,
			datasetID: name,
		}, stable)
		return child, 0
	}

	// For other services, not implemented yet
	return nil, syscall.ENOENT
}
