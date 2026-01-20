package fuse

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// =============================================================================
// GCS IAM Policy Nodes
// =============================================================================

// GCSIAMPolicyDirectoryNode represents .meta/iam-policy/ for a GCS bucket
type GCSIAMPolicyDirectoryNode struct {
	fs.Inode
	bucketName string
}

var _ fs.NodeReaddirer = (*GCSIAMPolicyDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMPolicyDirectoryNode)(nil)
var _ fs.NodeLookuper = (*GCSIAMPolicyDirectoryNode)(nil)

func (n *GCSIAMPolicyDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "bindings.json", Mode: fuse.S_IFREG},
		{Name: "by-role", Mode: fuse.S_IFDIR},
		{Name: "by-member", Mode: fuse.S_IFDIR},
	}
	return fs.NewListDirStream(entries), 0
}

func (n *GCSIAMPolicyDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *GCSIAMPolicyDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "bindings.json":
		stable := fs.StableAttr{Mode: fuse.S_IFREG}
		child := n.NewInode(ctx, &GCSIAMPolicyFileNode{bucketName: n.bucketName}, stable)

		// Populate attributes
		var attrOut fuse.AttrOut
		if errno := child.Operations().(fs.NodeGetattrer).Getattr(ctx, nil, &attrOut); errno == 0 {
			out.Attr = attrOut.Attr
		}
		return child, 0

	case "by-role":
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &GCSIAMByRoleDirectoryNode{bucketName: n.bucketName}, stable)
		return child, 0

	case "by-member":
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &GCSIAMByMemberDirectoryNode{bucketName: n.bucketName}, stable)
		return child, 0
	}

	return nil, syscall.ENOENT
}

// GCSIAMPolicyFileNode represents .meta/iam-policy/bindings.json
type GCSIAMPolicyFileNode struct {
	fs.Inode
	bucketName string
	bufferMu   sync.Mutex
	buffer     []byte
	bufValid   bool
}

var _ fs.NodeOpener = (*GCSIAMPolicyFileNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMPolicyFileNode)(nil)
var _ fs.NodeReader = (*GCSIAMPolicyFileNode)(nil)

func (n *GCSIAMPolicyFileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *GCSIAMPolicyFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content, err := n.generateContent(ctx)
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

func (n *GCSIAMPolicyFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n.bufferMu.Lock()
	defer n.bufferMu.Unlock()

	if !n.bufValid {
		content, err := n.generateContent(ctx)
		if err != nil {
			return nil, MapGCPError(err)
		}
		n.buffer = content
		n.bufValid = true
	}

	if off >= int64(len(n.buffer)) {
		return fuse.ReadResultData(nil), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(n.buffer)) {
		end = int64(len(n.buffer))
	}

	return fuse.ReadResultData(n.buffer[off:end]), 0
}

func (n *GCSIAMPolicyFileNode) generateContent(ctx context.Context) ([]byte, error) {
	cache := GetMetadataCache()

	return cache.GetBucketIAMPolicy(ctx, n.bucketName, func() ([]byte, error) {
		start := time.Now()
		policy, err := fetchBucketIAMPolicy(ctx, n.bucketName)
		if err != nil {
			logGC("GCS:GetBucketIAM", start, n.bucketName, "ERROR", err)
			return nil, err
		}

		logGC("GCS:GetBucketIAM", start, n.bucketName, len(policy.Roles()), "roles")
		return formatGCSPolicyAsJSON(policy)
	})
}

// GCSIAMByRoleDirectoryNode represents .meta/iam-policy/by-role/
type GCSIAMByRoleDirectoryNode struct {
	fs.Inode
	bucketName string
}

var _ fs.NodeReaddirer = (*GCSIAMByRoleDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMByRoleDirectoryNode)(nil)
var _ fs.NodeLookuper = (*GCSIAMByRoleDirectoryNode)(nil)

func (n *GCSIAMByRoleDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	policy, err := fetchBucketIAMPolicy(ctx, n.bucketName)
	if err != nil {
		logGC("GCS:GetBucketIAM", start, n.bucketName, "ERROR", err)
		return nil, MapGCPError(err)
	}

	roles := extractGCSRoles(policy)
	logGC("GCS:GetBucketIAM", start, n.bucketName, len(roles), "roles")

	entries := make([]fuse.DirEntry, 0, len(roles))
	for role := range roles {
		entries = append(entries, fuse.DirEntry{
			Name: role,
			Mode: fuse.S_IFDIR,
		})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *GCSIAMByRoleDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *GCSIAMByRoleDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	child := n.NewInode(ctx, &GCSIAMRoleDirectoryNode{
		bucketName: n.bucketName,
		role:       name,
	}, stable)
	return child, 0
}

// GCSIAMRoleDirectoryNode represents .meta/iam-policy/by-role/{role}/
type GCSIAMRoleDirectoryNode struct {
	fs.Inode
	bucketName string
	role       string
}

var _ fs.NodeReaddirer = (*GCSIAMRoleDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMRoleDirectoryNode)(nil)
var _ fs.NodeLookuper = (*GCSIAMRoleDirectoryNode)(nil)

func (n *GCSIAMRoleDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	policy, err := fetchBucketIAMPolicy(ctx, n.bucketName)
	if err != nil {
		return nil, MapGCPError(err)
	}

	roles := extractGCSRoles(policy)
	members, ok := roles[n.role]
	if !ok {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	entries := make([]fuse.DirEntry, 0, len(members))
	for _, member := range members {
		entries = append(entries, fuse.DirEntry{
			Name: sanitizeMemberName(member),
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *GCSIAMRoleDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *GCSIAMRoleDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	child := n.NewInode(ctx, &GCSIAMMarkerFileNode{}, stable)

	// Set size to 0 for marker files
	out.Attr.Mode = 0644
	out.Attr.Size = 0
	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Nlink = 1

	return child, 0
}

// GCSIAMByMemberDirectoryNode represents .meta/iam-policy/by-member/
type GCSIAMByMemberDirectoryNode struct {
	fs.Inode
	bucketName string
}

var _ fs.NodeReaddirer = (*GCSIAMByMemberDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMByMemberDirectoryNode)(nil)
var _ fs.NodeLookuper = (*GCSIAMByMemberDirectoryNode)(nil)

func (n *GCSIAMByMemberDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	policy, err := fetchBucketIAMPolicy(ctx, n.bucketName)
	if err != nil {
		logGC("GCS:GetBucketIAM", start, n.bucketName, "ERROR", err)
		return nil, MapGCPError(err)
	}

	members := extractGCSMembers(policy)
	logGC("GCS:GetBucketIAM", start, n.bucketName, len(members), "members")

	entries := make([]fuse.DirEntry, 0, len(members))
	for member := range members {
		entries = append(entries, fuse.DirEntry{
			Name: member,
			Mode: fuse.S_IFDIR,
		})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *GCSIAMByMemberDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *GCSIAMByMemberDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	child := n.NewInode(ctx, &GCSIAMMemberDirectoryNode{
		bucketName: n.bucketName,
		member:     name,
	}, stable)
	return child, 0
}

// GCSIAMMemberDirectoryNode represents .meta/iam-policy/by-member/{member}/
type GCSIAMMemberDirectoryNode struct {
	fs.Inode
	bucketName string
	member     string
}

var _ fs.NodeReaddirer = (*GCSIAMMemberDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMMemberDirectoryNode)(nil)
var _ fs.NodeLookuper = (*GCSIAMMemberDirectoryNode)(nil)

func (n *GCSIAMMemberDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	policy, err := fetchBucketIAMPolicy(ctx, n.bucketName)
	if err != nil {
		return nil, MapGCPError(err)
	}

	members := extractGCSMembers(policy)
	roles, ok := members[n.member]
	if !ok {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	entries := make([]fuse.DirEntry, 0, len(roles))
	for _, role := range roles {
		entries = append(entries, fuse.DirEntry{
			Name: role,
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *GCSIAMMemberDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *GCSIAMMemberDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	child := n.NewInode(ctx, &GCSIAMMarkerFileNode{}, stable)

	// Set size to 0 for marker files
	out.Attr.Mode = 0644
	out.Attr.Size = 0
	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Nlink = 1

	return child, 0
}

// GCSIAMMarkerFileNode represents empty marker files in by-role/ and by-member/ directories
type GCSIAMMarkerFileNode struct {
	fs.Inode
}

var _ fs.NodeOpener = (*GCSIAMMarkerFileNode)(nil)
var _ fs.NodeGetattrer = (*GCSIAMMarkerFileNode)(nil)
var _ fs.NodeReader = (*GCSIAMMarkerFileNode)(nil)

func (n *GCSIAMMarkerFileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *GCSIAMMarkerFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0644
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = 0
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1
	return 0
}

func (n *GCSIAMMarkerFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Always return empty content
	return fuse.ReadResultData(nil), 0
}

// =============================================================================
// BigQuery IAM Policy Nodes
// =============================================================================

// BQIAMPolicyDirectoryNode represents .meta/iam-policy/ for a BigQuery dataset
type BQIAMPolicyDirectoryNode struct {
	fs.Inode
	projectID string
	datasetID string
}

var _ fs.NodeReaddirer = (*BQIAMPolicyDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMPolicyDirectoryNode)(nil)
var _ fs.NodeLookuper = (*BQIAMPolicyDirectoryNode)(nil)

func (n *BQIAMPolicyDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "bindings.json", Mode: fuse.S_IFREG},
		{Name: "by-role", Mode: fuse.S_IFDIR},
		{Name: "by-member", Mode: fuse.S_IFDIR},
	}
	return fs.NewListDirStream(entries), 0
}

func (n *BQIAMPolicyDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *BQIAMPolicyDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "bindings.json":
		stable := fs.StableAttr{Mode: fuse.S_IFREG}
		child := n.NewInode(ctx, &BQIAMPolicyFileNode{
			projectID: n.projectID,
			datasetID: n.datasetID,
		}, stable)

		// Populate attributes
		var attrOut fuse.AttrOut
		if errno := child.Operations().(fs.NodeGetattrer).Getattr(ctx, nil, &attrOut); errno == 0 {
			out.Attr = attrOut.Attr
		}
		return child, 0

	case "by-role":
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &BQIAMByRoleDirectoryNode{
			projectID: n.projectID,
			datasetID: n.datasetID,
		}, stable)
		return child, 0

	case "by-member":
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &BQIAMByMemberDirectoryNode{
			projectID: n.projectID,
			datasetID: n.datasetID,
		}, stable)
		return child, 0
	}

	return nil, syscall.ENOENT
}

// BQIAMPolicyFileNode represents .meta/iam-policy/bindings.json for BigQuery
type BQIAMPolicyFileNode struct {
	fs.Inode
	projectID string
	datasetID string
	bufferMu  sync.Mutex
	buffer    []byte
	bufValid  bool
}

var _ fs.NodeOpener = (*BQIAMPolicyFileNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMPolicyFileNode)(nil)
var _ fs.NodeReader = (*BQIAMPolicyFileNode)(nil)

func (n *BQIAMPolicyFileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *BQIAMPolicyFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content, err := n.generateContent(ctx)
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

func (n *BQIAMPolicyFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n.bufferMu.Lock()
	defer n.bufferMu.Unlock()

	if !n.bufValid {
		content, err := n.generateContent(ctx)
		if err != nil {
			return nil, MapGCPError(err)
		}
		n.buffer = content
		n.bufValid = true
	}

	if off >= int64(len(n.buffer)) {
		return fuse.ReadResultData(nil), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(n.buffer)) {
		end = int64(len(n.buffer))
	}

	return fuse.ReadResultData(n.buffer[off:end]), 0
}

func (n *BQIAMPolicyFileNode) generateContent(ctx context.Context) ([]byte, error) {
	cache := GetMetadataCache()

	return cache.GetDatasetIAMPolicy(ctx, n.projectID, n.datasetID, func() ([]byte, error) {
		start := time.Now()
		entries, err := fetchDatasetIAMPolicy(ctx, n.projectID, n.datasetID)
		if err != nil {
			logGC("BQ:GetDatasetIAM", start, n.datasetID, "ERROR", err)
			return nil, err
		}

		logGC("BQ:GetDatasetIAM", start, n.datasetID, len(entries), "entries")
		return formatBQAccessAsJSON(entries)
	})
}

// BQIAMByRoleDirectoryNode represents .meta/iam-policy/by-role/ for BigQuery
type BQIAMByRoleDirectoryNode struct {
	fs.Inode
	projectID string
	datasetID string
}

var _ fs.NodeReaddirer = (*BQIAMByRoleDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMByRoleDirectoryNode)(nil)
var _ fs.NodeLookuper = (*BQIAMByRoleDirectoryNode)(nil)

func (n *BQIAMByRoleDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	entries, err := fetchDatasetIAMPolicy(ctx, n.projectID, n.datasetID)
	if err != nil {
		logGC("BQ:GetDatasetIAM", start, n.datasetID, "ERROR", err)
		return nil, MapGCPError(err)
	}

	roles := extractBQRoles(entries)
	logGC("BQ:GetDatasetIAM", start, n.datasetID, len(roles), "roles")

	dirEntries := make([]fuse.DirEntry, 0, len(roles))
	for role := range roles {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: role,
			Mode: fuse.S_IFDIR,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

func (n *BQIAMByRoleDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *BQIAMByRoleDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	child := n.NewInode(ctx, &BQIAMRoleDirectoryNode{
		projectID: n.projectID,
		datasetID: n.datasetID,
		role:      name,
	}, stable)
	return child, 0
}

// BQIAMRoleDirectoryNode represents .meta/iam-policy/by-role/{role}/ for BigQuery
type BQIAMRoleDirectoryNode struct {
	fs.Inode
	projectID string
	datasetID string
	role      string
}

var _ fs.NodeReaddirer = (*BQIAMRoleDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMRoleDirectoryNode)(nil)
var _ fs.NodeLookuper = (*BQIAMRoleDirectoryNode)(nil)

func (n *BQIAMRoleDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := fetchDatasetIAMPolicy(ctx, n.projectID, n.datasetID)
	if err != nil {
		return nil, MapGCPError(err)
	}

	roles := extractBQRoles(entries)
	members, ok := roles[n.role]
	if !ok {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	dirEntries := make([]fuse.DirEntry, 0, len(members))
	for _, member := range members {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: member,
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

func (n *BQIAMRoleDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *BQIAMRoleDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	child := n.NewInode(ctx, &BQIAMMarkerFileNode{}, stable)

	// Set size to 0 for marker files
	out.Attr.Mode = 0644
	out.Attr.Size = 0
	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Nlink = 1

	return child, 0
}

// BQIAMByMemberDirectoryNode represents .meta/iam-policy/by-member/ for BigQuery
type BQIAMByMemberDirectoryNode struct {
	fs.Inode
	projectID string
	datasetID string
}

var _ fs.NodeReaddirer = (*BQIAMByMemberDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMByMemberDirectoryNode)(nil)
var _ fs.NodeLookuper = (*BQIAMByMemberDirectoryNode)(nil)

func (n *BQIAMByMemberDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	entries, err := fetchDatasetIAMPolicy(ctx, n.projectID, n.datasetID)
	if err != nil {
		logGC("BQ:GetDatasetIAM", start, n.datasetID, "ERROR", err)
		return nil, MapGCPError(err)
	}

	members := extractBQMembers(entries)
	logGC("BQ:GetDatasetIAM", start, n.datasetID, len(members), "members")

	dirEntries := make([]fuse.DirEntry, 0, len(members))
	for member := range members {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: member,
			Mode: fuse.S_IFDIR,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

func (n *BQIAMByMemberDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *BQIAMByMemberDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	child := n.NewInode(ctx, &BQIAMMemberDirectoryNode{
		projectID: n.projectID,
		datasetID: n.datasetID,
		member:    name,
	}, stable)
	return child, 0
}

// BQIAMMemberDirectoryNode represents .meta/iam-policy/by-member/{member}/ for BigQuery
type BQIAMMemberDirectoryNode struct {
	fs.Inode
	projectID string
	datasetID string
	member    string
}

var _ fs.NodeReaddirer = (*BQIAMMemberDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMMemberDirectoryNode)(nil)
var _ fs.NodeLookuper = (*BQIAMMemberDirectoryNode)(nil)

func (n *BQIAMMemberDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := fetchDatasetIAMPolicy(ctx, n.projectID, n.datasetID)
	if err != nil {
		return nil, MapGCPError(err)
	}

	members := extractBQMembers(entries)
	roles, ok := members[n.member]
	if !ok {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	dirEntries := make([]fuse.DirEntry, 0, len(roles))
	for _, role := range roles {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: role,
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

func (n *BQIAMMemberDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *BQIAMMemberDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	child := n.NewInode(ctx, &BQIAMMarkerFileNode{}, stable)

	// Set size to 0 for marker files
	out.Attr.Mode = 0644
	out.Attr.Size = 0
	out.Attr.Uid = uint32(os.Getuid())
	out.Attr.Gid = uint32(os.Getgid())
	out.Attr.Nlink = 1

	return child, 0
}

// BQIAMMarkerFileNode represents empty marker files in by-role/ and by-member/ directories for BigQuery
type BQIAMMarkerFileNode struct {
	fs.Inode
}

var _ fs.NodeOpener = (*BQIAMMarkerFileNode)(nil)
var _ fs.NodeGetattrer = (*BQIAMMarkerFileNode)(nil)
var _ fs.NodeReader = (*BQIAMMarkerFileNode)(nil)

func (n *BQIAMMarkerFileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *BQIAMMarkerFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0644
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = 0
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1
	return 0
}

func (n *BQIAMMarkerFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Always return empty content
	return fuse.ReadResultData(nil), 0
}
