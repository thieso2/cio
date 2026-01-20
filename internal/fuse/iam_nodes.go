package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/thieso2/cio/iam"
)

// listIAMResourceTypes lists IAM resource types (currently only service-accounts)
func listIAMResourceTypes(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "service-accounts", Mode: fuse.S_IFDIR},
	}
	return fs.NewListDirStream(entries), 0
}

// IAMResourceTypeNode represents an IAM resource type directory (e.g., /mnt/gcp/iam/service-accounts/)
type IAMResourceTypeNode struct {
	fs.Inode
	projectID    string
	resourceType string
}

var _ fs.NodeReaddirer = (*IAMResourceTypeNode)(nil)
var _ fs.NodeGetattrer = (*IAMResourceTypeNode)(nil)
var _ fs.NodeLookuper = (*IAMResourceTypeNode)(nil)

// Readdir lists all service accounts in the project
func (n *IAMResourceTypeNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.resourceType != "service-accounts" {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	// Cache service account list for 30 minutes
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("iam:service-accounts:%s", n.projectID)

	accountData, err := cache.GetWithTTL(ctx, cacheKey, ListCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss or expiry
		apiStart := time.Now()
		accounts, err := iam.ListServiceAccounts(ctx, n.projectID)
		if err != nil {
			logGC("IAM:ListServiceAccounts", apiStart, n.projectID, "ERROR", err)
			return nil, err
		}

		// Serialize account emails as JSON
		emails := make([]string, 0, len(accounts))
		for _, account := range accounts {
			emails = append(emails, account.Email)
		}

		// Log successful API call
		logGC("IAM:ListServiceAccounts", apiStart, n.projectID, len(emails), "accounts")
		return json.Marshal(emails)
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Deserialize cached account emails
	var emails []string
	if err := json.Unmarshal(accountData, &emails); err != nil {
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(emails))
	for _, email := range emails {
		entries = append(entries, fuse.DirEntry{
			Name: email,
			Mode: fuse.S_IFDIR, // Service accounts are directories (have metadata)
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the resource type directory
func (n *IAMResourceTypeNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a service account by email
func (n *IAMResourceTypeNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Ignore files starting with "." (like .DS_Store)
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	// Only handle service accounts for now
	if n.resourceType != "service-accounts" {
		return nil, syscall.ENOENT
	}

	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR, // Service accounts are directories
	}
	child := n.NewInode(ctx, &ServiceAccountNode{
		projectID: n.projectID,
		email:     name,
	}, stable)
	return child, 0
}

// ServiceAccountNode represents a service account directory (e.g., /mnt/gcp/iam/service-accounts/account@project.iam.gserviceaccount.com/)
type ServiceAccountNode struct {
	fs.Inode
	projectID string
	email     string
}

var _ fs.NodeReaddirer = (*ServiceAccountNode)(nil)
var _ fs.NodeGetattrer = (*ServiceAccountNode)(nil)
var _ fs.NodeLookuper = (*ServiceAccountNode)(nil)

// Readdir lists virtual files in the service account directory
func (n *ServiceAccountNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "metadata.json", Mode: fuse.S_IFREG}, // Service account metadata
		{Name: "keys", Mode: fuse.S_IFDIR},          // Service account keys
		{Name: "usage", Mode: fuse.S_IFDIR},         // Where the SA has permissions
	}
	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the service account directory
func (n *ServiceAccountNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds virtual files in the service account directory
func (n *ServiceAccountNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Ignore files starting with "." (like .DS_Store)
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	switch name {
	case "metadata.json":
		stable := fs.StableAttr{Mode: fuse.S_IFREG}
		child := n.NewInode(ctx, &ServiceAccountMetaFileNode{
			projectID: n.projectID,
			email:     n.email,
		}, stable)
		return child, 0

	case "keys":
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &ServiceAccountKeysDirectoryNode{
			projectID: n.projectID,
			email:     n.email,
		}, stable)
		return child, 0

	case "usage":
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &ServiceAccountUsageDirectoryNode{
			projectID: n.projectID,
			email:     n.email,
		}, stable)
		return child, 0
	}

	return nil, syscall.ENOENT
}

// ServiceAccountMetaFileNode represents a virtual metadata file for a service account
type ServiceAccountMetaFileNode struct {
	fs.Inode
	projectID string
	email     string
}

var _ fs.NodeOpener = (*ServiceAccountMetaFileNode)(nil)
var _ fs.NodeGetattrer = (*ServiceAccountMetaFileNode)(nil)
var _ fs.NodeReader = (*ServiceAccountMetaFileNode)(nil)

// Open opens the virtual file for reading
func (n *ServiceAccountMetaFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Read-only
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Getattr returns attributes for the virtual file
func (n *ServiceAccountMetaFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Get metadata cache
	cache := GetMetadataCache()

	// Fetch metadata to get the actual size
	cacheKey := fmt.Sprintf("iam:account:%s:%s", n.projectID, n.email)
	metadata, err := cache.GetWithTTL(ctx, cacheKey, MetadataCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss
		apiStart := time.Now()
		account, err := iam.GetServiceAccount(ctx, n.projectID, n.email)
		if err != nil {
			logGC("IAM:GetServiceAccount", apiStart, n.projectID, n.email, "ERROR", err)
			return nil, err
		}

		// Format as JSON
		content := formatServiceAccountAsJSON(account)

		// Log successful API call
		logGC("IAM:GetServiceAccount", apiStart, n.projectID, n.email, "size", len(content))
		return []byte(content), nil
	})

	if err != nil {
		// If metadata fetch fails, use approximate size
		out.Size = 2048
	} else {
		// Set actual size from cached metadata
		out.Size = uint64(len(metadata))
	}

	out.Mode = 0444 | fuse.S_IFREG // Read-only file
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 1

	return 0
}

// Read returns the content of the virtual file
func (n *ServiceAccountMetaFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Get metadata cache
	cache := GetMetadataCache()

	// Use cache for service account metadata
	cacheKey := fmt.Sprintf("iam:account:%s:%s", n.projectID, n.email)
	metadata, err := cache.GetWithTTL(ctx, cacheKey, MetadataCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss
		apiStart := time.Now()
		account, err := iam.GetServiceAccount(ctx, n.projectID, n.email)
		if err != nil {
			logGC("IAM:GetServiceAccount", apiStart, n.projectID, n.email, "ERROR", err)
			return nil, err
		}

		// Format as JSON
		content := formatServiceAccountAsJSON(account)

		// Log successful API call
		logGC("IAM:GetServiceAccount", apiStart, n.projectID, n.email, "size", len(content))
		return []byte(content), nil
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Handle offset and length
	if off >= int64(len(metadata)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(metadata)) {
		end = int64(len(metadata))
	}

	return fuse.ReadResultData(metadata[off:end]), 0
}

// formatServiceAccountAsJSON formats service account info as JSON
func formatServiceAccountAsJSON(account *iam.ServiceAccountInfo) string {
	// Create JSON object with all service account fields
	data := map[string]interface{}{
		"email":        account.Email,
		"name":         account.Name,
		"display_name": account.DisplayName,
		"description":  account.Description,
		"disabled":     account.Disabled,
		"project_id":   account.ProjectID,
	}

	// Marshal to pretty JSON
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "{}"
	}

	return string(jsonBytes)
}

// =============================================================================
// Service Account Keys Directory
// =============================================================================

// ServiceAccountKeysDirectoryNode represents the keys/ directory for a service account
type ServiceAccountKeysDirectoryNode struct {
	fs.Inode
	projectID string
	email     string
}

var _ fs.NodeReaddirer = (*ServiceAccountKeysDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*ServiceAccountKeysDirectoryNode)(nil)
var _ fs.NodeLookuper = (*ServiceAccountKeysDirectoryNode)(nil)

func (n *ServiceAccountKeysDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Cache key list for 30 minutes
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("iam:keys:%s:%s", n.projectID, n.email)

	keyData, err := cache.GetWithTTL(ctx, cacheKey, ListCacheTTL, func() ([]byte, error) {
		apiStart := time.Now()
		keys, err := iam.ListServiceAccountKeys(ctx, n.projectID, n.email)
		if err != nil {
			logGC("IAM:ListKeys", apiStart, n.projectID, n.email, "ERROR", err)
			return nil, err
		}

		// Serialize key IDs as JSON
		keyIDs := make([]string, 0, len(keys))
		for _, key := range keys {
			if key.KeyID != "" {
				keyIDs = append(keyIDs, key.KeyID)
			}
		}

		logGC("IAM:ListKeys", apiStart, n.projectID, n.email, len(keyIDs), "keys")
		return json.Marshal(keyIDs)
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Deserialize cached key IDs
	var keyIDs []string
	if err := json.Unmarshal(keyData, &keyIDs); err != nil {
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(keyIDs))
	for _, keyID := range keyIDs {
		entries = append(entries, fuse.DirEntry{
			Name: keyID + ".json",
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *ServiceAccountKeysDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *ServiceAccountKeysDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Ignore files starting with "."
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	// Only allow .json files
	if len(name) < 6 || name[len(name)-5:] != ".json" {
		return nil, syscall.ENOENT
	}

	keyID := name[:len(name)-5]

	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	child := n.NewInode(ctx, &ServiceAccountKeyFileNode{
		projectID: n.projectID,
		email:     n.email,
		keyID:     keyID,
	}, stable)

	// Populate entry attributes
	var attrOut fuse.AttrOut
	if errno := child.Operations().(fs.NodeGetattrer).Getattr(ctx, nil, &attrOut); errno == 0 {
		out.Attr = attrOut.Attr
	}

	return child, 0
}

// ServiceAccountKeyFileNode represents a key metadata file
type ServiceAccountKeyFileNode struct {
	fs.Inode
	projectID string
	email     string
	keyID     string
}

var _ fs.NodeOpener = (*ServiceAccountKeyFileNode)(nil)
var _ fs.NodeGetattrer = (*ServiceAccountKeyFileNode)(nil)
var _ fs.NodeReader = (*ServiceAccountKeyFileNode)(nil)

func (n *ServiceAccountKeyFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *ServiceAccountKeyFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content, err := n.generateContent(ctx)
	if err != nil {
		return MapGCPError(err)
	}

	out.Mode = 0444 | fuse.S_IFREG
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(len(content))
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1
	return 0
}

func (n *ServiceAccountKeyFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	content, err := n.generateContent(ctx)
	if err != nil {
		return nil, MapGCPError(err)
	}

	if off >= int64(len(content)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(content)) {
		end = int64(len(content))
	}

	return fuse.ReadResultData(content[off:end]), 0
}

func (n *ServiceAccountKeyFileNode) generateContent(ctx context.Context) ([]byte, error) {
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("iam:key:%s:%s:%s", n.projectID, n.email, n.keyID)

	return cache.GetWithTTL(ctx, cacheKey, MetadataCacheTTL, func() ([]byte, error) {
		apiStart := time.Now()
		keys, err := iam.ListServiceAccountKeys(ctx, n.projectID, n.email)
		if err != nil {
			logGC("IAM:ListKeys", apiStart, n.projectID, n.email, "ERROR", err)
			return nil, err
		}

		// Find the specific key
		var targetKey *iam.ServiceAccountKeyInfo
		for _, key := range keys {
			if key.KeyID == n.keyID {
				targetKey = key
				break
			}
		}

		if targetKey == nil {
			return nil, fmt.Errorf("key not found: %s", n.keyID)
		}

		// Format as JSON
		data := map[string]interface{}{
			"name":               targetKey.Name,
			"key_id":             targetKey.KeyID,
			"key_type":           targetKey.KeyType,
			"key_algorithm":      targetKey.KeyAlgorithm,
			"valid_after_time":   targetKey.ValidAfterTime.Format(time.RFC3339),
			"valid_before_time":  targetKey.ValidBeforeTime.Format(time.RFC3339),
			"disabled":           targetKey.Disabled,
		}

		logGC("IAM:GetKey", apiStart, n.projectID, n.email, n.keyID)
		return json.MarshalIndent(data, "", "  ")
	})
}

// =============================================================================
// Service Account Usage Directory
// =============================================================================

// ServiceAccountUsageDirectoryNode represents the usage/ directory for a service account
type ServiceAccountUsageDirectoryNode struct {
	fs.Inode
	projectID string
	email     string
}

var _ fs.NodeReaddirer = (*ServiceAccountUsageDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*ServiceAccountUsageDirectoryNode)(nil)
var _ fs.NodeLookuper = (*ServiceAccountUsageDirectoryNode)(nil)

func (n *ServiceAccountUsageDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Cache usage data for 1 hour
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("iam:usage:%s:%s", n.projectID, n.email)

	usageData, err := cache.GetWithTTL(ctx, cacheKey, IAMPolicyCacheTTL, func() ([]byte, error) {
		apiStart := time.Now()
		usage, err := iam.GetServiceAccountUsage(ctx, n.projectID, n.email)
		if err != nil {
			logGC("IAM:GetUsage", apiStart, n.projectID, n.email, "ERROR", err)
			return nil, err
		}

		// Group by resource type
		typeMap := make(map[string]bool)
		for _, u := range usage {
			if u.ResourceType != "" {
				typeMap[u.ResourceType] = true
			}
		}

		// Serialize resource types as JSON
		types := make([]string, 0, len(typeMap))
		for t := range typeMap {
			types = append(types, t)
		}

		logGC("IAM:GetUsage", apiStart, n.projectID, n.email, len(types), "types")
		return json.Marshal(types)
	})

	if err != nil {
		// Return empty directory on error (usage tracking might not be enabled)
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	// Deserialize cached resource types
	var types []string
	if err := json.Unmarshal(usageData, &types); err != nil {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	entries := make([]fuse.DirEntry, 0, len(types))
	for _, t := range types {
		entries = append(entries, fuse.DirEntry{
			Name: t,
			Mode: fuse.S_IFDIR,
		})
	}

	// If no usage found, show a placeholder file
	if len(entries) == 0 {
		entries = append(entries, fuse.DirEntry{
			Name: "README.txt",
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *ServiceAccountUsageDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

func (n *ServiceAccountUsageDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Ignore files starting with "."
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	// Handle README placeholder
	if name == "README.txt" {
		stable := fs.StableAttr{Mode: fuse.S_IFREG}
		child := n.NewInode(ctx, &UsageReadmeFileNode{}, stable)
		return child, 0
	}

	// Otherwise, it's a resource type directory
	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	child := n.NewInode(ctx, &ServiceAccountUsageTypeDirectoryNode{
		projectID:    n.projectID,
		email:        n.email,
		resourceType: name,
	}, stable)
	return child, 0
}

// UsageReadmeFileNode shows a message when usage tracking returns no results
type UsageReadmeFileNode struct {
	fs.Inode
}

var _ fs.NodeOpener = (*UsageReadmeFileNode)(nil)
var _ fs.NodeGetattrer = (*UsageReadmeFileNode)(nil)
var _ fs.NodeReader = (*UsageReadmeFileNode)(nil)

func (n *UsageReadmeFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *UsageReadmeFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content := `Service Account Usage Tracking

No usage information found for this service account.

To enable usage tracking, you need to implement Cloud Asset Inventory API integration.
This requires enabling the cloudasset.googleapis.com API and granting the appropriate
permissions to search IAM policies across your organization or project.

The usage/ directory will show:
- storage/ - GCS buckets where this SA has permissions
- bigquery/ - BigQuery datasets where this SA has permissions
- compute/ - Compute Engine resources where this SA has permissions
- And other GCP resource types

For now, this feature returns an empty directory.
`

	out.Mode = 0444 | fuse.S_IFREG
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Size = uint64(len(content))
	out.Mtime = uint64(time.Now().Unix())
	out.Nlink = 1
	return 0
}

func (n *UsageReadmeFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	content := `Service Account Usage Tracking

No usage information found for this service account.

To enable usage tracking, you need to implement Cloud Asset Inventory API integration.
This requires enabling the cloudasset.googleapis.com API and granting the appropriate
permissions to search IAM policies across your organization or project.

The usage/ directory will show:
- storage/ - GCS buckets where this SA has permissions
- bigquery/ - BigQuery datasets where this SA has permissions
- compute/ - Compute Engine resources where this SA has permissions
- And other GCP resource types

For now, this feature returns an empty directory.
`

	if off >= int64(len(content)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(content)) {
		end = int64(len(content))
	}

	return fuse.ReadResultData([]byte(content)[off:end]), 0
}

// ServiceAccountUsageTypeDirectoryNode represents usage/{resource-type}/ directory
type ServiceAccountUsageTypeDirectoryNode struct {
	fs.Inode
	projectID    string
	email        string
	resourceType string
}

var _ fs.NodeReaddirer = (*ServiceAccountUsageTypeDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*ServiceAccountUsageTypeDirectoryNode)(nil)

func (n *ServiceAccountUsageTypeDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Get usage data from cache
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("iam:usage:%s:%s", n.projectID, n.email)

	usageData, err := cache.GetWithTTL(ctx, cacheKey, IAMPolicyCacheTTL, func() ([]byte, error) {
		usage, err := iam.GetServiceAccountUsage(ctx, n.projectID, n.email)
		if err != nil {
			return nil, err
		}

		// Serialize entire usage list as JSON
		return json.Marshal(usage)
	})

	if err != nil {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	// Deserialize usage list
	var usageList []*iam.UsageInfo
	if err := json.Unmarshal(usageData, &usageList); err != nil {
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	// Filter by resource type and create entries
	entries := []fuse.DirEntry{}
	for _, u := range usageList {
		if u.ResourceType == n.resourceType && u.ResourceName != "" {
			entries = append(entries, fuse.DirEntry{
				Name: u.ResourceName,
				Mode: fuse.S_IFREG,
			})
		}
	}

	return fs.NewListDirStream(entries), 0
}

func (n *ServiceAccountUsageTypeDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}
