package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/storage"
)

// GCSResource implements the Resource interface for Google Cloud Storage
type GCSResource struct {
	formatter PathFormatter
}

// CreateGCSResource creates a new GCS resource handler
func CreateGCSResource(formatter PathFormatter) *GCSResource {
	return &GCSResource{
		formatter: formatter,
	}
}

// Type returns the resource type
func (g *GCSResource) Type() Type {
	return TypeGCS
}

// List lists GCS buckets or objects at the given path
func (g *GCSResource) List(ctx context.Context, path string, options *ListOptions) ([]*ResourceInfo, error) {
	bucket, object, err := resolver.ParseGCSPath(path)
	if err != nil {
		return nil, err
	}

	// Handle bucket listing (gs:// or gs://project-id:)
	if bucket == "" || (bucket != "" && bucket[len(bucket)-1] == ':') {
		// Extract project ID if specified in path
		projectID := ""
		if bucket != "" && bucket[len(bucket)-1] == ':' {
			projectID = bucket[:len(bucket)-1]
		}
		// If no project ID specified in path, use from options
		if projectID == "" && options != nil {
			projectID = options.ProjectID
		}

		// Check if we have a project ID
		if projectID == "" {
			return nil, fmt.Errorf("project ID required for bucket listing. Use 'gs://project-id:' or set project_id in config")
		}

		// List buckets
		buckets, err := storage.ListBuckets(ctx, projectID)
		if err != nil {
			return nil, err
		}

		// Convert to ResourceInfo
		result := make([]*ResourceInfo, len(buckets))
		for i, b := range buckets {
			result[i] = &ResourceInfo{
				Path:     fmt.Sprintf("gs://%s/", b.Name),
				Name:     b.Name,
				Type:     "bucket",
				Location: b.Location,
				Details:  b,
			}
		}

		return result, nil
	}

	// Handle object listing
	storageOpts := &storage.ListOptions{
		Recursive:     options.Recursive,
		LongFormat:    options.LongFormat,
		HumanReadable: options.HumanReadable,
		MaxResults:    options.MaxResults,
		Delimiter:     "/", // Use delimiter to group by directories (non-recursive listing)
	}

	var objects []*storage.ObjectInfo
	if options.Pattern != "" || resolver.HasWildcard(object) {
		pattern := options.Pattern
		if pattern == "" {
			pattern = object
		}
		objects, err = storage.ListWithPattern(ctx, bucket, pattern, storageOpts)
	} else {
		objects, err = storage.ListByPath(ctx, path, storageOpts)
	}

	if err != nil {
		return nil, err
	}

	// Convert to ResourceInfo
	result := make([]*ResourceInfo, len(objects))
	for i, obj := range objects {
		objType := "file"
		isDir := obj.IsPrefix
		if isDir {
			objType = "directory"
		}

		// Extract name from path (last component after gs://bucket/)
		name := obj.Path
		if strings.HasPrefix(name, "gs://") {
			// Remove gs://bucket/ prefix
			if idx := strings.Index(name[5:], "/"); idx != -1 {
				name = name[5+idx+1:]
			}
			// Remove trailing slash for directories
			name = strings.TrimSuffix(name, "/")
			// Get just the last component
			if idx := strings.LastIndex(name, "/"); idx != -1 {
				name = name[idx+1:]
			}
		}

		result[i] = &ResourceInfo{
			Path:     obj.Path,
			Name:     name,
			Type:     objType,
			Size:     obj.Size,
			Modified: obj.Updated,
			IsDir:    isDir,
			Details:  obj,
		}
	}

	return result, nil
}

// Remove removes GCS object(s) at the given path
func (g *GCSResource) Remove(ctx context.Context, path string, options *RemoveOptions) error {
	bucket, object, err := resolver.ParseGCSPath(path)
	if err != nil {
		return err
	}

	client, err := storage.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Convert to storage.PathFormatter
	storageFormatter := storage.PathFormatter(g.formatter)

	// Use parallelism from options, default to 50 if not set
	parallelism := options.Parallelism
	if parallelism == 0 {
		parallelism = storage.DefaultConcurrentDeletes
	}

	// Check if path contains wildcards
	if resolver.HasWildcard(object) {
		return storage.RemoveWithPattern(ctx, client, bucket, object, options.Verbose, storageFormatter, parallelism)
	}

	// Check if this is a directory or single object
	isDirectory := object == "" || object[len(object)-1] == '/'

	if isDirectory {
		return storage.RemoveDirectory(ctx, client, bucket, object, options.Verbose, storageFormatter, parallelism)
	}

	return storage.RemoveObject(ctx, client, bucket, object, options.Verbose, storageFormatter)
}

// Info gets detailed information about a GCS object
func (g *GCSResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("info command not supported for GCS objects (use 'ls -l' instead)")
}

// ParsePath parses a GCS path into components
func (g *GCSResource) ParsePath(path string) (*PathComponents, error) {
	bucket, object, err := resolver.ParseGCSPath(path)
	if err != nil {
		return nil, err
	}

	return &PathComponents{
		ResourceType: TypeGCS,
		Bucket:       bucket,
		Object:       object,
	}, nil
}

// FormatShort formats GCS object info in short format
func (g *GCSResource) FormatShort(info *ResourceInfo, aliasPath string) string {
	// For buckets, show the gs:// path if no alias
	if info.Type == "bucket" {
		if aliasPath != "" && aliasPath != info.Path {
			return aliasPath
		}
		return info.Path
	}
	return aliasPath
}

// FormatLong formats GCS object info in long format
func (g *GCSResource) FormatLong(info *ResourceInfo, aliasPath string) string {
	if bucket, ok := info.Details.(*storage.BucketInfo); ok {
		return storage.FormatBucketLong(bucket)
	}
	if obj, ok := info.Details.(*storage.ObjectInfo); ok {
		return obj.FormatLongWithAlias(false, aliasPath)
	}
	return aliasPath
}

// FormatDetailed formats GCS object info with full details
func (g *GCSResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return g.FormatLong(info, aliasPath)
}

// FormatLongHeader returns the header line for long format listing
func (g *GCSResource) FormatLongHeader() string {
	// GCS doesn't use a header for now
	return ""
}

// SupportsInfo returns whether GCS supports the info command
func (g *GCSResource) SupportsInfo() bool {
	return false
}
