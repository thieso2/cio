package storage

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// ListOptions configures listing behavior
type ListOptions struct {
	Recursive     bool // List all objects recursively
	LongFormat    bool // Show detailed information
	HumanReadable bool // Show sizes in human-readable format
	Delimiter     string
	MaxResults    int // Maximum number of results (0 = no limit)
}

// DefaultListOptions returns the default listing options
func DefaultListOptions() *ListOptions {
	return &ListOptions{
		Recursive:     false,
		LongFormat:    false,
		HumanReadable: false,
		Delimiter:     "/", // Default delimiter for prefix grouping
		MaxResults:    0,   // No limit by default
	}
}

// List retrieves objects from a GCS bucket with optional prefix
func List(ctx context.Context, bucket, prefix string, opts *ListOptions) ([]*ObjectInfo, error) {
	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	if opts == nil {
		opts = DefaultListOptions()
	}

	// Configure query
	query := &storage.Query{
		Prefix: prefix,
	}

	// If not recursive, use delimiter to group by "directories"
	if !opts.Recursive {
		query.Delimiter = opts.Delimiter
	}

	// Execute query
	bucketHandle := client.Bucket(bucket)
	it := bucketHandle.Objects(ctx, query)

	var results []*ObjectInfo
	count := 0

	for {
		// Check if we've reached the max results limit
		if opts.MaxResults > 0 && count >= opts.MaxResults {
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate objects: %w", err)
		}

		// Handle prefixes (directories)
		if attrs.Prefix != "" {
			results = append(results, NewPrefixInfo(attrs.Prefix, bucket))
			count++
			continue
		}

		// Handle objects
		results = append(results, NewObjectInfo(attrs, bucket))
		count++
	}

	return results, nil
}

// ListByPath is a convenience function that parses a gs:// path and lists objects
func ListByPath(ctx context.Context, gcsPath string, opts *ListOptions) ([]*ObjectInfo, error) {
	bucket, prefix, err := parseGCSPath(gcsPath)
	if err != nil {
		return nil, err
	}

	return List(ctx, bucket, prefix, opts)
}

// parseGCSPath parses a gs:// path into bucket and prefix
func parseGCSPath(gcsPath string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid GCS path: must start with gs://")
	}

	// Remove gs:// prefix
	pathWithoutPrefix := strings.TrimPrefix(gcsPath, "gs://")

	// Split into bucket and prefix
	parts := strings.SplitN(pathWithoutPrefix, "/", 2)
	bucket = parts[0]

	if bucket == "" {
		return "", "", fmt.Errorf("invalid GCS path: bucket name is required")
	}

	if len(parts) > 1 {
		prefix = parts[1]
	}

	return bucket, prefix, nil
}
