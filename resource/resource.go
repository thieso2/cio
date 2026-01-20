package resource

import (
	"context"
	"time"
)

// Type represents the type of resource (GCS, BigQuery, or IAM)
type Type string

const (
	TypeGCS      Type = "gcs"
	TypeBigQuery Type = "bq"
	TypeIAM      Type = "iam"
)

// ResourceInfo holds unified information about a resource (object, table, dataset, etc.)
type ResourceInfo struct {
	Path        string    // Full path (gs://... or bq://... or iam://...)
	Name        string    // Just the name component
	Type        string    // "file", "directory", "table", "dataset", "service-account"
	Size        int64     // Size in bytes
	Rows        int64     // Number of rows (BigQuery only)
	Created     time.Time // Creation time
	Modified    time.Time // Last modified time
	Description string    // Description (if available)
	Location    string    // Location/region
	IsDir       bool      // Is this a directory?

	// For detailed info (BigQuery schema, IAM account info, etc.)
	Details  interface{} // Type-specific details
	Metadata interface{} // Type-specific metadata for formatting
}

// ListOptions contains options for listing resources
type ListOptions struct {
	Recursive     bool
	LongFormat    bool
	HumanReadable bool
	MaxResults    int
	Pattern       string // Wildcard pattern (if applicable)
	ProjectID     string // GCP Project ID (for bucket listing)
}

// RemoveOptions contains options for removing resources
type RemoveOptions struct {
	Recursive bool
	Force     bool
	Verbose   bool
}

// PathComponents represents parsed path components
type PathComponents struct {
	ResourceType Type
	Project      string // For BigQuery or GCS project
	Bucket       string // For GCS
	Object       string // For GCS
	Dataset      string // For BigQuery
	Table        string // For BigQuery
}

// Resource defines the interface for all resource types
type Resource interface {
	// Type returns the resource type
	Type() Type

	// List lists resources at the given path
	List(ctx context.Context, path string, options *ListOptions) ([]*ResourceInfo, error)

	// Remove removes resource(s) at the given path
	Remove(ctx context.Context, path string, options *RemoveOptions) error

	// Info gets detailed information about a specific resource
	Info(ctx context.Context, path string) (*ResourceInfo, error)

	// ParsePath parses a resource path into components
	ParsePath(path string) (*PathComponents, error)

	// FormatShort formats resource info in short format
	FormatShort(info *ResourceInfo, aliasPath string) string

	// FormatLong formats resource info in long format
	FormatLong(info *ResourceInfo, aliasPath string) string

	// FormatDetailed formats resource info with full details
	FormatDetailed(info *ResourceInfo, aliasPath string) string

	// FormatLongHeader returns the header line for long format (empty string if no header needed)
	FormatLongHeader() string

	// SupportsInfo returns whether this resource type supports detailed info
	SupportsInfo() bool
}

// PathFormatter is a function that converts full paths to alias format
type PathFormatter func(string) string
