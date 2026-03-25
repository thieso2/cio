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
	Path        string    `json:"path"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Size        int64     `json:"size,omitempty"`
	Rows        int64     `json:"rows,omitempty"`
	Created     time.Time `json:"created,omitempty"`
	Modified    time.Time `json:"modified,omitempty"`
	Description string    `json:"description,omitempty"`
	Location    string    `json:"location,omitempty"`
	IsDir       bool      `json:"is_dir,omitempty"`

	// For detailed info (BigQuery schema, IAM account info, etc.)
	Details  interface{} `json:"details,omitempty"`
	Metadata interface{} `json:"metadata,omitempty"`
}

// ListOptions contains options for listing resources
type ListOptions struct {
	Recursive     bool
	LongFormat    bool
	HumanReadable bool
	MaxResults    int
	Pattern       string // Wildcard pattern (if applicable)
	ProjectID     string // GCP Project ID (for bucket listing and Cloud Run)
	Region        string // GCP Region (for Cloud Run)
	ActiveOnly    bool   // Only show active resources (for Dataflow)
	AllStatuses   bool   // Show all statuses (e.g., include completed executions)
	Month         string // Month filter for billing (YYYYMM format)
}

// RemoveOptions contains options for removing resources
type RemoveOptions struct {
	Recursive   bool
	Force       bool
	Verbose     bool
	Parallelism int    // Number of parallel operations (for GCS only)
	Project     string // GCP Project ID (for Cloud Run)
	Region      string // GCP Region (for Cloud Run)
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
