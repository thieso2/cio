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

// Resource is the deep core every resource type implements: listing and
// formatting. It is intentionally narrow — every method here is satisfied by
// all twelve resource types, so the interface earns its keep. Operations that
// only some types support (remove, info, cancel) live in the capability
// interfaces below, which callers type-assert to. That keeps the promise honest:
// a type appears Removable only if it actually removes.
type Resource interface {
	// Type returns the resource type
	Type() Type

	// List lists resources at the given path
	List(ctx context.Context, path string, options *ListOptions) ([]*ResourceInfo, error)

	// FormatShort formats resource info in short format
	FormatShort(info *ResourceInfo, aliasPath string) string

	// FormatLong formats resource info in long format
	FormatLong(info *ResourceInfo, aliasPath string) string

	// FormatDetailed formats resource info with full details
	FormatDetailed(info *ResourceInfo, aliasPath string) string

	// FormatLongHeader returns the header line for long format (empty string if no header needed)
	FormatLongHeader() string
}

// Removable is implemented by resource types that support deletion via `cio rm`.
type Removable interface {
	Remove(ctx context.Context, path string, options *RemoveOptions) error
}

// Infoable is implemented by resource types that support `cio info` (detailed
// single-resource view) without needing an explicit project id.
type Infoable interface {
	Info(ctx context.Context, path string) (*ResourceInfo, error)
}

// ProjectInfoable is implemented by resource types whose detailed info needs an
// explicit project id (Pub/Sub, Cloud SQL). Callers prefer this over Infoable.
type ProjectInfoable interface {
	InfoWithProject(ctx context.Context, path, project string) (*ResourceInfo, error)
}

// Cancelable is implemented by resource types that support cancellation
// (currently Cloud Run job executions via `cio cancel`).
type Cancelable interface {
	Cancel(ctx context.Context, path string, options *RemoveOptions) error
}

// PathFormatter is a function that converts full paths to alias format
type PathFormatter func(string) string
