package web

import "time"

// AliasInfo represents an alias mapping
type AliasInfo struct {
	Name     string `json:"name"`
	FullPath string `json:"fullPath"`
	Type     string `json:"type"` // "gcs", "bigquery", "iam"
}

// ResourceItem represents a single resource (file, folder, table, etc.)
type ResourceItem struct {
	Name       string    `json:"name"`
	Type       string    `json:"type"` // "file", "dir", "table", "dataset", "service-account"
	Size       int64     `json:"size"`
	SizeHuman  string    `json:"sizeHuman"`
	Modified   time.Time `json:"modified"`
	Path       string    `json:"path"`       // Full path (gs://, bq://, iam://)
	AliasPath  string    `json:"aliasPath"`  // Alias path (:am/file)
	IsDir      bool      `json:"isDir"`
	Additional string    `json:"additional"` // Extra info (e.g., row count for BQ)
}

// Breadcrumb represents a navigation breadcrumb
type Breadcrumb struct {
	Name string `json:"name"`
	Path string `json:"path"` // Alias path
}

// BrowseResponse is the response for /api/browse
type BrowseResponse struct {
	Path        string         `json:"path"`        // Original path from request
	AliasPath   string         `json:"aliasPath"`   // Alias representation
	FullPath    string         `json:"fullPath"`    // Full GCS/BQ/IAM path
	Type        string         `json:"type"`        // "gcs", "bigquery", "iam"
	Resources   []ResourceItem `json:"resources"`
	Breadcrumbs []Breadcrumb   `json:"breadcrumbs"`
}

// InfoResponse is the response for /api/info
type InfoResponse struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Size       int64             `json:"size"`
	SizeHuman  string            `json:"sizeHuman"`
	Modified   time.Time         `json:"modified"`
	Path       string            `json:"path"`
	AliasPath  string            `json:"aliasPath"`
	Metadata   map[string]string `json:"metadata"`
}

// PreviewResponse is the response for /api/preview
type PreviewResponse struct {
	Name      string `json:"name"`
	Content   string `json:"content"`
	MimeType  string `json:"mimeType"`
	Size      int64  `json:"size"`
	TooLarge  bool   `json:"tooLarge"`
	Error     string `json:"error,omitempty"`
}

// ErrorResponse is the standard error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// HealthResponse is the response for /api/health
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}
