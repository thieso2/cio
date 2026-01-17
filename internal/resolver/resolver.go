package resolver

import (
	"fmt"
	"strings"

	"github.com/thies/cio/internal/config"
)

// Resolver handles alias-to-path resolution
type Resolver struct {
	config *config.Config
}

// New creates a new Resolver instance
func New(cfg *config.Config) *Resolver {
	return &Resolver{
		config: cfg,
	}
}

// Resolve converts an alias path to a full GCS path
// Input: "am/2024/01/data.txt"
// Output: "gs://io-spooler-onprem-archived-metrics/2024/01/data.txt"
func (r *Resolver) Resolve(aliasPath string) (string, error) {
	if aliasPath == "" {
		return "", fmt.Errorf("alias path cannot be empty")
	}

	// If already a gs:// path, return as-is
	if strings.HasPrefix(aliasPath, "gs://") {
		return aliasPath, nil
	}

	// Split by first "/" to get alias and suffix
	var alias, suffix string
	if idx := strings.Index(aliasPath, "/"); idx != -1 {
		alias = aliasPath[:idx]
		suffix = aliasPath[idx+1:] // Skip the "/"
	} else {
		alias = aliasPath
		suffix = ""
	}

	// Look up the alias in config
	basePath, exists := r.config.GetMapping(alias)
	if !exists {
		return "", fmt.Errorf("alias %q not found (run 'cio map list' to see available mappings)", alias)
	}

	// Ensure base path ends with /
	basePath = NormalizePath(basePath)

	// Join base path with suffix
	fullPath := basePath + suffix

	return fullPath, nil
}

// ResolveAlias gets just the alias part from a path
func (r *Resolver) ResolveAlias(aliasPath string) (string, error) {
	if aliasPath == "" {
		return "", fmt.Errorf("alias path cannot be empty")
	}

	// If already a gs:// path, return error
	if strings.HasPrefix(aliasPath, "gs://") {
		return "", fmt.Errorf("path is already a GCS path")
	}

	// Extract alias (everything before first /)
	if idx := strings.Index(aliasPath, "/"); idx != -1 {
		return aliasPath[:idx], nil
	}

	return aliasPath, nil
}

// GetBasePath returns the base GCS path for an alias
func (r *Resolver) GetBasePath(alias string) (string, error) {
	path, exists := r.config.GetMapping(alias)
	if !exists {
		return "", fmt.Errorf("alias %q not found", alias)
	}
	return path, nil
}

// ListAliases returns all available aliases
func (r *Resolver) ListAliases() map[string]string {
	return r.config.ListMappings()
}

// ParseGCSPath parses a gs:// path into bucket and object components
func ParseGCSPath(gcsPath string) (bucket, object string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("not a valid GCS path: %s", gcsPath)
	}

	// Remove gs:// prefix
	pathWithoutPrefix := strings.TrimPrefix(gcsPath, "gs://")

	// Split into bucket and object
	parts := strings.SplitN(pathWithoutPrefix, "/", 2)
	bucket = parts[0]

	if len(parts) > 1 {
		object = parts[1]
	}

	return bucket, object, nil
}

// IsGCSPath checks if a string is a GCS path
func IsGCSPath(path string) bool {
	return strings.HasPrefix(path, "gs://")
}
