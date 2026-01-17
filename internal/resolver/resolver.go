package resolver

import (
	"fmt"
	"strings"

	"github.com/thieso2/cio/internal/config"
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

// Resolve converts an alias path to a full GCS/BigQuery path
// Input: ":am/2024/01/data.txt"
// Output: "gs://io-spooler-onprem-archived-metrics/2024/01/data.txt"
// or: "bq://project-id.dataset"
func (r *Resolver) Resolve(aliasPath string) (string, error) {
	if aliasPath == "" {
		return "", fmt.Errorf("alias path cannot be empty")
	}

	// If already a gs:// or bq:// path, return as-is
	if strings.HasPrefix(aliasPath, "gs://") || strings.HasPrefix(aliasPath, "bq://") {
		return aliasPath, nil
	}

	// Check if it starts with : (alias prefix)
	if !strings.HasPrefix(aliasPath, ":") {
		return "", fmt.Errorf("alias paths must start with ':' (e.g., ':am/path')")
	}

	// Remove the : prefix
	aliasPath = strings.TrimPrefix(aliasPath, ":")

	// Split by first "/" or "." to get alias and suffix
	var alias, suffix string
	slashIdx := strings.Index(aliasPath, "/")
	dotIdx := strings.Index(aliasPath, ".")

	// Find the first separator (/ or .)
	var sepIdx int
	if slashIdx != -1 && (dotIdx == -1 || slashIdx < dotIdx) {
		sepIdx = slashIdx
	} else if dotIdx != -1 {
		sepIdx = dotIdx
	} else {
		sepIdx = -1
	}

	if sepIdx != -1 {
		alias = aliasPath[:sepIdx]
		suffix = aliasPath[sepIdx+1:] // Skip the separator
	} else {
		alias = aliasPath
		suffix = ""
	}

	// Look up the alias in config
	basePath, exists := r.config.GetMapping(alias)
	if !exists {
		return "", fmt.Errorf("alias %q not found (run 'cio map list' to see available mappings)", alias)
	}

	// Handle path joining based on type
	var fullPath string
	if strings.HasPrefix(basePath, "bq://") {
		// BigQuery path - use dot separator
		if suffix != "" {
			fullPath = basePath + "." + suffix
		} else {
			fullPath = basePath
		}
	} else {
		// GCS path - use slash separator and normalize
		basePath = NormalizePath(basePath)
		fullPath = basePath + suffix
	}

	return fullPath, nil
}

// ResolveAlias gets just the alias part from a path (without : prefix)
func (r *Resolver) ResolveAlias(aliasPath string) (string, error) {
	if aliasPath == "" {
		return "", fmt.Errorf("alias path cannot be empty")
	}

	// If already a gs:// path, return error
	if strings.HasPrefix(aliasPath, "gs://") {
		return "", fmt.Errorf("path is already a GCS path")
	}

	// Check if it starts with : (alias prefix)
	if !strings.HasPrefix(aliasPath, ":") {
		return "", fmt.Errorf("alias paths must start with ':'")
	}

	// Remove the : prefix
	aliasPath = strings.TrimPrefix(aliasPath, ":")

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
// Special cases:
//   - gs:// -> bucket="", object="" (list all buckets in default project)
//   - gs://project-id: -> bucket="project-id:", object="" (list buckets in specific project)
//   - gs://project-id:bucket-name/ -> bucket="bucket-name", object="" (project-id prefix is stripped)
//   - gs://bucket/ -> bucket="bucket", object="" (list objects in bucket)
//   - gs://bucket/obj -> bucket="bucket", object="obj"
func ParseGCSPath(gcsPath string) (bucket, object string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("not a valid GCS path: %s", gcsPath)
	}

	// Remove gs:// prefix
	pathWithoutPrefix := strings.TrimPrefix(gcsPath, "gs://")

	// Empty path means list all buckets
	if pathWithoutPrefix == "" {
		return "", "", nil
	}

	// Split into bucket and object
	parts := strings.SplitN(pathWithoutPrefix, "/", 2)
	bucket = parts[0]

	if len(parts) > 1 {
		object = parts[1]
	}

	// Check if bucket contains a colon (project-id prefix)
	// If it ends with ":", it's a bucket listing command (gs://project-id:)
	// If it contains ":" but doesn't end with it, strip the project-id prefix
	if strings.Contains(bucket, ":") && !strings.HasSuffix(bucket, ":") {
		// Extract just the bucket name after the colon
		colonIdx := strings.Index(bucket, ":")
		bucket = bucket[colonIdx+1:]
	}

	return bucket, object, nil
}

// IsGCSPath checks if a string is a GCS path
func IsGCSPath(path string) bool {
	return strings.HasPrefix(path, "gs://")
}

// ReverseResolve converts a GCS/BigQuery path back to an alias path
// Returns the alias path with : prefix if a matching alias exists, otherwise returns the original path
func (r *Resolver) ReverseResolve(fullPath string) string {
	if !IsGCSPath(fullPath) && !IsBQPath(fullPath) {
		return fullPath
	}

	// Try to find a matching alias
	for alias, basePath := range r.config.ListMappings() {
		if strings.HasPrefix(basePath, "bq://") {
			// BigQuery path - match with dot separator
			if strings.HasPrefix(fullPath, basePath) {
				suffix := strings.TrimPrefix(fullPath, basePath)
				suffix = strings.TrimPrefix(suffix, ".")
				if suffix == "" {
					return ":" + alias
				}
				return ":" + alias + "." + suffix
			}
		} else {
			// GCS path - match with slash separator
			basePath = NormalizePath(basePath)
			if strings.HasPrefix(fullPath, basePath) {
				suffix := strings.TrimPrefix(fullPath, basePath)
				if suffix == "" {
					return ":" + alias
				}
				return ":" + alias + "/" + suffix
			}
		}
	}

	// No matching alias found, return original path
	return fullPath
}

// IsBQPath checks if a string is a BigQuery path
func IsBQPath(path string) bool {
	return strings.HasPrefix(path, "bq://")
}

// GetAliasForInput extracts the alias from user input if one was used
// Returns the alias (without : prefix) and true if an alias was used, empty string and false otherwise
func (r *Resolver) GetAliasForInput(input string) (string, bool) {
	if IsGCSPath(input) {
		return "", false
	}

	// Check if it starts with : (alias prefix)
	if !strings.HasPrefix(input, ":") {
		return "", false
	}

	// Remove the : prefix
	input = strings.TrimPrefix(input, ":")

	// Extract alias (everything before first /)
	if idx := strings.Index(input, "/"); idx != -1 {
		alias := input[:idx]
		if _, exists := r.config.GetMapping(alias); exists {
			return alias, true
		}
	} else {
		// No slash, check if the whole thing is an alias
		if _, exists := r.config.GetMapping(input); exists {
			return input, true
		}
	}

	return "", false
}
