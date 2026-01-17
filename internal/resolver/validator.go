package resolver

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	// aliasPattern defines valid alias names (alphanumeric, hyphens, underscores)
	aliasPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

	// gcsPathPattern defines valid GCS paths
	gcsPathPattern = regexp.MustCompile(`^gs://[a-z0-9][a-z0-9._-]{1,61}[a-z0-9](/.*)?$`)
)

// ValidateAlias checks if an alias name is valid
func ValidateAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("alias cannot be empty")
	}

	if strings.Contains(alias, "/") {
		return fmt.Errorf("alias %q cannot contain '/'", alias)
	}

	if strings.Contains(alias, "..") {
		return fmt.Errorf("alias %q cannot contain '..'", alias)
	}

	if strings.HasPrefix(alias, ".") {
		return fmt.Errorf("alias %q cannot start with '.'", alias)
	}

	if !aliasPattern.MatchString(alias) {
		return fmt.Errorf("alias %q contains invalid characters (only alphanumeric, hyphens, and underscores allowed)", alias)
	}

	// Reserved words that might conflict with commands
	reserved := []string{"map", "ls", "cp", "rm", "mv", "server", "help", "version"}
	for _, r := range reserved {
		if alias == r {
			return fmt.Errorf("alias %q is a reserved word", alias)
		}
	}

	return nil
}

// ValidateGCSPath checks if a GCS path is valid
func ValidateGCSPath(path string) error {
	if path == "" {
		return fmt.Errorf("GCS path cannot be empty")
	}

	if !strings.HasPrefix(path, "gs://") {
		return fmt.Errorf("GCS path must start with 'gs://'")
	}

	if path == "gs://" {
		return fmt.Errorf("GCS path must include a bucket name")
	}

	// Extract bucket name (everything between gs:// and first /)
	pathWithoutPrefix := strings.TrimPrefix(path, "gs://")
	parts := strings.SplitN(pathWithoutPrefix, "/", 2)
	bucketName := parts[0]

	// Validate bucket name according to GCS rules
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return fmt.Errorf("bucket name must be between 3 and 63 characters")
	}

	if !regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*[a-z0-9]$`).MatchString(bucketName) {
		return fmt.Errorf("invalid bucket name %q", bucketName)
	}

	if strings.Contains(bucketName, "..") {
		return fmt.Errorf("bucket name cannot contain '..'")
	}

	return nil
}

// NormalizePath ensures a GCS path ends with / for consistent mapping
func NormalizePath(path string) string {
	// Trim any trailing whitespace
	path = strings.TrimSpace(path)

	// Ensure it ends with / for directory-style access
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	return path
}
