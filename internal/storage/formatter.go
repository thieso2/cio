package storage

import (
	"fmt"
	"time"

	"cloud.google.com/go/storage"
)

// FormatSize converts bytes to human-readable format
func FormatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}

// ObjectInfo holds information about a GCS object or prefix
type ObjectInfo struct {
	Path         string
	Size         int64
	Updated      time.Time
	IsPrefix     bool
	ContentType  string
	StorageClass string
}

// FormatShort formats object info in short format (just the path)
func (oi *ObjectInfo) FormatShort() string {
	return oi.Path
}

// FormatShortWithAlias formats object info in short format with alias substitution
func (oi *ObjectInfo) FormatShortWithAlias(aliasPath string) string {
	if aliasPath != "" {
		return aliasPath
	}
	return oi.Path
}

// FormatLong formats object info in long format (matching gcloud storage ls -l)
func (oi *ObjectInfo) FormatLong(humanReadable bool) string {
	if oi.IsPrefix {
		// Prefixes (directories) don't have size or timestamp in gcloud format
		return oi.Path
	}

	timestamp := oi.Updated.Format(time.RFC3339)

	var size string
	if humanReadable {
		size = FormatSize(oi.Size)
		// Pad size to align columns (15 chars)
		size = fmt.Sprintf("%-15s", size)
	} else {
		// Pad size to align columns (15 chars)
		size = fmt.Sprintf("%-15d", oi.Size)
	}

	return fmt.Sprintf("%s  %s  %s", timestamp, size, oi.Path)
}

// FormatLongWithAlias formats object info in long format with alias substitution
func (oi *ObjectInfo) FormatLongWithAlias(humanReadable bool, aliasPath string) string {
	if oi.IsPrefix {
		if aliasPath != "" {
			return aliasPath
		}
		return oi.Path
	}

	timestamp := oi.Updated.Format(time.RFC3339)

	var size string
	if humanReadable {
		size = FormatSize(oi.Size)
		// Pad size to align columns (15 chars)
		size = fmt.Sprintf("%-15s", size)
	} else {
		// Pad size to align columns (15 chars)
		size = fmt.Sprintf("%-15d", oi.Size)
	}

	displayPath := oi.Path
	if aliasPath != "" {
		displayPath = aliasPath
	}

	return fmt.Sprintf("%s  %s  %s", timestamp, size, displayPath)
}

// CreateObjectInfo creates ObjectInfo from a storage.ObjectAttrs
func CreateObjectInfo(attrs *storage.ObjectAttrs, bucketName string) *ObjectInfo {
	return &ObjectInfo{
		Path:         fmt.Sprintf("gs://%s/%s", bucketName, attrs.Name),
		Size:         attrs.Size,
		Updated:      attrs.Updated,
		IsPrefix:     false,
		ContentType:  attrs.ContentType,
		StorageClass: attrs.StorageClass,
	}
}

// CreatePrefixInfo creates ObjectInfo for a prefix (directory)
func CreatePrefixInfo(prefix, bucketName string) *ObjectInfo {
	gcsPath := fmt.Sprintf("gs://%s/%s", bucketName, prefix)
	return &ObjectInfo{
		Path:     gcsPath,
		IsPrefix: true,
	}
}
