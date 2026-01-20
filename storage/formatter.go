package storage

import (
	"fmt"
	"strings"
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

// FormatUnixTime formats a time in Unix ls style
// Recent files (< 6 months): "19 Jan. 16:54"
// Older files (>= 6 months): " 2 Jan.  2024"
func FormatUnixTime(t time.Time) string {
	now := time.Now()
	sixMonthsAgo := now.AddDate(0, -6, 0)

	// Get month abbreviation with period
	month := t.Format("Jan")
	month = strings.TrimSuffix(month, ".") + "."

	if t.After(sixMonthsAgo) {
		// Recent file: show time
		// Format: " 2 Jan. 16:54" or "19 Jan. 16:54"
		return fmt.Sprintf("%2d %s %s", t.Day(), month, t.Format("15:04"))
	}

	// Old file: show year
	// Format: " 2 Jan.  2024" or "19 Jan.  2024"
	return fmt.Sprintf("%2d %s %5d", t.Day(), month, t.Year())
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

// FormatLong formats object info in long format (matching Unix ls -l)
func (oi *ObjectInfo) FormatLong(humanReadable bool) string {
	if oi.IsPrefix {
		// Prefixes (directories) don't have size or timestamp
		return oi.Path
	}

	timestamp := FormatUnixTime(oi.Updated)

	var size string
	if humanReadable {
		size = FormatSize(oi.Size)
		// Pad size to align columns (10 chars for human-readable)
		size = fmt.Sprintf("%10s", size)
	} else {
		// Pad size to align columns (12 chars for raw bytes)
		size = fmt.Sprintf("%12d", oi.Size)
	}

	return fmt.Sprintf("%s  %s  %s", size, timestamp, oi.Path)
}

// FormatLongWithAlias formats object info in long format with alias substitution
func (oi *ObjectInfo) FormatLongWithAlias(humanReadable bool, aliasPath string) string {
	if oi.IsPrefix {
		if aliasPath != "" {
			return aliasPath
		}
		return oi.Path
	}

	timestamp := FormatUnixTime(oi.Updated)

	var size string
	if humanReadable {
		size = FormatSize(oi.Size)
		// Pad size to align columns (10 chars for human-readable)
		size = fmt.Sprintf("%10s", size)
	} else {
		// Pad size to align columns (12 chars for raw bytes)
		size = fmt.Sprintf("%12d", oi.Size)
	}

	displayPath := oi.Path
	if aliasPath != "" {
		displayPath = aliasPath
	}

	return fmt.Sprintf("%s  %s  %s", size, timestamp, displayPath)
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
