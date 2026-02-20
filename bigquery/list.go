package bigquery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// BQObjectInfo holds information about a BigQuery object (dataset or table)
type BQObjectInfo struct {
	Path        string
	Type        string // "dataset" or "table"
	Created     time.Time
	Modified    time.Time
	Description string
	Location    string
	SizeBytes   int64
	Schema      bigquery.Schema // Table schema (only for tables)
	NumRows     int64           // Number of rows (only for tables)
}

// FormatShort formats BigQuery object info in short format
func (bi *BQObjectInfo) FormatShort() string {
	return bi.Path
}

// FormatShortWithAlias formats BigQuery object info in short format using alias path
func (bi *BQObjectInfo) FormatShortWithAlias(aliasPath string) string {
	return aliasPath
}

// FormatLong formats BigQuery object info in long format
func (bi *BQObjectInfo) FormatLong() string {
	created := formatUnixTime(bi.Created)

	var size string
	if bi.Type == "table" && bi.SizeBytes > 0 {
		size = formatSize(bi.SizeBytes)
	} else {
		size = "-"
	}

	return fmt.Sprintf("%s  %-8s  %-15s  %s", created, bi.Type, size, bi.Path)
}

// FormatLongWithAlias formats BigQuery object info in long format using alias path
func (bi *BQObjectInfo) FormatLongWithAlias(aliasPath string) string {
	var size string
	if bi.Type == "table" && bi.SizeBytes > 0 {
		size = formatSize(bi.SizeBytes)
	} else {
		size = "-"
	}

	var rows string
	if bi.Type == "table" && bi.NumRows > 0 {
		rows = formatNumber(bi.NumRows)
	} else {
		rows = "-"
	}

	return fmt.Sprintf("%-8s  %15s  %20s  %s", bi.Type, size, rows, aliasPath)
}

// FormatLongHeader returns the header for long format listing
func FormatLongHeader() string {
	return fmt.Sprintf("%-8s  %15s  %20s  %s", "TYPE", "SIZE", "ROWS", "PATH")
}

// FormatDetailed formats BigQuery table info with schema details
func (bi *BQObjectInfo) FormatDetailed(aliasPath string) string {
	var output strings.Builder

	// Header information
	output.WriteString(fmt.Sprintf("Table: %s\n", aliasPath))
	if bi.Description != "" {
		output.WriteString(fmt.Sprintf("Description: %s\n", bi.Description))
	}
	output.WriteString(fmt.Sprintf("Created: %s\n", formatUnixTime(bi.Created)))
	output.WriteString(fmt.Sprintf("Modified: %s\n", formatUnixTime(bi.Modified)))
	output.WriteString(fmt.Sprintf("Location: %s\n", bi.Location))

	if bi.SizeBytes > 0 {
		output.WriteString(fmt.Sprintf("Size: %s\n", formatSize(bi.SizeBytes)))
	}
	if bi.NumRows > 0 {
		output.WriteString(fmt.Sprintf("Rows: %s\n", formatNumber(bi.NumRows)))
	}

	// Schema information
	if len(bi.Schema) > 0 {
		output.WriteString("\nSchema:\n")
		for _, field := range bi.Schema {
			output.WriteString(formatSchemaField(field, 0))
		}
	}

	return output.String()
}

// formatSchemaField formats a schema field with proper indentation for nested fields
func formatSchemaField(field *bigquery.FieldSchema, indent int) string {
	var output strings.Builder
	prefix := strings.Repeat("  ", indent)

	// Format the field type
	fieldType := string(field.Type)
	if field.Repeated {
		fieldType = "REPEATED " + fieldType
	}

	// Basic field info
	output.WriteString(fmt.Sprintf("%s- %s (%s)", prefix, field.Name, fieldType))

	if field.Description != "" {
		output.WriteString(fmt.Sprintf(" - %s", field.Description))
	}
	output.WriteString("\n")

	// Nested fields (for STRUCT/RECORD types)
	if len(field.Schema) > 0 {
		for _, nestedField := range field.Schema {
			output.WriteString(formatSchemaField(nestedField, indent+1))
		}
	}

	return output.String()
}

// formatNumber formats a number with thousands separators
func formatNumber(n int64) string {
	s := fmt.Sprintf("%d", n)
	// Add commas every 3 digits from the right
	var result strings.Builder
	for i, digit := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(digit)
	}
	return result.String()
}

// formatSize converts bytes to human-readable format
func formatSize(bytes int64) string {
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

// formatUnixTime formats a time in Unix ls style
// Recent files (< 6 months): "19 Jan. 16:54"
// Older files (>= 6 months): " 2 Jan.  2024"
func formatUnixTime(t time.Time) string {
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

// ListDatasets lists all datasets in a project
func ListDatasets(ctx context.Context, projectID string) ([]*BQObjectInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	var results []*BQObjectInfo
	apilog.Logf("[BQ] Datasets.List(project=%s)", projectID)
	it := client.Datasets(ctx)

	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate datasets: %w", err)
		}

		// Get dataset metadata
		apilog.Logf("[BQ] Dataset.Metadata(project=%s, dataset=%s)", projectID, dataset.DatasetID)
		meta, err := dataset.Metadata(ctx)
		if err != nil {
			// Skip datasets we can't access
			continue
		}

		results = append(results, &BQObjectInfo{
			Path:        fmt.Sprintf("bq://%s.%s", projectID, dataset.DatasetID),
			Type:        "dataset",
			Created:     meta.CreationTime,
			Modified:    meta.LastModifiedTime,
			Description: meta.Description,
			Location:    meta.Location,
		})
	}

	return results, nil
}

// ListTables lists all tables in a dataset
func ListTables(ctx context.Context, projectID, datasetID string) ([]*BQObjectInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	dataset := client.Dataset(datasetID)
	var results []*BQObjectInfo
	apilog.Logf("[BQ] Tables.List(project=%s, dataset=%s)", projectID, datasetID)
	it := dataset.Tables(ctx)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate tables: %w", err)
		}

		// Get table metadata
		apilog.Logf("[BQ] Table.Metadata(project=%s, dataset=%s, table=%s)", projectID, datasetID, table.TableID)
		meta, err := table.Metadata(ctx)
		if err != nil {
			// Skip tables we can't access
			continue
		}

		results = append(results, &BQObjectInfo{
			Path:        fmt.Sprintf("bq://%s.%s.%s", projectID, datasetID, table.TableID),
			Type:        "table",
			Created:     meta.CreationTime,
			Modified:    meta.LastModifiedTime,
			Description: meta.Description,
			Location:    meta.Location,
			SizeBytes:   meta.NumBytes,
			NumRows:     int64(meta.NumRows),
		})
	}

	return results, nil
}

// DescribeTable shows table schema and details
func DescribeTable(ctx context.Context, projectID, datasetID, tableID string) (*BQObjectInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	table := client.Dataset(datasetID).Table(tableID)
	apilog.Logf("[BQ] Table.Metadata(project=%s, dataset=%s, table=%s)", projectID, datasetID, tableID)
	meta, err := table.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	return &BQObjectInfo{
		Path:        fmt.Sprintf("bq://%s.%s.%s", projectID, datasetID, tableID),
		Type:        "table",
		Created:     meta.CreationTime,
		Modified:    meta.LastModifiedTime,
		Description: meta.Description,
		Location:    meta.Location,
		SizeBytes:   meta.NumBytes,
		Schema:      meta.Schema,
		NumRows:     int64(meta.NumRows),
	}, nil
}

// ParseBQPath parses a bq:// path into components
// Examples:
//   bq:// -> ("", "", "") - list datasets in default project
//   bq://project-id -> (project-id, "", "")
//   bq://project-id.dataset -> (project-id, dataset, "")
//   bq://project-id.dataset.table -> (project-id, dataset, table)
func ParseBQPath(bqPath string) (projectID, datasetID, tableID string, err error) {
	if !strings.HasPrefix(bqPath, "bq://") {
		return "", "", "", fmt.Errorf("not a valid BigQuery path: %s", bqPath)
	}

	// Remove bq:// prefix
	pathWithoutPrefix := strings.TrimPrefix(bqPath, "bq://")

	// Empty path means list datasets in default project
	if pathWithoutPrefix == "" {
		return "", "", "", nil
	}

	// Split by dots
	parts := strings.Split(pathWithoutPrefix, ".")

	switch len(parts) {
	case 1:
		// bq://project-id
		projectID = parts[0]
	case 2:
		// bq://project-id.dataset
		projectID = parts[0]
		datasetID = parts[1]
	case 3:
		// bq://project-id.dataset.table
		projectID = parts[0]
		datasetID = parts[1]
		tableID = parts[2]
	default:
		return "", "", "", fmt.Errorf("invalid BigQuery path format: %s", bqPath)
	}

	return projectID, datasetID, tableID, nil
}

// IsBQPath checks if a string is a BigQuery path
func IsBQPath(path string) bool {
	return strings.HasPrefix(path, "bq://")
}
