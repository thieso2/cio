package bigquery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
	bqapi "google.golang.org/api/bigquery/v2"
)

// BQObjectInfo holds information about a BigQuery object (dataset or table)
type BQObjectInfo struct {
	Path        string          `json:"path"`
	Type        string          `json:"type"`
	Created     time.Time       `json:"created"`
	Modified    time.Time       `json:"modified"`
	Description string          `json:"description,omitempty"`
	Location    string          `json:"location,omitempty"`
	SizeBytes   int64           `json:"size_bytes,omitempty"`
	Schema      bigquery.Schema `json:"-"`
	NumRows     int64           `json:"num_rows,omitempty"`
	ViewQuery   string          `json:"view_query,omitempty"`

	// Storage info (from INFORMATION_SCHEMA.TABLE_STORAGE)
	NumPartitions           int64 `json:"num_partitions,omitempty"`
	TotalLogicalBytes       int64 `json:"total_logical_bytes,omitempty"`
	ActiveLogicalBytes      int64 `json:"active_logical_bytes,omitempty"`
	LongTermLogicalBytes    int64 `json:"long_term_logical_bytes,omitempty"`
	CurrentPhysicalBytes    int64 `json:"current_physical_bytes,omitempty"`
	TotalPhysicalBytes      int64 `json:"total_physical_bytes,omitempty"`
	ActivePhysicalBytes     int64 `json:"active_physical_bytes,omitempty"`
	LongTermPhysicalBytes   int64 `json:"long_term_physical_bytes,omitempty"`
	TimeTravelPhysicalBytes int64 `json:"time_travel_physical_bytes,omitempty"`
	HasStorageInfo          bool  `json:"-"`
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
	if bi.Type != "dataset" && bi.SizeBytes > 0 {
		size = formatSize(bi.SizeBytes)
	} else {
		size = "-"
	}

	return fmt.Sprintf("%s  %-18s  %-15s  %s", created, bi.Type, size, bi.Path)
}

// FormatLongWithAlias formats BigQuery object info in long format using alias path
func (bi *BQObjectInfo) FormatLongWithAlias(aliasPath string) string {
	var size string
	if bi.Type != "dataset" && bi.SizeBytes > 0 {
		size = formatSize(bi.SizeBytes)
	} else {
		size = "-"
	}

	var rows string
	if bi.Type != "dataset" && bi.NumRows > 0 {
		rows = formatNumber(bi.NumRows)
	} else {
		rows = "-"
	}

	return fmt.Sprintf("%-18s  %15s  %20s  %s", bi.Type, size, rows, aliasPath)
}

// FormatLongHeader returns the header for long format listing
func FormatLongHeader() string {
	return fmt.Sprintf("%-18s  %15s  %20s  %s", "TYPE", "SIZE", "ROWS", "PATH")
}

// FormatDetailed formats BigQuery table info with schema details
func (bi *BQObjectInfo) FormatDetailed(aliasPath string) string {
	var output strings.Builder

	// Header - use type-specific label
	label := strings.ToUpper(bi.Type[:1]) + bi.Type[1:]
	fmt.Fprintf(&output, "%s: %s\n", label, aliasPath)
	if bi.Description != "" {
		fmt.Fprintf(&output, "Description: %s\n", bi.Description)
	}
	fmt.Fprintf(&output, "Created: %s\n", formatUnixTime(bi.Created))
	fmt.Fprintf(&output, "Modified: %s\n", formatUnixTime(bi.Modified))
	fmt.Fprintf(&output, "Location: %s\n", bi.Location)

	// For views: show the SQL query
	if bi.ViewQuery != "" {
		fmt.Fprintf(&output, "\nView SQL:\n%s\n", bi.ViewQuery)
	}

	// For tables: show storage info
	if bi.Type == "table" || bi.Type == "materialized_view" {
		if bi.HasStorageInfo {
			output.WriteString("\nStorage info:\n")
			fmt.Fprintf(&output, "  %-30s %s\n", "Number of rows", formatNumber(bi.NumRows))
			if bi.NumPartitions > 0 {
				fmt.Fprintf(&output, "  %-30s %s\n", "Number of partitions", formatNumber(bi.NumPartitions))
			}
			fmt.Fprintf(&output, "  %-30s %s\n", "Total logical bytes", formatSize(bi.TotalLogicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Active logical bytes", formatSize(bi.ActiveLogicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Long term logical bytes", formatSize(bi.LongTermLogicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Current physical bytes", formatSize(bi.CurrentPhysicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Total physical bytes", formatSize(bi.TotalPhysicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Active physical bytes", formatSize(bi.ActivePhysicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Long term physical bytes", formatSize(bi.LongTermPhysicalBytes))
			fmt.Fprintf(&output, "  %-30s %s\n", "Time travel physical bytes", formatSize(bi.TimeTravelPhysicalBytes))
		} else {
			if bi.SizeBytes > 0 {
				fmt.Fprintf(&output, "Size: %s\n", formatSize(bi.SizeBytes))
			}
			if bi.NumRows > 0 {
				fmt.Fprintf(&output, "Rows: %s\n", formatNumber(bi.NumRows))
			}
		}
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

// SchemaFieldJSON is a JSON-serializable representation of a BigQuery schema field.
type SchemaFieldJSON struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Description string            `json:"description,omitempty"`
	Repeated    bool              `json:"repeated,omitempty"`
	Fields      []SchemaFieldJSON `json:"fields,omitempty"`
}

// InfoJSON is a JSON-serializable representation of BQObjectInfo.
type InfoJSON struct {
	Path        string `json:"path"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Created     string `json:"created"`
	Modified    string `json:"modified"`
	Location    string `json:"location"`

	// View-specific
	ViewQuery string `json:"view_query,omitempty"`

	// Storage info (tables only)
	NumRows                 int64 `json:"num_rows,omitempty"`
	NumPartitions           int64 `json:"num_partitions,omitempty"`
	TotalLogicalBytes       int64 `json:"total_logical_bytes,omitempty"`
	ActiveLogicalBytes      int64 `json:"active_logical_bytes,omitempty"`
	LongTermLogicalBytes    int64 `json:"long_term_logical_bytes,omitempty"`
	CurrentPhysicalBytes    int64 `json:"current_physical_bytes,omitempty"`
	TotalPhysicalBytes      int64 `json:"total_physical_bytes,omitempty"`
	ActivePhysicalBytes     int64 `json:"active_physical_bytes,omitempty"`
	LongTermPhysicalBytes   int64 `json:"long_term_physical_bytes,omitempty"`
	TimeTravelPhysicalBytes int64 `json:"time_travel_physical_bytes,omitempty"`

	Schema []SchemaFieldJSON `json:"schema,omitempty"`
}

// ToJSON converts BQObjectInfo to a JSON-serializable struct.
func (bi *BQObjectInfo) ToJSON(aliasPath string) *InfoJSON {
	j := &InfoJSON{
		Path:        aliasPath,
		Type:        bi.Type,
		Description: bi.Description,
		Created:     bi.Created.Format(time.RFC3339),
		Modified:    bi.Modified.Format(time.RFC3339),
		Location:    bi.Location,
		ViewQuery:   bi.ViewQuery,
	}

	if bi.HasStorageInfo {
		j.NumRows = bi.NumRows
		j.NumPartitions = bi.NumPartitions
		j.TotalLogicalBytes = bi.TotalLogicalBytes
		j.ActiveLogicalBytes = bi.ActiveLogicalBytes
		j.LongTermLogicalBytes = bi.LongTermLogicalBytes
		j.CurrentPhysicalBytes = bi.CurrentPhysicalBytes
		j.TotalPhysicalBytes = bi.TotalPhysicalBytes
		j.ActivePhysicalBytes = bi.ActivePhysicalBytes
		j.LongTermPhysicalBytes = bi.LongTermPhysicalBytes
		j.TimeTravelPhysicalBytes = bi.TimeTravelPhysicalBytes
	} else if bi.Type == "table" || bi.Type == "materialized_view" {
		j.NumRows = bi.NumRows
		j.TotalLogicalBytes = bi.SizeBytes
	}

	if len(bi.Schema) > 0 {
		j.Schema = convertSchema(bi.Schema)
	}

	return j
}

func convertSchema(fields bigquery.Schema) []SchemaFieldJSON {
	result := make([]SchemaFieldJSON, len(fields))
	for i, f := range fields {
		result[i] = SchemaFieldJSON{
			Name:        f.Name,
			Type:        string(f.Type),
			Description: f.Description,
			Repeated:    f.Repeated,
		}
		if len(f.Schema) > 0 {
			result[i].Fields = convertSchema(f.Schema)
		}
	}
	return result
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

const metadataWorkers = 8

// ListTables lists all tables in a dataset, fetching metadata in parallel.
func ListTables(ctx context.Context, projectID, datasetID string) ([]*BQObjectInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	dataset := client.Dataset(datasetID)
	apilog.Logf("[BQ] Tables.List(project=%s, dataset=%s)", projectID, datasetID)
	it := dataset.Tables(ctx)

	// Collect all table handles first (fast — no API call per table).
	type tableHandle struct {
		table *bigquery.Table
	}
	var handles []tableHandle
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate tables: %w", err)
		}
		handles = append(handles, tableHandle{table: table})
	}

	// Fan-out: fetch metadata in parallel with a bounded worker pool.
	type result struct {
		info *BQObjectInfo
	}

	jobs := make(chan tableHandle, len(handles))
	results := make(chan result, len(handles))

	workers := metadataWorkers
	if len(handles) < workers {
		workers = len(handles)
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range jobs {
				apilog.Logf("[BQ] Table.Metadata(project=%s, dataset=%s, table=%s)", projectID, datasetID, h.table.TableID)
				meta, err := h.table.Metadata(ctx)
				if err != nil {
					// Skip tables we can't access.
					continue
				}
				results <- result{
					info: &BQObjectInfo{
						Path:        fmt.Sprintf("bq://%s.%s.%s", projectID, datasetID, h.table.TableID),
						Type:        bqTableType(meta.Type),
						Created:     meta.CreationTime,
						Modified:    meta.LastModifiedTime,
						Description: meta.Description,
						Location:    meta.Location,
						SizeBytes:   meta.NumBytes,
						NumRows:     int64(meta.NumRows),
					},
				}
			}
		}()
	}

	for _, h := range handles {
		jobs <- h
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var infos []*BQObjectInfo
	for r := range results {
		infos = append(infos, r.info)
	}

	// Sort by path to keep output deterministic.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Path < infos[j].Path
	})

	return infos, nil
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
		Type:        bqTableType(meta.Type),
		Created:     meta.CreationTime,
		Modified:    meta.LastModifiedTime,
		Description: meta.Description,
		Location:    meta.Location,
		SizeBytes:   meta.NumBytes,
		Schema:      meta.Schema,
		NumRows:     int64(meta.NumRows),
		ViewQuery:   meta.ViewQuery,
	}, nil
}

// FetchStorageInfo uses the BigQuery REST API to get detailed storage stats
// (physical bytes, partitions, etc.) and populates the storage fields on BQObjectInfo.
func FetchStorageInfo(ctx context.Context, projectID, datasetID, tableID string, info *BQObjectInfo) error {
	apilog.Logf("[BQ] REST Tables.Get(project=%s, dataset=%s, table=%s)", projectID, datasetID, tableID)

	svc, err := bqapi.NewService(ctx)
	if err != nil {
		return err
	}

	table, err := svc.Tables.Get(projectID, datasetID, tableID).Context(ctx).Do()
	if err != nil {
		return err
	}

	info.NumRows = int64(table.NumRows)
	info.NumPartitions = table.NumPartitions
	info.TotalLogicalBytes = table.NumTotalLogicalBytes
	info.ActiveLogicalBytes = table.NumActiveLogicalBytes
	info.LongTermLogicalBytes = table.NumLongTermLogicalBytes
	info.CurrentPhysicalBytes = table.NumCurrentPhysicalBytes
	info.TotalPhysicalBytes = table.NumTotalPhysicalBytes
	info.ActivePhysicalBytes = table.NumActivePhysicalBytes
	info.LongTermPhysicalBytes = table.NumLongTermPhysicalBytes
	info.TimeTravelPhysicalBytes = table.NumTimeTravelPhysicalBytes
	info.HasStorageInfo = true

	return nil
}

// bqTableType converts a BigQuery TableType to a lowercase display string.
func bqTableType(t bigquery.TableType) string {
	switch t {
	case bigquery.ViewTable:
		return "view"
	case bigquery.MaterializedView:
		return "materialized_view"
	case bigquery.ExternalTable:
		return "external"
	case bigquery.Snapshot:
		return "snapshot"
	default:
		return "table"
	}
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
