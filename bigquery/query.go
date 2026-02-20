package bigquery

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/olekukonko/tablewriter"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// QueryResult holds the results of a BigQuery query execution
type QueryResult struct {
	Schema         bigquery.Schema
	Rows           [][]bigquery.Value
	TotalRows      uint64
	JobID          string
	BytesProcessed int64
	CacheHit       bool
	ExecutionTime  time.Duration
}

// QueryStats contains statistics about query execution
type QueryStats struct {
	RowCount       uint64
	BytesProcessed int64
	CacheHit       bool
	ExecutionTime  time.Duration
}

// ExecuteQuery runs a BigQuery SQL query and returns the results
func ExecuteQuery(ctx context.Context, projectID, sql string, maxResults int) (*QueryResult, error) {
	startTime := time.Now()

	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get BigQuery client: %w", err)
	}

	query := client.Query(sql)

	apilog.Logf("[BQ] Query.Run(project=%s)", projectID)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("query job failed: %w", err)
	}
	if status.Err() != nil {
		return nil, fmt.Errorf("query error: %w", status.Err())
	}

	executionTime := time.Since(startTime)

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read query results: %w", err)
	}

	// Read all rows
	var rows [][]bigquery.Value
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		rows = append(rows, row)
	}

	// Get cache hit information from query statistics
	var cacheHit bool
	if queryStats, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
		cacheHit = queryStats.CacheHit
	}

	return &QueryResult{
		Schema:         it.Schema,
		Rows:           rows,
		TotalRows:      it.TotalRows,
		JobID:          job.ID(),
		BytesProcessed: status.Statistics.TotalBytesProcessed,
		CacheHit:       cacheHit,
		ExecutionTime:  executionTime,
	}, nil
}

// DryRunQuery validates a query without executing it
func DryRunQuery(ctx context.Context, projectID, sql string) (int64, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return 0, fmt.Errorf("failed to get BigQuery client: %w", err)
	}

	query := client.Query(sql)
	query.DryRun = true

	apilog.Logf("[BQ] Query.Run(project=%s, dry_run=true)", projectID)
	job, err := query.Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("query validation failed: %w", err)
	}

	status, err := job.Status(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get job status: %w", err)
	}

	if status.Err() != nil {
		return 0, fmt.Errorf("query validation error: %w", status.Err())
	}

	return status.Statistics.TotalBytesProcessed, nil
}

// FormatQueryResultTable formats query results as an ASCII table
func FormatQueryResultTable(result *QueryResult, w io.Writer) error {
	if len(result.Rows) == 0 {
		fmt.Fprintln(w, "(No rows returned)")
		return nil
	}

	table := tablewriter.NewWriter(w)

	// Set headers from schema
	headers := make([]interface{}, len(result.Schema))
	for i, field := range result.Schema {
		headers[i] = field.Name
	}
	table.Header(headers...)

	// Add rows
	for _, row := range result.Rows {
		rowData := make([]interface{}, len(row))
		for i, val := range row {
			rowData[i] = formatValue(val)
		}
		table.Append(rowData...)
	}

	table.Render()
	return nil
}

// FormatQueryResultJSON formats query results as JSON array
func FormatQueryResultJSON(result *QueryResult, w io.Writer) error {
	if len(result.Rows) == 0 {
		fmt.Fprintln(w, "[]")
		return nil
	}

	// Convert rows to array of objects
	rows := make([]map[string]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		obj := make(map[string]interface{})
		for i, field := range result.Schema {
			if i < len(row) {
				obj[field.Name] = row[i]
			}
		}
		rows = append(rows, obj)
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(rows)
}

// FormatQueryResultCSV formats query results as CSV
func FormatQueryResultCSV(result *QueryResult, w io.Writer) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Write header row
	headers := make([]string, len(result.Schema))
	for i, field := range result.Schema {
		headers[i] = field.Name
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for _, row := range result.Rows {
		rowStrings := make([]string, len(row))
		for i, val := range row {
			rowStrings[i] = formatValue(val)
		}
		if err := writer.Write(rowStrings); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

// GetStats returns query statistics
func (qr *QueryResult) GetStats() QueryStats {
	return QueryStats{
		RowCount:       uint64(len(qr.Rows)),
		BytesProcessed: qr.BytesProcessed,
		CacheHit:       qr.CacheHit,
		ExecutionTime:  qr.ExecutionTime,
	}
}

// formatValue converts a BigQuery value to a string for display
func formatValue(val bigquery.Value) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case string:
		return v
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case time.Time:
		return v.Format(time.RFC3339)
	case []bigquery.Value:
		// Handle arrays
		strs := make([]string, len(v))
		for i, item := range v {
			strs[i] = formatValue(item)
		}
		return "[" + strings.Join(strs, ", ") + "]"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// FormatBytes formats bytes in human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatDuration formats duration in human-readable format
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.0fms", d.Seconds()*1000)
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// PrintSchema prints a BigQuery schema with proper indentation
func PrintSchema(schema bigquery.Schema, indent int) {
	for _, field := range schema {
		printSchemaField(field, indent)
	}
}

// printSchemaField prints a schema field with proper indentation for nested fields
func printSchemaField(field *bigquery.FieldSchema, indent int) {
	prefix := strings.Repeat("  ", indent)

	// Format the field type
	fieldType := string(field.Type)
	if field.Repeated {
		fieldType = "REPEATED " + fieldType
	}

	// Basic field info
	fmt.Printf("%s- %s (%s)", prefix, field.Name, fieldType)

	if field.Description != "" {
		fmt.Printf(" - %s", field.Description)
	}
	fmt.Println()

	// Nested fields (for STRUCT/RECORD types)
	if len(field.Schema) > 0 {
		for _, nestedField := range field.Schema {
			printSchemaField(nestedField, indent+1)
		}
	}
}
