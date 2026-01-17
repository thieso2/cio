package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/internal/bigquery"
	"github.com/thieso2/cio/internal/resolver"
)

// BigQueryResource implements the Resource interface for BigQuery
type BigQueryResource struct {
	formatter PathFormatter
}

// CreateBigQueryResource creates a new BigQuery resource handler
func CreateBigQueryResource(formatter PathFormatter) *BigQueryResource {
	return &BigQueryResource{
		formatter: formatter,
	}
}

// Type returns the resource type
func (b *BigQueryResource) Type() Type {
	return TypeBigQuery
}

// List lists BigQuery datasets/tables at the given path
func (b *BigQueryResource) List(ctx context.Context, path string, options *ListOptions) ([]*ResourceInfo, error) {
	projectID, datasetID, tableID, err := bigquery.ParseBQPath(path)
	if err != nil {
		return nil, err
	}

	// If no project ID specified in path, use from options
	if projectID == "" && options != nil {
		projectID = options.ProjectID
	}

	// Check if we have a project ID
	if projectID == "" {
		return nil, fmt.Errorf("project ID required for BigQuery operations. Use 'bq://project-id' or set project_id in config")
	}

	var bqObjects []*bigquery.BQObjectInfo

	// Handle table listing with wildcards
	if tableID != "" && (options.Pattern != "" || resolver.HasWildcard(tableID)) {
		pattern := options.Pattern
		if pattern == "" {
			pattern = tableID
		}

		allTables, err := bigquery.ListTables(ctx, projectID, datasetID)
		if err != nil {
			return nil, err
		}

		// Filter tables by pattern
		for _, table := range allTables {
			if idx := strings.LastIndex(table.Path, "."); idx != -1 {
				tableName := table.Path[idx+1:]
				if resolver.MatchPattern(tableName, pattern) {
					bqObjects = append(bqObjects, table)
				}
			}
		}
	} else if tableID != "" {
		// Describe specific table
		obj, err := bigquery.DescribeTable(ctx, projectID, datasetID, tableID)
		if err != nil {
			return nil, err
		}
		bqObjects = []*bigquery.BQObjectInfo{obj}
	} else if datasetID != "" {
		// List tables in dataset
		bqObjects, err = bigquery.ListTables(ctx, projectID, datasetID)
		if err != nil {
			return nil, err
		}
	} else {
		// List datasets in project
		bqObjects, err = bigquery.ListDatasets(ctx, projectID)
		if err != nil {
			return nil, err
		}
	}

	// Convert to ResourceInfo
	result := make([]*ResourceInfo, len(bqObjects))
	for i, obj := range bqObjects {
		result[i] = &ResourceInfo{
			Path:        obj.Path,
			Type:        obj.Type,
			Size:        obj.SizeBytes,
			Rows:        obj.NumRows,
			Created:     obj.Created,
			Modified:    obj.Modified,
			Description: obj.Description,
			Location:    obj.Location,
			Details:     obj,
		}
	}

	return result, nil
}

// Remove removes BigQuery table(s)/dataset at the given path
func (b *BigQueryResource) Remove(ctx context.Context, path string, options *RemoveOptions) error {
	projectID, datasetID, tableID, err := bigquery.ParseBQPath(path)
	if err != nil {
		return err
	}

	// Convert to bigquery.PathFormatter
	bqFormatter := bigquery.PathFormatter(b.formatter)

	// Case 1: Wildcard in table name
	if tableID != "" && resolver.HasWildcard(tableID) {
		_, err := bigquery.RemoveTablesWithPattern(ctx, projectID, datasetID, tableID, bqFormatter, resolver.MatchPattern)
		return err
	}

	// Case 2: Specific table
	if tableID != "" {
		return bigquery.RemoveTable(ctx, projectID, datasetID, tableID, bqFormatter)
	}

	// Case 3: Dataset (requires recursive)
	if datasetID != "" {
		if !options.Recursive {
			return fmt.Errorf("cannot remove dataset without -r flag")
		}
		return bigquery.RemoveDataset(ctx, projectID, datasetID, true, bqFormatter)
	}

	return fmt.Errorf("cannot remove entire project")
}

// Info gets detailed information about a BigQuery table
func (b *BigQueryResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	projectID, datasetID, tableID, err := bigquery.ParseBQPath(path)
	if err != nil {
		return nil, err
	}

	if tableID == "" {
		return nil, fmt.Errorf("info command requires a full table path")
	}

	obj, err := bigquery.DescribeTable(ctx, projectID, datasetID, tableID)
	if err != nil {
		return nil, err
	}

	return &ResourceInfo{
		Path:        obj.Path,
		Type:        obj.Type,
		Size:        obj.SizeBytes,
		Rows:        obj.NumRows,
		Created:     obj.Created,
		Modified:    obj.Modified,
		Description: obj.Description,
		Location:    obj.Location,
		Details:     obj,
	}, nil
}

// ParsePath parses a BigQuery path into components
func (b *BigQueryResource) ParsePath(path string) (*PathComponents, error) {
	projectID, datasetID, tableID, err := bigquery.ParseBQPath(path)
	if err != nil {
		return nil, err
	}

	return &PathComponents{
		ResourceType: TypeBigQuery,
		Project:      projectID,
		Dataset:      datasetID,
		Table:        tableID,
	}, nil
}

// FormatShort formats BigQuery object info in short format
func (b *BigQueryResource) FormatShort(info *ResourceInfo, aliasPath string) string {
	if obj, ok := info.Details.(*bigquery.BQObjectInfo); ok {
		return obj.FormatShortWithAlias(aliasPath)
	}
	return aliasPath
}

// FormatLong formats BigQuery object info in long format
func (b *BigQueryResource) FormatLong(info *ResourceInfo, aliasPath string) string {
	if obj, ok := info.Details.(*bigquery.BQObjectInfo); ok {
		return obj.FormatLongWithAlias(aliasPath)
	}
	return aliasPath
}

// FormatDetailed formats BigQuery object info with full details
func (b *BigQueryResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	if obj, ok := info.Details.(*bigquery.BQObjectInfo); ok {
		return obj.FormatDetailed(aliasPath)
	}
	return aliasPath
}

// FormatLongHeader returns the header line for long format listing
func (b *BigQueryResource) FormatLongHeader() string {
	return bigquery.FormatLongHeader()
}

// SupportsInfo returns whether BigQuery supports the info command
func (b *BigQueryResource) SupportsInfo() bool {
	return true
}
