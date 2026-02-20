package bigquery

import (
	"context"
	"fmt"

	"github.com/thieso2/cio/apilog"
)

// PathFormatter is a function that converts full paths to alias format
type PathFormatter func(string) string

// RemoveTable deletes a BigQuery table
func RemoveTable(ctx context.Context, projectID, datasetID, tableID string, formatter PathFormatter) error {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	table := client.Dataset(datasetID).Table(tableID)

	// Build full path for logging
	fullPath := fmt.Sprintf("bq://%s.%s.%s", projectID, datasetID, tableID)
	displayPath := fullPath
	if formatter != nil {
		displayPath = formatter(fullPath)
	}

	apilog.Logf("[BQ] Table.Delete(bq://%s.%s.%s)", projectID, datasetID, tableID)
	if err := table.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	// Always log deletions
	fmt.Printf("Deleted: %s\n", displayPath)

	return nil
}

// RemoveDataset deletes a BigQuery dataset
// If recursive is true, all tables in the dataset will be deleted first
func RemoveDataset(ctx context.Context, projectID, datasetID string, recursive bool, formatter PathFormatter) error {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	dataset := client.Dataset(datasetID)

	if recursive {
		// Delete all tables first
		tables, err := ListTables(ctx, projectID, datasetID)
		if err != nil {
			return fmt.Errorf("failed to list tables for deletion: %w", err)
		}

		for _, table := range tables {
			// Extract table ID from path
			_, _, tableID, err := ParseBQPath(table.Path)
			if err != nil {
				continue
			}

			if err := RemoveTable(ctx, projectID, datasetID, tableID, formatter); err != nil {
				return fmt.Errorf("failed to delete table %s: %w", tableID, err)
			}
		}
	}

	// Build full path for logging
	fullPath := fmt.Sprintf("bq://%s.%s", projectID, datasetID)
	displayPath := fullPath
	if formatter != nil {
		displayPath = formatter(fullPath)
	}

	apilog.Logf("[BQ] Dataset.Delete(bq://%s.%s)", projectID, datasetID)
	if err := dataset.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	// Always log deletions
	fmt.Printf("Deleted: %s\n", displayPath)

	return nil
}

// RemoveTablesWithPattern deletes all tables matching a wildcard pattern
func RemoveTablesWithPattern(ctx context.Context, projectID, datasetID, pattern string, formatter PathFormatter, matchPattern func(string, string) bool) ([]string, error) {
	// List all tables
	tables, err := ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	var deletedTables []string

	// Filter and delete matching tables
	for _, table := range tables {
		// Extract table ID from path
		_, _, tableID, err := ParseBQPath(table.Path)
		if err != nil {
			continue
		}

		if matchPattern(tableID, pattern) {
			if err := RemoveTable(ctx, projectID, datasetID, tableID, formatter); err != nil {
				return deletedTables, fmt.Errorf("failed to delete table %s: %w", tableID, err)
			}
			deletedTables = append(deletedTables, tableID)
		}
	}

	return deletedTables, nil
}
