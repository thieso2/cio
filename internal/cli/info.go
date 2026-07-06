package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/bigquery"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var infoCmd = &cobra.Command{
	Use:   "info <path>",
	Short: "Show detailed information about resources",
	Long: `Display detailed information about resources including schema, size, metadata, and dependency graphs.

Supports BigQuery tables/views, Pub/Sub topics/subscriptions, Cloud SQL instances,
Cloud Scheduler jobs, and GCP projects. GCS objects should use 'ls -l' instead.
Supports wildcards: cio info 'bq://project.dataset.v_*'

Examples:
  cio info :mydata.events
  cio info bq://my-project-id.my-dataset.my-table
  cio info 'bq://my-project-id.my-dataset.v_*'
  cio info --json :mydata.events
  cio info pubsub://topics/my-topic
  cio info scheduler://my-job
  cio info project://my-project-id`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Resolve the input (shared prelude). info builds the factory itself so its
		// BigQuery-wildcard branch can fan out before creating a single handler.
		r, fullPath, inputWasAlias, err := resolveInput(path)
		if err != nil {
			return err
		}

		ctx := context.Background()
		factory := newResourceFactory(r, inputWasAlias)

		// Check for wildcard in BigQuery path
		if resolver.IsBQPath(fullPath) && resolver.HasWildcard(fullPath) {
			return handleBQWildcardInfo(ctx, fullPath, factory, r, inputWasAlias)
		}

		// Get appropriate resource handler
		res, err := factory.Create(fullPath)
		if err != nil {
			return err
		}

		// Dispatch by capability: resources whose info needs an explicit project
		// (Pub/Sub, Cloud SQL) implement ProjectInfoable; the rest implement
		// Infoable. A type that does neither doesn't support `cio info`.
		var info *resource.ResourceInfo
		switch ir := res.(type) {
		case resource.ProjectInfoable:
			projectID := cfg.Defaults.ProjectID
			if projectID == "" {
				return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
			}
			info, err = ir.InfoWithProject(ctx, fullPath, projectID)
		case resource.Infoable:
			info, err = ir.Info(ctx, fullPath)
		default:
			return fmt.Errorf("info command not supported for %s resources (use 'ls -l' instead)", res.Type())
		}
		if err != nil {
			return fmt.Errorf("failed to get resource info: %w", err)
		}

		displayPath := info.Path
		if inputWasAlias {
			displayPath = r.ReverseResolve(info.Path)
		}

		if outputJSON {
			return printInfoJSON(info, displayPath)
		}

		fmt.Print(res.FormatDetailed(info, displayPath))
		return nil
	},
}

func handleBQWildcardInfo(ctx context.Context, fullPath string, _ *resource.Factory, r *resolver.Resolver, inputWasAlias bool) error {
	projectID, datasetID, tableID, err := bigquery.ParseBQPath(fullPath)
	if err != nil {
		return err
	}
	if datasetID == "" || tableID == "" {
		return fmt.Errorf("wildcard info requires dataset and table pattern: bq://project.dataset.pattern*")
	}

	// List all tables, filter by pattern
	allTables, err := bigquery.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return err
	}

	var matched []*bigquery.BQObjectInfo
	for _, table := range allTables {
		if idx := strings.LastIndex(table.Path, "."); idx != -1 {
			tableName := table.Path[idx+1:]
			if resolver.MatchPattern(tableName, tableID) {
				matched = append(matched, table)
			}
		}
	}

	if len(matched) == 0 {
		return fmt.Errorf("no tables matching pattern: %s", tableID)
	}

	// For JSON mode, collect all results into an array
	if outputJSON {
		var jsonResults []*bigquery.InfoJSON
		for _, table := range matched {
			// Parse table name from path
			parts := strings.Split(table.Path, ".")
			tID := parts[len(parts)-1]

			obj, err := bigquery.DescribeTable(ctx, projectID, datasetID, tID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", table.Path, err)
				continue
			}
			if obj.Type == "table" || obj.Type == "materialized_view" {
				_ = bigquery.FetchStorageInfo(ctx, projectID, datasetID, tID, obj)
			}

			displayPath := obj.Path
			if inputWasAlias {
				displayPath = r.ReverseResolve(obj.Path)
			}
			jsonResults = append(jsonResults, obj.ToJSON(displayPath))
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(jsonResults)
	}

	// Text mode: print each one separated by blank line
	for i, table := range matched {
		parts := strings.Split(table.Path, ".")
		tID := parts[len(parts)-1]

		obj, err := bigquery.DescribeTable(ctx, projectID, datasetID, tID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", table.Path, err)
			continue
		}
		if obj.Type == "table" || obj.Type == "materialized_view" {
			_ = bigquery.FetchStorageInfo(ctx, projectID, datasetID, tID, obj)
		}

		displayPath := obj.Path
		if inputWasAlias {
			displayPath = r.ReverseResolve(obj.Path)
		}

		if i > 0 {
			fmt.Println()
		}
		fmt.Print(obj.FormatDetailed(displayPath))
	}

	return nil
}

func printInfoJSON(info *resource.ResourceInfo, displayPath string) error {
	if obj, ok := info.Details.(*bigquery.BQObjectInfo); ok {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(obj.ToJSON(displayPath))
	}
	// Fallback: serialize ResourceInfo directly
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(info)
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
