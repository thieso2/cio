package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/bigquery"
	"github.com/thieso2/cio/internal/config"
	"github.com/thieso2/cio/internal/resolver"
)

var (
	queryFormat     string
	queryMaxResults int
	queryDryRun     bool
	queryFile       string
	queryShowStats  bool
)

var queryCmd = &cobra.Command{
	Use:   "query [SQL]",
	Short: "Execute BigQuery SQL queries",
	Long: `Execute BigQuery SQL queries with support for aliases.

Examples:
  # Non-interactive mode
  cio query "SELECT 1 as num"
  cio query "SELECT * FROM :mydata.events LIMIT 10"

  # Interactive mode (launches shell)
  cio query

  # Different output formats
  cio query --format json "SELECT * FROM :mydata.events LIMIT 5"
  cio query --format csv "SELECT id, name FROM :mydata.users"

  # Dry run (validate without executing)
  cio query --dry-run "SELECT * FROM :mydata.huge_table"

  # Read from file
  cio query --file analysis.sql`,
	RunE: runQuery,
}

func init() {
	queryCmd.Flags().StringVarP(&queryFormat, "format", "f", "table", "Output format: table, json, csv")
	queryCmd.Flags().IntVarP(&queryMaxResults, "max-results", "n", 1000, "Maximum number of results to return")
	queryCmd.Flags().BoolVar(&queryDryRun, "dry-run", false, "Validate query without executing")
	queryCmd.Flags().StringVar(&queryFile, "file", "", "Read SQL from file")
	queryCmd.Flags().BoolVar(&queryShowStats, "stats", true, "Show query statistics")

	rootCmd.AddCommand(queryCmd)
}

func runQuery(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	cfg := GetConfig()

	// If no SQL provided and no file, launch interactive shell
	if len(args) == 0 && queryFile == "" {
		return runInteractiveShell(ctx, cfg)
	}

	// Get SQL from args or file
	var sql string
	if queryFile != "" {
		content, err := os.ReadFile(queryFile)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", queryFile, err)
		}
		sql = string(content)
	} else {
		sql = strings.Join(args, " ")
	}

	// Resolve aliases in SQL
	resolvedSQL, err := resolveAliasesInSQL(sql, cfg)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Resolved SQL: %s\n", resolvedSQL)
	}

	// Get project ID from config or flag
	projectID := cfg.Defaults.ProjectID
	if projectID == "" {
		return fmt.Errorf("project ID not set. Use --project flag or set it in config")
	}

	// Dry run mode
	if queryDryRun {
		bytesProcessed, err := bigquery.DryRunQuery(ctx, projectID, resolvedSQL)
		if err != nil {
			return fmt.Errorf("query validation failed: %w", err)
		}
		fmt.Printf("Query is valid.\n")
		fmt.Printf("Estimated bytes to process: %s\n", bigquery.FormatBytes(bytesProcessed))
		return nil
	}

	// Execute query
	result, err := bigquery.ExecuteQuery(ctx, projectID, resolvedSQL, queryMaxResults)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Format output based on format flag
	switch queryFormat {
	case "table":
		if err := bigquery.FormatQueryResultTable(result, os.Stdout); err != nil {
			return err
		}
	case "json":
		if err := bigquery.FormatQueryResultJSON(result, os.Stdout); err != nil {
			return err
		}
	case "csv":
		if err := bigquery.FormatQueryResultCSV(result, os.Stdout); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported format: %s (use table, json, or csv)", queryFormat)
	}

	// Show statistics
	if queryShowStats {
		stats := result.GetStats()
		fmt.Fprintf(os.Stderr, "\n")
		if stats.CacheHit {
			fmt.Fprintf(os.Stderr, "(%d rows in %s, cached)\n",
				stats.RowCount,
				bigquery.FormatDuration(stats.ExecutionTime))
		} else {
			fmt.Fprintf(os.Stderr, "(%d rows in %s, %s processed)\n",
				stats.RowCount,
				bigquery.FormatDuration(stats.ExecutionTime),
				bigquery.FormatBytes(stats.BytesProcessed))
		}
	}

	return nil
}

// resolveAliasesInSQL replaces :alias references with full BigQuery paths
func resolveAliasesInSQL(sql string, cfg *config.Config) (string, error) {
	r := resolver.Create(cfg)

	// Find all words that start with :
	words := strings.Fields(sql)
	for i, word := range words {
		// Handle :alias and :alias.table patterns
		if strings.HasPrefix(word, ":") {
			// Find where the alias ends (at . or non-alphanumeric character)
			aliasEnd := strings.IndexAny(word[1:], ".,;) \t\n") + 1
			if aliasEnd == 0 {
				aliasEnd = len(word)
			}

			alias := word[:aliasEnd]
			remainder := word[aliasEnd:]

			// Try to resolve the alias
			fullPath, err := r.Resolve(alias)
			if err != nil {
				// Not a valid alias, skip
				continue
			}

			// Convert bq://project.dataset.table → project.dataset.table
			fullPath = strings.TrimPrefix(fullPath, "bq://")

			// Replace in the original word
			words[i] = fullPath + remainder
		}
	}

	return strings.Join(words, " "), nil
}
