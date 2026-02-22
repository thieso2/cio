package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/storage"
)

var (
	duSummarize bool
	duBytes     bool
)

var duCmd = &cobra.Command{
	Use:   "du <path>",
	Short: "Show disk usage of GCS paths",
	Long: `Show disk usage of GCS objects, similar to Unix du.

Without -s, shows the size of each immediate subdirectory followed by the
grand total. With -s, shows only the grand total.

For wildcard paths, each matching entry is always shown as a per-match
summary, followed by a grand total line (-s has no effect on the per-match
lines since they are already summaries).

Subdirectory sizes are calculated in parallel using SetAttrSelection to fetch
only Name and Size, significantly reducing API payload and speeding up large
bucket traversals. Parallelism is controlled by the global -j flag.

The path can be:
  - An alias (with : prefix): ':am', ':am/2024/'
  - A full GCS path: 'gs://bucket-name/', 'gs://bucket-name/prefix/'

Examples:
  # Show usage per subdirectory + total
  cio du :am/2024/

  # Show only the grand total
  cio du -s :am/2024/

  # Wildcard: per-match summaries + grand total
  cio du "gs://bucket/prefix*/"
  cio du -s "gs://bucket/prefix*/"

  # Show raw byte counts
  cio du --bytes :am/

Note: parallelism is controlled by the global -j flag (default: 50).`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		r := resolver.Create(cfg)
		var fullPath string
		var inputWasAlias bool

		if resolver.IsGCSPath(path) {
			fullPath = path
		} else {
			var err error
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
			inputWasAlias = true
		}

		if !resolver.IsGCSPath(fullPath) {
			return fmt.Errorf("du only supports GCS paths (gs:// or aliases mapping to GCS)")
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "Calculating disk usage: %s\n", fullPath)
		}

		bucket, prefix, err := resolver.ParseGCSPath(fullPath)
		if err != nil {
			return err
		}

		ctx := context.Background()

		displayPath := func(gcsPath string) string {
			if inputWasAlias {
				return r.ReverseResolve(gcsPath)
			}
			return gcsPath
		}

		// Wildcard path: find all matching entries and sum each in parallel.
		if strings.ContainsAny(prefix, "*?") {
			entries, err := storage.DiskUsagePattern(ctx, bucket, prefix, &storage.DUOptions{Workers: parallelism})
			if err != nil {
				return fmt.Errorf("failed to calculate disk usage: %w", err)
			}
			if len(entries) == 0 {
				if verbose {
					fmt.Fprintf(os.Stderr, "No matching paths found\n")
				}
				return nil
			}
			// Each match is already a summary; always show them plus a grand total.
			var total int64
			for _, entry := range entries {
				total += entry.Size
				fmt.Printf("%s  %s\n", formatDUSize(entry.Size, duBytes), displayPath(entry.Path))
			}
			fmt.Printf("%s  total\n", formatDUSize(total, duBytes))
			return nil
		}

		// Non-wildcard path: shallow-list subdirs, sum each in parallel.
		result, err := storage.DiskUsage(ctx, bucket, prefix, &storage.DUOptions{Workers: parallelism})
		if err != nil {
			return fmt.Errorf("failed to calculate disk usage: %w", err)
		}

		if duSummarize {
			fmt.Printf("%s  %s\n", formatDUSize(result.Total, duBytes), displayPath(result.RootPath))
			return nil
		}

		for _, entry := range result.Entries {
			fmt.Printf("%s  %s\n", formatDUSize(entry.Size, duBytes), displayPath(entry.Path))
		}
		fmt.Printf("%s  %s\n", formatDUSize(result.Total, duBytes), displayPath(result.RootPath))

		return nil
	},
}

// formatDUSize right-aligns the size value to a fixed column width, matching
// the style used by ls -l in this codebase.
func formatDUSize(bytes int64, rawBytes bool) string {
	if rawBytes {
		return fmt.Sprintf("%12d", bytes)
	}
	return fmt.Sprintf("%10s", storage.FormatSize(bytes))
}

func init() {
	duCmd.Flags().BoolVarP(&duSummarize, "summarize", "s", false, "display only a total for each argument")
	duCmd.Flags().BoolVarP(&duBytes, "bytes", "b", false, "print raw byte counts instead of human-readable sizes")

	rootCmd.AddCommand(duCmd)
}
