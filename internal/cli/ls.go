package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/resolver"
	"github.com/thieso2/cio/internal/storage"
)

var (
	lsLongFormat    bool
	lsHumanReadable bool
	lsRecursive     bool
	lsMaxResults    int
)

var lsCmd = &cobra.Command{
	Use:   "ls <path>",
	Short: "List GCS objects",
	Long: `List objects in a GCS bucket using an alias or full gs:// path.

The path can be either:
  - An alias: 'am', 'am/2024/', 'am/2024/01/data.txt'
  - A full GCS path: 'gs://bucket-name/', 'gs://bucket-name/prefix/'

Examples:
  # List top-level of mapped bucket
  cio ls am

  # List with details
  cio ls -l am

  # List with human-readable sizes
  cio ls -l --human-readable am/2024/

  # List recursively
  cio ls -r am/2024/

  # List recursively with all details
  cio ls -lr --human-readable am/2024/

  # List using full GCS path
  cio ls gs://my-bucket/path/`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Resolve alias to full GCS path if needed
		r := resolver.New(cfg)
		gcsPath, err := r.Resolve(path)
		if err != nil {
			return err
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "Listing: %s\n", gcsPath)
		}

		// Configure list options
		opts := &storage.ListOptions{
			Recursive:     lsRecursive,
			LongFormat:    lsLongFormat,
			HumanReadable: lsHumanReadable,
			MaxResults:    lsMaxResults,
		}

		// List objects
		ctx := context.Background()
		objects, err := storage.ListByPath(ctx, gcsPath, opts)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Handle empty results
		if len(objects) == 0 {
			if verbose {
				fmt.Fprintf(os.Stderr, "No objects found\n")
			}
			return nil
		}

		// Print results
		for _, obj := range objects {
			if lsLongFormat {
				fmt.Println(obj.FormatLong(lsHumanReadable))
			} else {
				fmt.Println(obj.FormatShort())
			}
		}

		return nil
	},
}

func init() {
	// Add flags
	lsCmd.Flags().BoolVarP(&lsLongFormat, "long", "l", false, "use long listing format (timestamp, size, path)")
	lsCmd.Flags().BoolVar(&lsHumanReadable, "human-readable", false, "print sizes in human-readable format (e.g., 1.2 MB)")
	lsCmd.Flags().BoolVarP(&lsRecursive, "recursive", "r", false, "list all objects recursively")
	lsCmd.Flags().IntVar(&lsMaxResults, "max-results", 0, "maximum number of results (0 = no limit)")

	// Add to root command
	rootCmd.AddCommand(lsCmd)
}
