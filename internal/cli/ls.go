package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/resolver"
	"github.com/thieso2/cio/internal/resource"
)

var (
	lsLongFormat    bool
	lsHumanReadable bool
	lsRecursive     bool
	lsMaxResults    int
	lsNoMap         bool
)

var lsCmd = &cobra.Command{
	Use:   "ls <path>",
	Short: "List GCS buckets/objects or BigQuery datasets/tables",
	Long: `List GCS buckets, objects, or BigQuery datasets/tables using an alias or full path.

The path can be either:
  - An alias (with : prefix): ':am', ':am/2024/', ':am/2024/01/data.txt'
  - A full GCS path: 'gs://bucket-name/', 'gs://bucket-name/prefix/'
  - List all buckets: 'gs://project-id:' (note the colon at the end)
  - A full BigQuery path: 'bq://project-id', 'bq://project-id.dataset'
  - List all datasets: 'bq://' (uses default project from config)
  - Wildcard pattern: ':am/logs/*.log', ':am/data/2024-*.csv'

Examples (GCS):
  # List buckets in a project
  cio ls 'gs://my-project-id:'

  # List objects in bucket
  cio ls :am
  cio ls -l :am/2024/
  cio ls ':am/logs/*.log'

Examples (BigQuery):
  # List datasets in default project
  cio ls bq://

  # List datasets in specific project
  cio ls bq://my-project-id

  # List tables in dataset
  cio ls :mydata
  cio ls ':mydata.events_*'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Resolve alias to full path if needed
		r := resolver.Create(cfg)
		var fullPath string
		var err error
		var inputWasAlias bool

		// If it's already a gs:// or bq:// path, use it directly
		if resolver.IsGCSPath(path) || resolver.IsBQPath(path) {
			fullPath = path
			inputWasAlias = false
		} else {
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
			inputWasAlias = true // User provided an alias
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "Listing: %s\n", fullPath)
		}

		ctx := context.Background()

		// Create resource factory
		factory := resource.CreateFactory(r.ReverseResolve)

		// Get appropriate resource handler
		res, err := factory.Create(fullPath)
		if err != nil {
			return err
		}

		// List resources
		options := &resource.ListOptions{
			Recursive:     lsRecursive,
			LongFormat:    lsLongFormat,
			HumanReadable: lsHumanReadable,
			MaxResults:    lsMaxResults,
			ProjectID:     cfg.Defaults.ProjectID,
		}

		resources, err := res.List(ctx, fullPath, options)
		if err != nil {
			return fmt.Errorf("failed to list resources: %w", err)
		}

		// Handle empty results
		if len(resources) == 0 {
			if verbose {
				fmt.Fprintf(os.Stderr, "No resources found\n")
			}
			return nil
		}

		// Determine whether to reverse-map output
		// Only reverse-map if: input was an alias AND --no-map flag is not set
		shouldReverseMap := inputWasAlias && !lsNoMap

		// Print header for long format if resource type provides one
		if lsLongFormat {
			header := res.FormatLongHeader()
			if header != "" {
				fmt.Println(header)
			}
		}

		// Print results
		for _, info := range resources {
			displayPath := info.Path
			if shouldReverseMap {
				displayPath = r.ReverseResolve(info.Path)
			}

			if lsLongFormat {
				fmt.Println(res.FormatLong(info, displayPath))
			} else {
				fmt.Println(res.FormatShort(info, displayPath))
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
	lsCmd.Flags().BoolVarP(&lsNoMap, "no-map", "n", false, "show full paths without alias mapping")

	// Add to root command
	rootCmd.AddCommand(lsCmd)
}
