package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/storage"
)

var catCmd = &cobra.Command{
	Use:   "cat <path> [<path>...]",
	Short: "Print GCS object(s) to stdout",
	Long: `Stream one or more Google Cloud Storage objects to stdout.

Supports alias paths, full gs:// paths, and wildcard patterns.

Examples:
  # Print a single file
  cio cat :am/logs/app.log

  # Print all matching files
  cio cat 'gs://io-db-legacy-exports/spdbbn023-1.ioint.de/*.log'

  # Concatenate several files
  cio cat :am/2024/01/a.csv :am/2024/01/b.csv`,
	Args: cobra.MinimumNArgs(1),
	RunE: runCat,
}

func init() {
	rootCmd.AddCommand(catCmd)
}

func runCat(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	r := resolver.Create(cfg)

	client, err := storage.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	for _, arg := range args {
		// Resolve alias or use path as-is
		var fullPath string
		if resolver.IsGCSPath(arg) {
			fullPath = arg
		} else {
			fullPath, err = r.Resolve(arg)
			if err != nil {
				return fmt.Errorf("failed to resolve %q: %w", arg, err)
			}
		}

		bucket, object, err := resolver.ParseGCSPath(fullPath)
		if err != nil {
			return err
		}

		if resolver.HasWildcard(object) {
			if err := storage.CatWithPattern(ctx, client, bucket, object, os.Stdout); err != nil {
				return err
			}
		} else {
			if err := storage.CatObject(ctx, client, bucket, object, os.Stdout); err != nil {
				return err
			}
		}
	}
	return nil
}
