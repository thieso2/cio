package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/resolver"
	"github.com/thieso2/cio/internal/resource"
)

var infoCmd = &cobra.Command{
	Use:   "info <path>",
	Short: "Show detailed information about resources",
	Long: `Display detailed information about resources including schema, size, and metadata.

Currently supports BigQuery tables. GCS objects should use 'ls -l' instead.

Examples:
  cio info :mydata.events
  cio info bq://my-project-id.my-dataset.my-table`,
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
			inputWasAlias = true
		}

		ctx := context.Background()

		// Create resource factory with appropriate formatter
		var formatter resource.PathFormatter
		if inputWasAlias {
			formatter = r.ReverseResolve
		} else {
			formatter = func(path string) string { return path }
		}
		factory := resource.CreateFactory(formatter)

		// Get appropriate resource handler
		res, err := factory.Create(fullPath)
		if err != nil {
			return err
		}

		// Check if this resource type supports info
		if !res.SupportsInfo() {
			return fmt.Errorf("info command not supported for %s resources (use 'ls -l' instead)", res.Type())
		}

		// Get detailed info
		info, err := res.Info(ctx, fullPath)
		if err != nil {
			return fmt.Errorf("failed to get resource info: %w", err)
		}

		// Show detailed format - use appropriate display path
		displayPath := info.Path
		if inputWasAlias {
			displayPath = r.ReverseResolve(info.Path)
		}
		fmt.Print(res.FormatDetailed(info, displayPath))

		return nil
	},
}

func init() {
	// Add to root command
	rootCmd.AddCommand(infoCmd)
}
