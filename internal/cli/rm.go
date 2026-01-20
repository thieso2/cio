package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var (
	rmRecursive bool
	rmForce     bool
)

var rmCmd = &cobra.Command{
	Use:   "rm <path>",
	Short: "Remove objects from GCS or BigQuery",
	Long: `Remove objects from Google Cloud Storage or BigQuery tables/datasets.

Examples (GCS):
  cio rm :am/2024/data.csv
  cio rm ':am/temp/*.tmp'
  cio rm -rf :am/old-data/

Examples (BigQuery):
  cio rm :mydata.events
  cio rm ':mydata.temp_*'
  cio rm -r :mydata

CAUTION: Deleted objects and tables cannot be recovered.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Resolve alias
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
				return fmt.Errorf("failed to resolve path: %w", err)
			}
			inputWasAlias = true // User provided an alias
		}

		ctx := context.Background()

		// Create resource factory with appropriate formatter
		// Only use reverse resolver if input was an alias
		var formatter resource.PathFormatter
		if inputWasAlias {
			formatter = r.ReverseResolve
		} else {
			// Use identity formatter (returns path as-is)
			formatter = func(path string) string { return path }
		}
		factory := resource.CreateFactory(formatter)

		// Get appropriate resource handler
		res, err := factory.Create(fullPath)
		if err != nil {
			return err
		}

		// Parse path to check for wildcards
		components, err := res.ParsePath(fullPath)
		if err != nil {
			return err
		}

		// Only reverse-map if input was an alias
		displayPath := fullPath
		if inputWasAlias {
			displayPath = r.ReverseResolve(fullPath)
		}

		// Check for wildcards and list matching resources first
		hasWildcard := false
		if components.ResourceType == resource.TypeGCS {
			hasWildcard = resolver.HasWildcard(components.Object)
		} else if components.ResourceType == resource.TypeBigQuery {
			hasWildcard = resolver.HasWildcard(components.Table)
		}

		if hasWildcard {
			// List matching resources
			resources, err := res.List(ctx, fullPath, &resource.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list matching resources: %w", err)
			}

			if len(resources) == 0 {
				fmt.Println("No matching resources found.")
				return nil
			}

			// Show matching resources
			resourceWord := "object(s)"
			if components.ResourceType == resource.TypeBigQuery {
				resourceWord = "table(s)"
			}

			fmt.Printf("Found %d matching %s:\n", len(resources), resourceWord)
			for _, info := range resources {
				// Only reverse-map if input was an alias
				displayResourcePath := info.Path
				if inputWasAlias {
					displayResourcePath = r.ReverseResolve(info.Path)
				}
				fmt.Printf("  - %s\n", displayResourcePath)
			}
			fmt.Println()

			// Confirm deletion unless force flag is set
			if !rmForce {
				fmt.Printf("Remove all %d %s? (y/N): ", len(resources), resourceWord)
				var response string
				fmt.Scanln(&response)
				if response != "y" && response != "Y" {
					fmt.Println("Cancelled.")
					return nil
				}
			}
		} else {
			// For non-wildcard paths, confirm deletion
			if !rmForce {
				resourceType := "file"
				if components.ResourceType == resource.TypeBigQuery {
					if components.Table != "" {
						resourceType = "table"
					} else {
						resourceType = "dataset"
					}
				} else if components.ResourceType == resource.TypeGCS {
					isDirectory := components.Object == "" || components.Object[len(components.Object)-1] == '/'
					if isDirectory {
						resourceType = "directory"
					}
				}

				fmt.Printf("Remove %s %s? (y/N): ", resourceType, displayPath)
				var response string
				fmt.Scanln(&response)
				if response != "y" && response != "Y" {
					fmt.Println("Cancelled.")
					return nil
				}
			}
		}

		// Execute removal
		options := &resource.RemoveOptions{
			Recursive: rmRecursive,
			Force:     rmForce,
			Verbose:   verbose,
		}

		return res.Remove(ctx, fullPath, options)
	},
}

func init() {
	// Add flags
	rmCmd.Flags().BoolVarP(&rmRecursive, "recursive", "r", false, "remove directories and their contents recursively")
	rmCmd.Flags().BoolVarP(&rmForce, "force", "f", false, "force removal without confirmation")

	// Add to root command
	rootCmd.AddCommand(rmCmd)
}
