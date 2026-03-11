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
	Short: "Remove objects from GCS, BigQuery, Cloud Run, Compute Engine, or Pub/Sub",
	Long: `Remove objects from Google Cloud Storage, BigQuery tables/datasets, Cloud Run job executions, VM instances, or Pub/Sub topics/subscriptions.

Examples (GCS):
  cio rm :am/2024/data.csv
  cio rm ':am/temp/*.tmp'
  cio rm -rf :am/old-data/

Examples (BigQuery):
  cio rm :mydata.events
  cio rm ':mydata.temp_*'
  cio rm -r :mydata

Examples (Cloud Run Jobs):
  # Remove a specific execution
  cio rm jobs://my-job/my-job-abc123

  # Remove all completed/failed executions (skips running ones)
  cio rm 'jobs://my-job/*'

  # Force remove without confirmation
  cio rm -f 'jobs://my-job/*'

Examples (VM):
  # Stop and delete a VM instance
  cio rm vm://europe-west3-a/my-instance

  # Stop and delete matching instances (all zones)
  cio rm 'vm://*/staging-*'

  # Stop and delete matching instances in a zone
  cio rm 'vm://europe-west3-a/staging-*'

  # Force remove without confirmation
  cio rm -f vm://europe-west3-a/my-instance

Examples (Pub/Sub):
  # Delete a topic (warns about orphaned subscriptions)
  cio rm pubsub://topics/my-topic

  # Delete a subscription
  cio rm pubsub://subs/my-sub

  # Delete subscriptions matching a pattern
  cio rm 'pubsub://subs/staging-*'

  # Force delete without confirmation
  cio rm -f pubsub://subs/test-sub

CAUTION: Deleted objects, tables, executions, VMs, and Pub/Sub resources cannot be recovered.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Resolve alias
		r := resolver.Create(cfg)
		var fullPath string
		var err error
		var inputWasAlias bool

		// If it's already a direct path, use it as-is
		if resolver.IsGCSPath(path) || resolver.IsBQPath(path) || resolver.IsCloudRunPath(path) || resolver.IsVMPath(path) || resolver.IsPubSubPath(path) {
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

		// Cloud Run, VM, and Pub/Sub handle their own listing/confirmation in Remove
		if resolver.IsCloudRunPath(fullPath) || resolver.IsVMPath(fullPath) || resolver.IsPubSubPath(fullPath) {
			options := &resource.RemoveOptions{
				Force:   rmForce,
				Verbose: verbose,
				Project: cfg.Defaults.ProjectID,
				Region:  cfg.Defaults.Region,
			}
			return res.Remove(ctx, fullPath, options)
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
			Recursive:   rmRecursive,
			Force:       rmForce,
			Verbose:     verbose,
			Parallelism: GetParallelism(),
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
