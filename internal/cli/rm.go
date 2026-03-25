package cli

import (
	"context"
	"fmt"
	"os"

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
  # Delete a job
  cio rm jobs://my-job

  # Delete jobs matching a pattern
  cio rm 'jobs://sqlmesh-*'

  # Delete jobs across projects (discover mode)
  cio rm 'jobs:/my-project/sqlmesh-*'

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

		// Check for discover mode: scheme:/project-pattern/rest
		if projectPattern, scheme, rest, ok := parseDiscoverPath(path); ok {
			return runDiscoverRemove(cmd, scheme, projectPattern, rest)
		}

		// Resolve alias
		r := resolver.Create(cfg)
		var fullPath string
		var err error
		var inputWasAlias bool

		// If it's already a direct path, use it as-is
		if resolver.IsGCSPath(path) || resolver.IsBQPath(path) || resolver.IsCloudRunPath(path) || resolver.IsVMPath(path) || resolver.IsPubSubPath(path) || resolver.IsCloudSQLPath(path) {
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
		if resolver.IsCloudRunPath(fullPath) || resolver.IsVMPath(fullPath) || resolver.IsPubSubPath(fullPath) || resolver.IsCloudSQLPath(fullPath) {
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

// runDiscoverRemove lists matching resources across projects, shows them, and asks for confirmation.
func runDiscoverRemove(cmd *cobra.Command, scheme, projectPattern, rest string) error {
	ctx := context.Background()

	projectIDs, err := resource.ListProjectIDs(ctx, projectPattern)
	if err != nil {
		return err
	}
	if len(projectIDs) == 0 {
		fmt.Fprintf(os.Stderr, "No projects matching %s\n", projectPattern)
		return nil
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Discover: %d projects matching %s\n", len(projectIDs), projectPattern)
	}

	r := resolver.Create(cfg)
	factory := resource.CreateFactory(r.ReverseResolve)

	// First pass: collect all matching resources across projects
	type discoverMatch struct {
		projectID string
		info      *resource.ResourceInfo
	}
	var allMatches []discoverMatch

	for _, projectID := range projectIDs {
		var resourcePath string
		switch scheme {
		case "bq":
			resourcePath = "bq://" + projectID
			if rest != "" {
				resourcePath += "." + rest
			}
		case "iam":
			resourcePath = "iam://" + projectID
			if rest != "" {
				resourcePath += "/" + rest
			}
		default:
			resourcePath = scheme + "://"
			if rest != "" {
				resourcePath += rest
			}
		}

		res, err := factory.Create(resourcePath)
		if err != nil {
			continue
		}

		options := &resource.ListOptions{
			ProjectID: projectID,
			Region:    cfg.Defaults.Region,
		}

		resources, err := res.List(ctx, resourcePath, options)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
			}
			continue
		}

		for _, info := range resources {
			allMatches = append(allMatches, discoverMatch{projectID: projectID, info: info})
		}
	}

	if len(allMatches) == 0 {
		fmt.Println("No matching resources found.")
		return nil
	}

	// Show what will be deleted
	fmt.Printf("Found %d resource(s) to delete:\n", len(allMatches))
	for _, m := range allMatches {
		fmt.Printf("  - %s:%s\n", m.projectID, m.info.Name)
	}
	fmt.Println()

	if !rmForce {
		fmt.Printf("Delete all %d resource(s)? (y/N): ", len(allMatches))
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	// Collect only projects that had matches
	matchedProjects := make(map[string]bool)
	for _, m := range allMatches {
		matchedProjects[m.projectID] = true
	}

	// Second pass: delete only in projects that had matches
	for _, projectID := range projectIDs {
		if !matchedProjects[projectID] {
			continue
		}
		var resourcePath string
		switch scheme {
		case "bq":
			resourcePath = "bq://" + projectID
			if rest != "" {
				resourcePath += "." + rest
			}
		case "iam":
			resourcePath = "iam://" + projectID
			if rest != "" {
				resourcePath += "/" + rest
			}
		default:
			resourcePath = scheme + "://"
			if rest != "" {
				resourcePath += rest
			}
		}

		res, err := factory.Create(resourcePath)
		if err != nil {
			continue
		}

		options := &resource.RemoveOptions{
			Force:   true, // Already confirmed above
			Verbose: verbose,
			Project: projectID,
			Region:  cfg.Defaults.Region,
		}

		if err := res.Remove(ctx, resourcePath, options); err != nil {
			fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
		}
	}

	return nil
}

func init() {
	// Add flags
	rmCmd.Flags().BoolVarP(&rmRecursive, "recursive", "r", false, "remove directories and their contents recursively")
	rmCmd.Flags().BoolVarP(&rmForce, "force", "f", false, "force removal without confirmation")

	// Add to root command
	rootCmd.AddCommand(rmCmd)
}
