package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var stopForce bool

var stopCmd = &cobra.Command{
	Use:   "stop <path>",
	Short: "Stop VM or Cloud SQL instances",
	Long: `Stop one or more Compute Engine VM or Cloud SQL instances.

Running instances are stopped in parallel. Already-stopped instances are skipped.

Examples:
  # Stop a VM instance
  cio stop vm://europe-west3-a/my-instance

  # Stop VMs matching a pattern (all zones)
  cio stop 'vm://*/bastion-ephemeral*'

  # Stop a Cloud SQL instance
  cio stop sql://my-instance

  # Stop Cloud SQL instances matching a pattern
  cio stop 'sql://staging-*'

  # Force stop without confirmation
  cio stop -f sql://my-instance`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Check for discover mode: scheme:/project-pattern/rest
		if projectPattern, scheme, rest, ok := parseDiscoverPath(path); ok {
			return runDiscoverStop(scheme, projectPattern, rest)
		}

		r := resolver.Create(cfg)
		var fullPath string
		var err error

		if resolver.IsVMPath(path) || resolver.IsCloudSQLPath(path) {
			fullPath = path
		} else {
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
		}

		ctx := context.Background()

		if resolver.IsCloudSQLPath(fullPath) {
			matched, err := resource.MatchCloudSQLInstances(ctx, fullPath, cfg.Defaults.ProjectID)
			if err != nil {
				return err
			}
			return resource.StopCloudSQLInstances(ctx, cfg.Defaults.ProjectID, matched, stopForce)
		}

		if resolver.IsVMPath(fullPath) {
			matched, err := resource.MatchVMInstances(ctx, fullPath, cfg.Defaults.ProjectID)
			if err != nil {
				return err
			}
			return resource.StopVMInstances(ctx, cfg.Defaults.ProjectID, matched, stopForce)
		}

		return fmt.Errorf("stop supports VM (vm://) and Cloud SQL (sql://) paths, got: %s", fullPath)
	},
}

func runDiscoverStop(scheme, projectPattern, rest string) error {
	ctx := context.Background()

	projectIDs, err := resource.ListProjectIDs(ctx, projectPattern)
	if err != nil {
		return err
	}
	if len(projectIDs) == 0 {
		fmt.Printf("No projects matching %s\n", projectPattern)
		return nil
	}

	for _, projectID := range projectIDs {
		resourcePath := scheme + "://"
		if rest != "" {
			resourcePath += rest
		}

		switch scheme {
		case "vm":
			matched, err := resource.MatchVMInstances(ctx, resourcePath, projectID)
			if err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
				continue
			}
			if len(matched) == 0 {
				continue
			}
			// Prefix names for display
			for _, m := range matched {
				m.Name = projectID + ":" + m.Name
			}
			if err := resource.StopVMInstances(ctx, projectID, matched, stopForce); err != nil {
				fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
			}
		case "sql":
			matched, err := resource.MatchCloudSQLInstances(ctx, resourcePath, projectID)
			if err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
				continue
			}
			if len(matched) == 0 {
				continue
			}
			for _, m := range matched {
				m.Name = projectID + ":" + m.Name
			}
			if err := resource.StopCloudSQLInstances(ctx, projectID, matched, stopForce); err != nil {
				fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
			}
		default:
			return fmt.Errorf("stop discover mode not supported for %s://", scheme)
		}
	}
	return nil
}

func init() {
	stopCmd.Flags().BoolVarP(&stopForce, "force", "f", false, "stop without confirmation")
	rootCmd.AddCommand(stopCmd)
}
