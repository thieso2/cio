package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var cancelForce bool

var cancelCmd = &cobra.Command{
	Use:   "cancel <jobs-path>",
	Short: "Cancel running Cloud Run job executions",
	Long: `Cancel one or more running Cloud Run job executions.

Only Running or Pending executions can be cancelled. Already completed
or failed executions are skipped.

Examples:
  # Cancel a specific execution
  cio cancel jobs://my-job/my-job-execution-abc123

  # Cancel all running executions for a job
  cio cancel 'jobs://my-job/*'

  # Force cancel without confirmation
  cio cancel -f 'jobs://my-job/*'

  # Cancel across projects (discover mode)
  cio cancel 'jobs:/iom-*/sqlmesh*/*'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Check for discover mode: jobs:/project-pattern/rest
		if projectPattern, scheme, rest, ok := parseDiscoverPath(path); ok {
			if scheme != "jobs" {
				return fmt.Errorf("cancel only supports Cloud Run job paths (jobs://), got: %s://", scheme)
			}
			return runDiscoverCancel(projectPattern, rest)
		}

		r := resolver.Create(cfg)
		var fullPath string
		var err error

		if resolver.IsCloudRunPath(path) {
			fullPath = path
		} else {
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
		}

		if !resolver.IsJobsPath(fullPath) {
			return fmt.Errorf("cancel only supports Cloud Run job paths (jobs://), got: %s", fullPath)
		}

		ctx := context.Background()

		res := resource.CreateCloudRunResource(r.ReverseResolve)
		return res.Cancel(ctx, fullPath, &resource.RemoveOptions{
			Force:   cancelForce,
			Verbose: verbose,
			Project: cfg.Defaults.ProjectID,
			Region:  cfg.Defaults.Region,
		})
	},
}

func runDiscoverCancel(projectPattern, rest string) error {
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
		fmt.Fprintf(os.Stderr, "Discover: %d project(s) matching %s\n", len(projectIDs), projectPattern)
	}

	// rest is "jobPattern/*" or "jobPattern/executionPattern" or just "jobPattern"
	// Default execution pattern to "*" if not specified
	jobPattern := rest
	executionPattern := "*"
	if slashIdx := strings.LastIndex(rest, "/"); slashIdx >= 0 {
		jobPattern = rest[:slashIdx]
		executionPattern = rest[slashIdx+1:]
	}

	hasJobWildcard := resolver.HasWildcard(jobPattern)

	for _, projectID := range projectIDs {
		region := cfg.Defaults.Region

		if hasJobWildcard {
			// List all jobs and match against pattern
			res := resource.CreateCloudRunResource(func(p string) string { return p })
			jobs, err := res.List(ctx, "jobs://", &resource.ListOptions{
				ProjectID: projectID,
				Region:    region,
			})
			if err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
				continue
			}

			for _, job := range jobs {
				if !resolver.MatchPattern(job.Name, jobPattern) {
					continue
				}
				cancelPath := "jobs://" + job.Name + "/" + executionPattern
				if err := res.Cancel(ctx, cancelPath, &resource.RemoveOptions{
					Force:   cancelForce,
					Verbose: verbose,
					Project: projectID,
					Region:  region,
				}); err != nil {
					if verbose {
						fmt.Fprintf(os.Stderr, "Warning: %s/%s: %v\n", projectID, job.Name, err)
					}
				}
			}
		} else {
			// Concrete job name, cancel directly
			cancelPath := "jobs://" + jobPattern + "/" + executionPattern
			res := resource.CreateCloudRunResource(func(p string) string { return p })
			if err := res.Cancel(ctx, cancelPath, &resource.RemoveOptions{
				Force:   cancelForce,
				Verbose: verbose,
				Project: projectID,
				Region:  region,
			}); err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
			}
		}
	}
	return nil
}

func init() {
	cancelCmd.Flags().BoolVarP(&cancelForce, "force", "f", false, "cancel without confirmation")
	rootCmd.AddCommand(cancelCmd)
}
