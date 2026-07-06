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
	Use:     "stop <path>",
	Aliases: []string{"disable", "pause"},
	Short:   "Stop VM/Cloud SQL instances or disable Cloud Scheduler jobs",
	Long: `Stop one or more Compute Engine VM or Cloud SQL instances, or pause
(disable) Cloud Scheduler jobs.

Instances/jobs are processed in parallel. Already-stopped instances and
already-paused jobs are skipped. 'disable' and 'pause' are aliases for stop.

Examples:
  # Stop a VM instance
  cio stop vm://europe-west3-a/my-instance

  # Stop VMs matching a pattern (all zones)
  cio stop 'vm://*/bastion-ephemeral*'

  # Stop a Cloud SQL instance
  cio stop sql://my-instance

  # Stop Cloud SQL instances matching a pattern
  cio stop 'sql://staging-*'

  # Disable (pause) a Cloud Scheduler job
  cio stop scheduler://my-job
  cio disable scheduler://my-job

  # Disable scheduler jobs matching a pattern
  cio disable 'scheduler://nightly-*'

  # Force stop without confirmation
  cio stop -f sql://my-instance`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Check for discover mode: scheme:/project-pattern/rest
		if projectPattern, scheme, rest, ok := parseDiscoverPath(path); ok {
			return runDiscoverStop(scheme, projectPattern, rest)
		}

		_, fullPath, _, err := resolveInput(path)
		if err != nil {
			return err
		}

		ctx := context.Background()

		if resolver.IsSchedulerPath(fullPath) {
			matched, err := resource.MatchSchedulerJobs(ctx, fullPath, cfg.Defaults.ProjectID, cfg.Defaults.Region)
			if err != nil {
				return err
			}
			return resource.PauseSchedulerJobs(ctx, cfg.Defaults.ProjectID, cfg.Defaults.Region, matched, stopForce)
		}

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

		return fmt.Errorf("stop supports VM (vm://), Cloud SQL (sql://), and Cloud Scheduler (scheduler://) paths, got: %s", fullPath)
	},
}

func runDiscoverStop(scheme, projectPattern, rest string) error {
	ctx := context.Background()

	return forEachDiscoveredProject(ctx, scheme, projectPattern, rest, func(projectID, resourcePath string) error {
		// Names must stay raw here — the stop/pause API calls use them. Project
		// context comes from a header line instead of prefixing each name.
		switch scheme {
		case "vm":
			matched, err := resource.MatchVMInstances(ctx, resourcePath, projectID)
			if err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
				return nil
			}
			if len(matched) == 0 {
				return nil
			}
			fmt.Printf("Project %s:\n", projectID)
			if err := resource.StopVMInstances(ctx, projectID, matched, stopForce); err != nil {
				fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
			}
		case "sql":
			matched, err := resource.MatchCloudSQLInstances(ctx, resourcePath, projectID)
			if err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
				return nil
			}
			if len(matched) == 0 {
				return nil
			}
			fmt.Printf("Project %s:\n", projectID)
			if err := resource.StopCloudSQLInstances(ctx, projectID, matched, stopForce); err != nil {
				fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
			}
		case "scheduler":
			matched, err := resource.MatchSchedulerJobs(ctx, resourcePath, projectID, cfg.Defaults.Region)
			if err != nil {
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
				}
				return nil
			}
			if len(matched) == 0 {
				return nil
			}
			fmt.Printf("Project %s:\n", projectID)
			if err := resource.PauseSchedulerJobs(ctx, projectID, cfg.Defaults.Region, matched, stopForce); err != nil {
				fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
			}
		default:
			return fmt.Errorf("stop discover mode not supported for %s://", scheme)
		}
		return nil
	})
}

func init() {
	stopCmd.Flags().BoolVarP(&stopForce, "force", "f", false, "stop without confirmation")
	rootCmd.AddCommand(stopCmd)
}
