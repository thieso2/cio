package cli

import (
	"context"
	"fmt"

	"os"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var startForce bool

var startCmd = &cobra.Command{
	Use:     "start <path>",
	Aliases: []string{"enable", "resume"},
	Short:   "Start Cloud SQL instances or enable Cloud Scheduler jobs",
	Long: `Start one or more stopped Cloud SQL instances, or resume (enable) paused
Cloud Scheduler jobs.

Instances/jobs are processed in parallel. Already-running instances and
already-enabled jobs are skipped. 'enable' and 'resume' are aliases for start.

Examples:
  # Start a single instance
  cio start sql://my-instance

  # Start instances matching a pattern
  cio start 'sql://staging-*'

  # Enable (resume) a Cloud Scheduler job
  cio start scheduler://my-job
  cio enable scheduler://my-job

  # Enable scheduler jobs matching a pattern
  cio enable 'scheduler://nightly-*'

  # Discover mode: enable scheduler jobs across projects
  cio enable 'scheduler:/iom-data*/'

  # Force start without confirmation
  cio start -f sql://my-instance`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Check for discover mode: scheme:/project-pattern/rest
		if projectPattern, scheme, rest, ok := parseDiscoverPath(path); ok {
			return runDiscoverStart(scheme, projectPattern, rest)
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
			return resource.ResumeSchedulerJobs(ctx, cfg.Defaults.ProjectID, cfg.Defaults.Region, matched, startForce)
		}

		if resolver.IsCloudSQLPath(fullPath) {
			matched, err := resource.MatchCloudSQLInstances(ctx, fullPath, cfg.Defaults.ProjectID)
			if err != nil {
				return err
			}
			return resource.StartCloudSQLInstances(ctx, cfg.Defaults.ProjectID, matched, startForce)
		}

		return fmt.Errorf("start supports Cloud SQL (sql://) and Cloud Scheduler (scheduler://) paths, got: %s", fullPath)
	},
}

func runDiscoverStart(scheme, projectPattern, rest string) error {
	ctx := context.Background()

	return forEachDiscoveredProject(ctx, scheme, projectPattern, rest, func(projectID, resourcePath string) error {
		// Names must stay raw here — the start/resume API calls use them. Project
		// context comes from a header line instead of prefixing each name.
		switch scheme {
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
			if err := resource.StartCloudSQLInstances(ctx, projectID, matched, startForce); err != nil {
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
			if err := resource.ResumeSchedulerJobs(ctx, projectID, cfg.Defaults.Region, matched, startForce); err != nil {
				fmt.Fprintf(os.Stderr, "Error in %s: %v\n", projectID, err)
			}
		default:
			return fmt.Errorf("start discover mode not supported for %s://", scheme)
		}
		return nil
	})
}

func init() {
	startCmd.Flags().BoolVarP(&startForce, "force", "f", false, "start without confirmation")
	rootCmd.AddCommand(startCmd)
}
