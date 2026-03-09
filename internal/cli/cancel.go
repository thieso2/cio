package cli

import (
	"context"
	"fmt"

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
  cio cancel -f 'jobs://my-job/*'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

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

func init() {
	cancelCmd.Flags().BoolVarP(&cancelForce, "force", "f", false, "cancel without confirmation")
	rootCmd.AddCommand(cancelCmd)
}
