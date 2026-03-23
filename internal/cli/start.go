package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var startForce bool

var startCmd = &cobra.Command{
	Use:   "start <sql-path>",
	Short: "Start Cloud SQL instances",
	Long: `Start one or more stopped Cloud SQL instances.

Stopped instances are started in parallel. Already-running instances are skipped.

Examples:
  # Start a single instance
  cio start sql://my-instance

  # Start instances matching a pattern
  cio start 'sql://staging-*'

  # Force start without confirmation
  cio start -f sql://my-instance`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		r := resolver.Create(cfg)
		var fullPath string
		var err error

		if resolver.IsCloudSQLPath(path) {
			fullPath = path
		} else {
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
		}

		if !resolver.IsCloudSQLPath(fullPath) {
			return fmt.Errorf("start only supports Cloud SQL paths (sql://), got: %s", fullPath)
		}

		ctx := context.Background()
		matched, err := resource.MatchCloudSQLInstances(ctx, fullPath, cfg.Defaults.ProjectID)
		if err != nil {
			return err
		}
		return resource.StartCloudSQLInstances(ctx, cfg.Defaults.ProjectID, matched, startForce)
	},
}

func init() {
	startCmd.Flags().BoolVarP(&startForce, "force", "f", false, "start without confirmation")
	rootCmd.AddCommand(startCmd)
}
