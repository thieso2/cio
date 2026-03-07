package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var stopForce bool

var stopCmd = &cobra.Command{
	Use:   "stop <vm-path>",
	Short: "Stop VM instances",
	Long: `Stop one or more Compute Engine VM instances.

Running instances are stopped in parallel. Already-stopped instances are skipped.

Examples:
  # Stop a single instance
  cio stop vm://europe-west3-a/my-instance

  # Stop instances matching a pattern (all zones)
  cio stop 'vm://*/bastion-ephemeral*'

  # Stop instances in a specific zone
  cio stop 'vm://europe-west3-a/staging-*'

  # Force stop without confirmation
  cio stop -f 'vm://*/bastion-ephemeral*'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Resolve alias if needed
		r := resolver.Create(cfg)
		var fullPath string
		var err error

		if resolver.IsVMPath(path) {
			fullPath = path
		} else {
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
		}

		if !resolver.IsVMPath(fullPath) {
			return fmt.Errorf("stop only supports VM paths (vm://), got: %s", fullPath)
		}

		ctx := context.Background()

		matched, err := resource.MatchVMInstances(ctx, fullPath, cfg.Defaults.ProjectID)
		if err != nil {
			return err
		}

		return resource.StopVMInstances(ctx, cfg.Defaults.ProjectID, matched, stopForce)
	},
}

func init() {
	stopCmd.Flags().BoolVarP(&stopForce, "force", "f", false, "stop without confirmation")
	rootCmd.AddCommand(stopCmd)
}
