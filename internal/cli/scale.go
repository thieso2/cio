package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/cloudrun"
	"github.com/thieso2/cio/resolver"
)

var scaleCmd = &cobra.Command{
	Use:   "scale <worker-pool> <instance-count>",
	Short: "Scale worker pool instances",
	Long: `Change the number of instances for a Cloud Run worker pool.

Examples:
  # Scale to 3 instances
  cio scale worker://iomp-processor 3

  # Scale to zero
  cio scale worker://iomp-processor 0`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]
		count, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid instance count: %s", args[1])
		}

		r := resolver.Create(cfg)
		var fullPath string

		if resolver.IsWorkerPath(path) {
			fullPath = path
		} else {
			fullPath, err = r.Resolve(path)
			if err != nil {
				return err
			}
		}

		if !resolver.IsWorkerPath(fullPath) {
			return fmt.Errorf("scale only supports worker pool paths (worker://), got: %s", fullPath)
		}

		// Extract worker pool name from worker://name
		name := fullPath[len("worker://"):]
		if name == "" {
			return fmt.Errorf("worker pool name required")
		}

		ctx := context.Background()
		err = cloudrun.UpdateWorkerPoolInstances(ctx, cfg.Defaults.ProjectID, cfg.Defaults.Region, name, int32(count))
		if err != nil {
			return err
		}

		fmt.Printf("Scaled %s to %d instances\n", name, count)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(scaleCmd)
}
