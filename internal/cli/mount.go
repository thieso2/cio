package cli

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	fusepkg "github.com/thieso2/cio/internal/fuse"
)

var (
	mountDebug    bool
	mountReadOnly bool
	mountOptions  string // Comma-separated mount options (e.g., "allow_other,default_permissions")
	logGCS        bool   // Log GCS API calls with timing
	cleanCache    bool   // Clear metadata cache on startup
)

var mountCmd = &cobra.Command{
	Use:   "mount <mountpoint>",
	Short: "Mount GCP resources as a FUSE filesystem",
	Long: `Mount GCP resources as a FUSE filesystem for browsing with standard Unix tools.

The filesystem structure is:
  <mountpoint>/
    └─ <project-id>/
         ├─ storage/    (GCS buckets and objects)
         ├─ bigquery/   (datasets and tables)
         └─ pubsub/     (topics and subscriptions)

Examples:
  # Mount with default project
  cio mount /mnt/gcp

  # Mount with specific project
  cio mount --project my-project /mnt/gcp

  # Mount with debug logging
  cio mount --debug /mnt/gcp

  # Mount in read-only mode
  cio mount --read-only /mnt/gcp

  # Mount with FUSE options (macOS/macFUSE)
  cio mount -o allow_other,default_permissions /mnt/gcp

  # Mount with GCS logging and clean cache
  cio mount --log-gcs --clean-cache /mnt/gcp

  # Common FUSE options:
  #   allow_other           - Allow other users to access
  #   default_permissions   - Enable kernel permission checking
  #   ro                    - Read-only mount

To unmount:
  umount /mnt/gcp
  # or
  fusermount -u /mnt/gcp`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		mountpoint := args[0]

		// Get configuration (for project ID)
		cfg := GetConfig()
		if cfg.Defaults.ProjectID == "" {
			return fmt.Errorf("project ID is required (use --project flag or set in config)")
		}

		// Parse mount options
		var mountOpts []string
		if mountOptions != "" {
			mountOpts = strings.Split(mountOptions, ",")
			// Trim spaces from each option
			for i, opt := range mountOpts {
				mountOpts[i] = strings.TrimSpace(opt)
			}
		}

		// Create mount options
		opts := fusepkg.MountOptions{
			ProjectID:  cfg.Defaults.ProjectID,
			Debug:      mountDebug,
			ReadOnly:   mountReadOnly,
			MountOpts:  mountOpts,
			LogGCS:     logGCS,
			CleanCache: cleanCache,
		}

		// Mount the filesystem
		fmt.Printf("Mounting GCP filesystem at %s (project: %s)...\n", mountpoint, cfg.Defaults.ProjectID)
		server, err := fusepkg.Mount(mountpoint, opts)
		if err != nil {
			return fmt.Errorf("failed to mount: %w", err)
		}

		fmt.Printf("Filesystem mounted successfully. Press Ctrl+C to unmount.\n")

		// Set up signal handling for graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		// Wait for signal or unmount
		go func() {
			<-sigChan
			fmt.Printf("\nUnmounting filesystem...\n")
			if err := server.Unmount(); err != nil {
				fmt.Fprintf(os.Stderr, "Error during unmount: %v\n", err)
			}
		}()

		// Block until unmounted
		server.Wait()
		fmt.Printf("Filesystem unmounted.\n")

		return nil
	},
}

func init() {
	mountCmd.Flags().BoolVar(&mountDebug, "debug", false, "Enable debug logging")
	mountCmd.Flags().BoolVar(&mountReadOnly, "read-only", false, "Mount filesystem in read-only mode")
	mountCmd.Flags().StringVarP(&mountOptions, "options", "o", "", "Comma-separated FUSE mount options (e.g., allow_other,default_permissions)")
	mountCmd.Flags().BoolVar(&logGCS, "log-gcs", false, "Log GCS API calls with timing information")
	mountCmd.Flags().BoolVar(&cleanCache, "clean-cache", false, "Clear metadata cache on startup")
	rootCmd.AddCommand(mountCmd)
}
