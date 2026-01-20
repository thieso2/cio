package cli

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/config"
)

var (
	// Global flags
	cfgFile     string
	projectID   string
	region      string
	verbose     bool
	parallelism int // Number of concurrent operations (cp/rm)

	// Global config instance
	cfg *config.Config

	// Version information
	versionInfo = VersionInfo{
		Version: "dev",
		Commit:  "none",
		Date:    "unknown",
		BuiltBy: "unknown",
	}
)

// VersionInfo holds version information
type VersionInfo struct {
	Version string
	Commit  string
	Date    string
	BuiltBy string
}

// SetVersionInfo sets the version information
func SetVersionInfo(version, commit, date, builtBy string) {
	versionInfo.Version = version
	versionInfo.Commit = commit
	versionInfo.Date = date
	versionInfo.BuiltBy = builtBy
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "cio",
	Short:         "Cloud IO - A fast CLI for Google Cloud Storage and BigQuery",
	Version:       versionInfo.Version,
	SilenceUsage:  true, // Don't show usage on errors
	SilenceErrors: true, // Don't print errors (main.go handles this)
	Long: `cio (Cloud IO) is a CLI tool that replaces common gcloud storage and bq commands
with short aliases and provides a FUSE filesystem for browsing Google Cloud resources.

Aliases are prefixed with : to distinguish them from regular paths.

Examples:
  # Create a mapping
  cio map am gs://io-spooler-onprem-archived-metrics/
  cio map mydata bq://project-id.dataset

  # List bucket contents
  cio ls :am

  # List BigQuery tables
  cio ls :mydata

  # List with details
  cio ls -l :am/2024/

  # List recursively with human-readable sizes
  cio ls -lr --human-readable :am/2024/`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		cfg, err = config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Override config with flags if provided
		if projectID != "" {
			cfg.Defaults.ProjectID = projectID
		}
		if region != "" {
			cfg.Defaults.Region = region
		}

		// Handle parallelism configuration priority:
		// 1. Command-line flag (if not default)
		// 2. Environment variable CIO_PARALLEL
		// 3. Config file value
		// 4. Default value (50)
		if cmd.Flags().Changed("parallel") {
			// Flag was explicitly set, use it
			cfg.Defaults.Parallelism = parallelism
		} else if envParallel := os.Getenv("CIO_PARALLEL"); envParallel != "" {
			// Try environment variable
			if val, err := strconv.Atoi(envParallel); err == nil {
				cfg.Defaults.Parallelism = val
			}
		}
		// Otherwise use config file value or default (already set in config)

		// Validate config
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid configuration: %w", err)
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "Config loaded from: %s\n", cfg.GetFilePath())
			fmt.Fprintf(os.Stderr, "Project: %s\n", cfg.Defaults.ProjectID)
			fmt.Fprintf(os.Stderr, "Region: %s\n", cfg.Defaults.Region)
			fmt.Fprintf(os.Stderr, "Parallelism: %d\n", cfg.Defaults.Parallelism)
		}

		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ~/.config/cio/config.yaml)")
	rootCmd.PersistentFlags().StringVar(&projectID, "project", "", "GCP project ID (overrides config)")
	rootCmd.PersistentFlags().StringVar(&region, "region", "", "GCP region (overrides config)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().IntVarP(&parallelism, "parallel", "j", 50, "number of parallel operations for cp/rm (1-200, can also be set via CIO_PARALLEL env var or config file)")
}

// GetConfig returns the global config instance
func GetConfig() *config.Config {
	return cfg
}

// GetParallelism returns the configured parallelism level
// Returns a value between 1 and 200
func GetParallelism() int {
	// Use the config value which has already been resolved from flag/env/config
	val := cfg.Defaults.Parallelism
	if val < config.MinParallelism {
		return config.MinParallelism
	}
	if val > config.MaxParallelism {
		return config.MaxParallelism
	}
	return val
}
