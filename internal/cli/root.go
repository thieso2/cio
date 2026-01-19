package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/config"
)

var (
	// Global flags
	cfgFile   string
	projectID string
	region    string
	verbose   bool

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
	Use:     "cio",
	Short:   "Cloud IO - A fast CLI for Google Cloud Storage and BigQuery",
	Version: versionInfo.Version,
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

		// Validate config
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid configuration: %w", err)
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "Config loaded from: %s\n", cfg.GetFilePath())
			fmt.Fprintf(os.Stderr, "Project: %s\n", cfg.Defaults.ProjectID)
			fmt.Fprintf(os.Stderr, "Region: %s\n", cfg.Defaults.Region)
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
}

// GetConfig returns the global config instance
func GetConfig() *config.Config {
	return cfg
}
