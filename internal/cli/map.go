package cli

import (
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
)

var mapCmd = &cobra.Command{
	Use:   "map",
	Short: "Manage GCS and BigQuery path mappings",
	Long: `Manage alias mappings to GCS bucket paths and BigQuery datasets.

Mappings allow you to use short aliases instead of full gs:// or bq:// paths.

Examples:
  GCS:      mapping 'am' to 'gs://my-bucket/' lets you use 'cio ls :am'
  BigQuery: mapping 'mydata' to 'bq://project.dataset' lets you use 'cio ls :mydata'

Note: Aliases are created without the : prefix, but must be used with it.`,
}

var mapAddCmd = &cobra.Command{
	Use:   "map <alias> <path>",
	Short: "Create or update a mapping",
	Long: `Create or update an alias mapping to a GCS or BigQuery path.

Examples:
  # GCS mappings
  cio map am gs://io-spooler-onprem-archived-metrics/
  cio map logs gs://my-project-logs/
  cio map data gs://my-data-bucket/raw/

  # BigQuery mappings
  cio map mydata bq://my-project-id.my-dataset
  cio map analytics bq://prod-project.analytics_data`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		alias := args[0]
		path := args[1]

		// Validate alias
		if err := resolver.ValidateAlias(alias); err != nil {
			return err
		}

		// Validate path (supports both gs:// and bq://)
		if err := resolver.ValidateGCSPath(path); err != nil {
			return err
		}

		// Normalize path only for GCS (BigQuery doesn't need trailing slash)
		if resolver.IsGCSPath(path) {
			path = resolver.NormalizePath(path)
		}

		// Check if alias already exists
		if existingPath, exists := cfg.GetMapping(alias); exists {
			fmt.Printf("Updating mapping: %s\n", alias)
			fmt.Printf("  Old: %s\n", existingPath)
			fmt.Printf("  New: %s\n", path)
		} else {
			fmt.Printf("Creating mapping: %s -> %s\n", alias, path)
		}

		// Add mapping
		cfg.AddMapping(alias, path)

		// Save config
		if err := cfg.Save(); err != nil {
			return fmt.Errorf("failed to save config: %w", err)
		}

		fmt.Println("Mapping saved successfully")
		return nil
	},
}

var mapListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all mappings",
	Long:  `List all configured alias mappings.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mappings := cfg.ListMappings()

		if len(mappings) == 0 {
			fmt.Println("No mappings configured")
			fmt.Println("\nCreate a mapping with: cio map <alias> <gs-path>")
			return nil
		}

		// Sort aliases for consistent output
		aliases := make([]string, 0, len(mappings))
		for alias := range mappings {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)

		// Print mappings in a table
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ALIAS\tPATH")
		fmt.Fprintln(w, "-----\t----")

		for _, alias := range aliases {
			fmt.Fprintf(w, "%s\t%s\n", alias, mappings[alias])
		}

		w.Flush()
		return nil
	},
}

var mapShowCmd = &cobra.Command{
	Use:   "show <alias>",
	Short: "Show the full path for an alias",
	Long:  `Display the full GCS or BigQuery path for a given alias.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		alias := args[0]

		path, exists := cfg.GetMapping(alias)
		if !exists {
			return fmt.Errorf("alias %q not found", alias)
		}

		fmt.Println(path)
		return nil
	},
}

var mapDeleteCmd = &cobra.Command{
	Use:   "delete <alias>",
	Short: "Delete a mapping",
	Long:  `Remove an alias mapping from the configuration.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		alias := args[0]

		if !cfg.DeleteMapping(alias) {
			return fmt.Errorf("alias %q not found", alias)
		}

		if err := cfg.Save(); err != nil {
			return fmt.Errorf("failed to save config: %w", err)
		}

		fmt.Printf("Deleted mapping: %s\n", alias)
		return nil
	},
}

func init() {
	// Add subcommands to map command
	mapCmd.AddCommand(mapListCmd)
	mapCmd.AddCommand(mapShowCmd)
	mapCmd.AddCommand(mapDeleteCmd)

	// The main "map" command without subcommands acts as "map add"
	mapCmd.RunE = mapAddCmd.RunE
	mapCmd.Args = mapAddCmd.Args

	// Add map command to root
	rootCmd.AddCommand(mapCmd)
}
