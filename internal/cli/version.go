package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  `Print version information including version, commit hash, build date, and builder.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("cio version %s\n", versionInfo.Version)
		fmt.Printf("  commit: %s\n", versionInfo.Commit)
		fmt.Printf("  built:  %s\n", versionInfo.Date)
		fmt.Printf("  by:     %s\n", versionInfo.BuiltBy)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
