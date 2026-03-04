package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/cloudrun"
	"github.com/thieso2/cio/resolver"
)

var (
	tailFollow   bool
	tailNumLines int
)

var tailCmd = &cobra.Command{
	Use:   "tail [-f] [-n N] <path>",
	Short: "Show or stream Cloud Run logs",
	Long: `Show recent logs or stream live logs for Cloud Run services, jobs, or workers.

Paths:
  svc://service-name              Cloud Run service logs
  jobs://job-name                 Logs for all executions of a job
  jobs://job-name/execution-id    Logs for a specific execution
  worker://pool-name              Cloud Run worker pool logs

Examples:
  # Show last 50 log lines from a service
  cio tail svc://my-service

  # Stream live logs from all executions of a job
  cio tail -f jobs://archived-metrics-importer

  # Show logs for a specific execution
  cio tail jobs://archived-metrics-importer/archived-metrics-importer-lgb5r

  # Stream live logs for a specific execution
  cio tail -f jobs://archived-metrics-importer/archived-metrics-importer-lgb5r`,
	Args: cobra.ExactArgs(1),
	RunE: runTail,
}

var showCmd = &cobra.Command{
	Use:   "show <path>",
	Short: "Show recent Cloud Run logs (alias for tail without -f)",
	Long: `Show recent log lines for Cloud Run services, jobs, or workers.
Equivalent to 'cio tail -n N <path>'.

See 'cio tail --help' for path format and examples.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		tailFollow = false
		return runTail(cmd, args)
	},
}

func runTail(cmd *cobra.Command, args []string) error {
	path := args[0]

	// Resolve alias if needed
	r := resolver.Create(cfg)
	var err error
	if !resolver.IsCloudRunPath(path) {
		path, err = r.Resolve(path)
		if err != nil {
			return err
		}
	}

	if !resolver.IsCloudRunPath(path) {
		return fmt.Errorf("tail only supports Cloud Run paths (svc://, jobs://, worker://), got: %s", path)
	}

	projectID := cfg.Defaults.ProjectID
	if projectID == "" {
		return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}
	region := cfg.Defaults.Region

	scheme, name, execution := parseTailPath(path)
	filter := cloudrun.LogFilter(projectID, region, scheme, name, execution)

	// Derive display prefix and whether it should be fixed (not overridden by labels).
	// - job-level (execution == ""):  use fixed job name, labels not present anyway
	// - all executions (execution == "*"): no fixed prefix; let execution_name label show
	// - specific execution: use fixed execution id as prefix
	logPrefix := name
	fixedPrefix := true
	if execution == "*" {
		logPrefix = ""
		fixedPrefix = false
	} else if execution != "" {
		logPrefix = execution
		fixedPrefix = true
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Filter: %s\n", filter)
	}

	ctx := context.Background()

	// Always fetch historical lines first
	entries, err := cloudrun.FetchLogs(ctx, projectID, filter, tailNumLines)
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}
	cloudrun.PrintLogs(entries, logPrefix, fixedPrefix)

	if !tailFollow {
		if len(entries) == 0 {
			fmt.Fprintln(os.Stderr, "No log entries found.")
		}
		return nil
	}

	// Then stream live
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	go func() {
		<-sigChan
		fmt.Fprintln(os.Stderr, "\nInterrupted.")
		cancel()
	}()

	return cloudrun.StreamLogs(ctx, projectID, filter, logPrefix, fixedPrefix)
}

// parseTailPath splits a Cloud Run path into (scheme, name, execution).
// Trailing slashes are stripped so "jobs://my-job/" → name "my-job".
func parseTailPath(path string) (scheme, name, execution string) {
	path = strings.TrimRight(path, "/")
	for _, s := range []string{"svc", "jobs", "worker"} {
		prefix := s + "://"
		if strings.HasPrefix(path, prefix) {
			rest := strings.TrimPrefix(path, prefix)
			parts := strings.SplitN(rest, "/", 2)
			scheme = s
			name = parts[0]
			if len(parts) > 1 {
				execution = parts[1]
			}
			return
		}
	}
	return
}

func init() {
	tailCmd.Flags().BoolVarP(&tailFollow, "follow", "f", false, "stream live logs (follow mode)")
	tailCmd.Flags().IntVarP(&tailNumLines, "lines", "n", 50, "number of lines to show")
	rootCmd.AddCommand(tailCmd)

	showCmd.Flags().IntVarP(&tailNumLines, "lines", "n", 50, "number of lines to show")
	rootCmd.AddCommand(showCmd)
}
