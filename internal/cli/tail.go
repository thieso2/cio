package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/logging"
	"github.com/spf13/cobra"
	"github.com/thieso2/cio/cloudrun"
	"github.com/thieso2/cio/compute"
	"github.com/thieso2/cio/dataflow"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var (
	tailFollow   bool
	tailNumLines int
	tailAudit    bool
	tailSeverity string
	tailLogType  string
)

var tailCmd = &cobra.Command{
	Use:   "tail [-f] [-n N] <path>",
	Short: "Show or stream Cloud Run / Dataflow / VM logs",
	Long: `Show recent logs or stream live logs for Cloud Run services, jobs, workers, Dataflow jobs, or VM instances.

Paths:
  svc://service-name              Cloud Run service logs
  jobs://job-name                 Logs for all executions of a job
  jobs://job-name/execution-id    Logs for a specific execution
  worker://pool-name              Cloud Run worker pool logs
  dataflow://job-id               Dataflow job logs
  vm://zone/instance-name         VM Cloud Logging output
  vm://zone/instance-name/serial  VM serial port output
  vm://*/pattern*                 VM logs across all zones (wildcard)

Dataflow log types (--log-type):
  all      All log types with [J]/[W]/[S] prefix (default)
  job      Job-level orchestration logs
  worker   Worker infrastructure logs
  step     Application/transform logs

Examples:
  # Show last 50 log lines from a service
  cio tail svc://my-service

  # Stream live logs from all executions of a job
  cio tail -f jobs://archived-metrics-importer

  # Show Dataflow job logs
  cio tail dataflow://2024-01-15_12_00_00-12345

  # Stream only step logs from a Dataflow job
  cio tail -f --log-type step dataflow://2024-01-15_12_00_00-12345

  # Show VM Cloud Logging output
  cio tail vm://europe-west3-a/my-instance

  # Stream logs from VMs matching a pattern (all zones)
  cio tail -f 'vm://*/ingress*'

  # Stream VM serial port output
  cio tail -f vm://europe-west3-a/my-instance/serial`,
	Args: cobra.ExactArgs(1),
	RunE: runTail,
}

var showCmd = &cobra.Command{
	Use:   "show <path>",
	Short: "Show recent Cloud Run / Dataflow logs (alias for tail without -f)",
	Long: `Show recent log lines for Cloud Run services, jobs, workers, or Dataflow jobs.
Equivalent to 'cio tail -n N <path>'.

See 'cio tail --help' for path format and examples.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		tailFollow = false
		return runTail(cmd, args)
	},
}

func runTail(cmd *cobra.Command, args []string) error {
	inputPath := args[0]

	// Resolve alias if needed
	r := resolver.Create(cfg)
	var err error
	if !resolver.IsCloudRunPath(inputPath) && !resolver.IsDataflowPath(inputPath) && !resolver.IsVMPath(inputPath) {
		inputPath, err = r.Resolve(inputPath)
		if err != nil {
			return err
		}
	}

	// Dispatch to Dataflow handler if applicable.
	if resolver.IsDataflowPath(inputPath) {
		return runDataflowTail(inputPath)
	}

	// Dispatch to VM handler if applicable.
	if resolver.IsVMPath(inputPath) {
		return runVMTail(inputPath)
	}

	crPath := inputPath
	if !resolver.IsCloudRunPath(crPath) {
		return fmt.Errorf("tail only supports Cloud Run (svc://, jobs://, worker://), Dataflow (dataflow://), and VM (vm://) paths, got: %s", crPath)
	}

	projectID := cfg.Defaults.ProjectID
	if projectID == "" {
		return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}
	region := cfg.Defaults.Region

	scheme, name, execution := parseTailPath(crPath)

	// When the job name contains wildcards, expand to concrete names first —
	// Cloud Logging filters cannot handle glob patterns.
	var filter string
	var matchedJobs []string // non-nil only when wildcard was expanded
	if scheme == "jobs" && strings.ContainsAny(name, "*?") {
		ctx0 := context.Background()
		jobs, err := cloudrun.ListJobs(ctx0, projectID, region)
		if err != nil {
			return fmt.Errorf("expanding job wildcard: %w", err)
		}
		for _, j := range jobs {
			if ok, _ := path.Match(name, j.Name); ok {
				matchedJobs = append(matchedJobs, j.Name)
			}
		}
		if len(matchedJobs) == 0 {
			return fmt.Errorf("no jobs match pattern %q", name)
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "Matched jobs: %s\n", strings.Join(matchedJobs, ", "))
		}
		filter = cloudrun.LogFilterMultiJob(region, matchedJobs, execution, tailAudit, tailSeverity)
	} else {
		filter = cloudrun.LogFilter(projectID, region, scheme, name, execution, tailAudit, tailSeverity)
	}

	// Derive display prefix and whether it should be fixed (not overridden by labels).
	// Derive display prefix and whether it should be fixed (not overridden by labels).
	// - audit mode, wildcard: show job name from resource labels per entry
	// - audit mode, single job: fixed job name
	// - wildcard job, no execution: map execution label → job name via knownJobs
	// - all executions ("*"): show execution_name label directly
	// - specific execution: fixed execution id as prefix
	// - single job, no execution: fixed job name
	logPrefix := name
	fixedPrefix := true
	if tailAudit {
		if len(matchedJobs) > 0 {
			// Wildcard expanded in audit mode — derive job name from resource labels.
			logPrefix = ""
			fixedPrefix = false
		}
	} else {
		if execution != "" && execution != "*" {
			// Specific execution: show its id as fixed prefix.
			logPrefix = execution
			fixedPrefix = true
		} else {
			// No execution or wildcard: show execution_name label from each entry.
			// For wildcard jobs, SetKnownJobs maps execution names back to job names.
			logPrefix = ""
			fixedPrefix = false
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Filter: %s\n", filter)
	}

	// Single formatter shared by historical print and live stream so column
	// widths accumulated during history are preserved in streaming mode.
	f := cloudrun.NewLogFormatter(logPrefix, fixedPrefix)
	if len(matchedJobs) > 0 && (execution == "" || tailAudit) {
		f.SetKnownJobs(matchedJobs)
	}

	ctx := context.Background()

	// Fetch historical lines: n lines per job when wildcard was expanded,
	// otherwise n lines total.
	var entries []*logging.Entry
	if len(matchedJobs) > 1 {
		entries, err = cloudrun.FetchLogsMultiJob(ctx, projectID, region, matchedJobs, execution, tailNumLines, tailAudit, tailSeverity)
	} else {
		entries, err = cloudrun.FetchLogs(ctx, projectID, filter, tailNumLines)
	}
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}
	cloudrun.PrintLogs(entries, f)

	if !tailFollow {
		if len(entries) == 0 {
			fmt.Fprintln(os.Stderr, "No log entries found.")
		}
		return nil
	}

	// Then stream live using the same formatter.
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

	return cloudrun.StreamLogs(ctx, projectID, filter, f)
}

// looksLikeJobID returns true if the string looks like a Dataflow job ID
// (e.g., "2026-03-05_09_04_02-16947321753236499583") rather than a job name.
// Job IDs start with a date pattern and contain underscores.
func looksLikeJobID(s string) bool {
	// Job IDs typically look like: YYYY-MM-DD_HH_MM_SS-NNNNN
	return len(s) > 20 && s[4] == '-' && s[7] == '-' && s[10] == '_'
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

// runDataflowTail handles tail/show for Dataflow paths.
func runDataflowTail(dfPath string) error {
	projectID := cfg.Defaults.ProjectID
	if projectID == "" {
		return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}

	// Parse dataflow://job-id-or-name
	jobIDOrName := strings.TrimPrefix(dfPath, "dataflow://")
	jobIDOrName = strings.TrimRight(jobIDOrName, "/")
	if jobIDOrName == "" {
		return fmt.Errorf("dataflow job ID or name is required (e.g., dataflow://2024-01-15_12_00_00-12345 or dataflow://my-job-name)")
	}

	region := cfg.Defaults.Region
	if region == "" {
		return fmt.Errorf("region is required for Dataflow (use --region flag or set defaults.region in config)")
	}

	// Resolve job name to ID if it doesn't look like a Dataflow job ID.
	// Job IDs contain underscores and dashes in a specific pattern (e.g., 2024-01-15_12_00_00-12345).
	jobID := jobIDOrName
	ctx := context.Background()
	if !looksLikeJobID(jobIDOrName) {
		if verbose {
			fmt.Fprintf(os.Stderr, "Resolving job name %q to job ID...\n", jobIDOrName)
		}
		job, err := dataflow.FindJobByName(ctx, projectID, region, jobIDOrName)
		if err != nil {
			return err
		}
		jobID = job.ID
		if verbose {
			fmt.Fprintf(os.Stderr, "Resolved to job ID: %s (state: %s)\n", jobID, job.State)
		}
	}

	// Validate log type
	lt := dataflow.LogType(tailLogType)
	switch lt {
	case dataflow.LogTypeAll, dataflow.LogTypeJob, dataflow.LogTypeWorker, dataflow.LogTypeStep:
	default:
		return fmt.Errorf("invalid --log-type %q, must be one of: %s", tailLogType, strings.Join(dataflow.ValidLogTypes(), ", "))
	}

	showPrefix := lt == dataflow.LogTypeAll
	f := dataflow.NewLogFormatter(showPrefix)

	if verbose {
		filters := dataflow.LogFilters(projectID, jobID, lt, tailSeverity)
		for flt, filter := range filters {
			fmt.Fprintf(os.Stderr, "Filter [%s]: %s\n", flt, filter)
		}
	}

	entries, err := dataflow.FetchLogs(ctx, projectID, jobID, lt, tailSeverity, tailNumLines)
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}
	dataflow.PrintLogs(entries, f)

	if !tailFollow {
		if len(entries) == 0 {
			fmt.Fprintln(os.Stderr, "No log entries found.")
		}
		return nil
	}

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

	return dataflow.StreamLogs(ctx, projectID, jobID, lt, tailSeverity, f)
}

// runVMTail handles tail/show for VM paths.
// Supports wildcards — resolves to matching instances, then tails all of them.
//
//	vm://zone/instance-name         → Cloud Logging output (single VM)
//	vm://zone/instance-name/serial  → serial port output (single VM only)
//	vm://*/pattern*                 → Cloud Logging for matching VMs across all zones
//	vm://zone/web-*                 → Cloud Logging for matching VMs in zone
func runVMTail(vmPath string) error {
	projectID := cfg.Defaults.ProjectID
	if projectID == "" {
		return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}

	// Check for /serial suffix before matching
	isSerial := strings.HasSuffix(vmPath, "/serial")
	lookupPath := vmPath
	if isSerial {
		lookupPath = strings.TrimSuffix(vmPath, "/serial")
	}

	// Resolve wildcards to concrete instances
	ctx := context.Background()
	matched, err := resource.MatchVMInstances(ctx, lookupPath, projectID)
	if err != nil {
		return err
	}
	if len(matched) == 0 {
		return fmt.Errorf("no VM instances match %s", lookupPath)
	}

	// Serial port output: only supported for a single instance
	if isSerial {
		if len(matched) > 1 {
			return fmt.Errorf("serial port output only supports a single instance, but %d matched", len(matched))
		}
		return runVMSerialTail(projectID, matched[0].Zone, matched[0].Name)
	}

	// Cloud Logging: build filter for one or many instances
	var names []string
	for _, inst := range matched {
		names = append(names, inst.Name)
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "Matched VMs: %s\n", strings.Join(names, ", "))
	}

	return runVMCloudLogTail(projectID, matched)
}

// runVMSerialTail fetches and optionally follows serial port output.
func runVMSerialTail(projectID, zone, instanceName string) error {
	ctx := context.Background()

	content, next, err := compute.GetSerialPortOutput(ctx, projectID, zone, instanceName, 0)
	if err != nil {
		return fmt.Errorf("failed to get serial port output: %w", err)
	}

	lines := strings.Split(content, "\n")
	if len(lines) > tailNumLines {
		lines = lines[len(lines)-tailNumLines:]
	}
	for _, line := range lines {
		if line != "" {
			fmt.Println(line)
		}
	}

	if !tailFollow {
		return nil
	}

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

	fmt.Fprintf(os.Stderr, "Streaming serial port output... (Ctrl+C to stop)\n")
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}

		content, newNext, err := compute.GetSerialPortOutput(ctx, projectID, zone, instanceName, next)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to get serial port output: %w", err)
		}
		if newNext > next && content != "" {
			fmt.Print(content)
		}
		next = newNext
	}
}

// vmLogFilter builds a Cloud Logging filter for one or more VM instances.
func vmLogFilter(instances []*compute.InstanceInfo, severity string) string {
	var parts []string
	parts = append(parts, `resource.type="gce_instance"`)

	if len(instances) == 1 {
		inst := instances[0]
		parts = append(parts, fmt.Sprintf(`labels.instance_name="%s"`, inst.Name))
	} else {
		var nameFilters []string
		for _, inst := range instances {
			nameFilters = append(nameFilters, fmt.Sprintf(`labels.instance_name="%s"`, inst.Name))
		}
		parts = append(parts, "("+strings.Join(nameFilters, " OR ")+")")
	}

	if severity != "" {
		parts = append(parts, fmt.Sprintf(`severity>=%s`, strings.ToUpper(severity)))
	}
	return strings.Join(parts, " AND ")
}

// runVMCloudLogTail fetches and optionally streams Cloud Logging entries for VM instances.
func runVMCloudLogTail(projectID string, instances []*compute.InstanceInfo) error {
	filter := vmLogFilter(instances, tailSeverity)

	if verbose {
		fmt.Fprintf(os.Stderr, "Filter: %s\n", filter)
	}

	// Use instance name as prefix label; for single VM use fixed prefix.
	singleVM := len(instances) == 1
	prefix := ""
	if singleVM {
		prefix = instances[0].Name
	}
	f := cloudrun.NewLogFormatter(prefix, singleVM)
	if !singleVM {
		f.SetLabelKeys([]string{"instance_name"})
	}

	ctx := context.Background()

	entries, err := cloudrun.FetchLogs(ctx, projectID, filter, tailNumLines)
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}
	cloudrun.PrintLogs(entries, f)

	if !tailFollow {
		if len(entries) == 0 {
			fmt.Fprintln(os.Stderr, "No log entries found.")
		}
		return nil
	}

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

	return cloudrun.StreamLogs(ctx, projectID, filter, f)
}

func init() {
	tailCmd.Flags().BoolVarP(&tailFollow, "follow", "f", false, "stream live logs (follow mode)")
	tailCmd.Flags().IntVarP(&tailNumLines, "lines", "n", 50, "number of lines to show")
	tailCmd.Flags().BoolVar(&tailAudit, "audit", false, "show Cloud Audit logs (job-level events: created, updated, deleted)")
	tailCmd.Flags().StringVarP(&tailSeverity, "severity", "s", "", "minimum severity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
	tailCmd.Flags().StringVar(&tailLogType, "log-type", "all", "Dataflow log type: all, job, worker, step")
	rootCmd.AddCommand(tailCmd)

	showCmd.Flags().IntVarP(&tailNumLines, "lines", "n", 50, "number of lines to show")
	showCmd.Flags().BoolVar(&tailAudit, "audit", false, "show Cloud Audit logs (job-level events: created, updated, deleted)")
	showCmd.Flags().StringVarP(&tailSeverity, "severity", "s", "", "minimum severity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
	showCmd.Flags().StringVar(&tailLogType, "log-type", "all", "Dataflow log type: all, job, worker, step")
	rootCmd.AddCommand(showCmd)
}
