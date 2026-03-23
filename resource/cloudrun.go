package resource

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/thieso2/cio/cloudrun"
	"github.com/thieso2/cio/dataflow"
)

const (
	TypeCloudRunService Type = "svc"
	TypeCloudRunJob     Type = "jobs"
	TypeCloudRunWorker  Type = "worker"
)

// CloudRunResource implements Resource for Cloud Run services, jobs, and worker pools.
type CloudRunResource struct {
	formatter  PathFormatter
	lastScheme string // set during List() to drive FormatLongHeader()
}

// CreateCloudRunResource creates a new Cloud Run resource handler.
func CreateCloudRunResource(formatter PathFormatter) *CloudRunResource {
	return &CloudRunResource{formatter: formatter}
}

// Type returns the resource type.
func (r *CloudRunResource) Type() Type { return TypeCloudRunService }

// SupportsInfo returns whether this resource type supports detailed info.
func (r *CloudRunResource) SupportsInfo() bool { return false }

// ParsePath parses a Cloud Run path into components.
func (r *CloudRunResource) ParsePath(path string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeCloudRunService}, nil
}

// List lists Cloud Run resources. Project and Region are taken from opts.
func (r *CloudRunResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project, region string
	if opts != nil {
		project = opts.ProjectID
		region = opts.Region
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for Cloud Run (use --project flag or set defaults.project_id in config)")
	}
	if region == "" {
		return nil, fmt.Errorf("region is required for Cloud Run (use --region flag or set defaults.region in config)")
	}

	p := parseCloudRunPath(path)
	r.lastScheme = p.scheme

	switch p.scheme {
	case "svc":
		return r.listServices(ctx, project, region)
	case "jobs":
		showAll := opts != nil && opts.AllStatuses
		nameHasWildcard := strings.ContainsAny(p.name, "*?")
		if p.name == "" {
			return r.listJobs(ctx, project, region)
		}
		if nameHasWildcard {
			// Pattern like jobs://legacy*  → filtered job list
			// Pattern like jobs://legacy*/* → executions for all matching jobs
			return r.listJobsOrExecutionsByPattern(ctx, project, region, p.name, p.execution, showAll)
		}
		return r.listExecutions(ctx, project, region, p.name, showAll)
	case "worker":
		return r.listWorkerPools(ctx, project, region)
	default:
		return nil, fmt.Errorf("unknown Cloud Run scheme in path: %s", path)
	}
}

// crPath holds parsed Cloud Run path components.
type crPath struct {
	scheme    string // "svc", "jobs", "worker"
	name      string // resource name, may be empty (list all)
	execution string // execution name (jobs only)
}

// parseCloudRunPath parses svc://, jobs://, or worker:// paths.
func parseCloudRunPath(path string) crPath {
	for _, scheme := range []string{"svc", "jobs", "worker"} {
		prefix := scheme + "://"
		if strings.HasPrefix(path, prefix) {
			rest := strings.TrimPrefix(path, prefix)
			parts := strings.SplitN(rest, "/", 2)
			p := crPath{scheme: scheme, name: parts[0]}
			if len(parts) > 1 {
				p.execution = parts[1]
			}
			return p
		}
	}
	return crPath{}
}

func (r *CloudRunResource) listServices(ctx context.Context, project, region string) ([]*ResourceInfo, error) {
	services, err := cloudrun.ListServices(ctx, project, region)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, svc := range services {
		resources = append(resources, &ResourceInfo{
			Name:     svc.Name,
			Path:     "svc://" + svc.Name,
			Type:     "service",
			Modified: svc.Updated,
			Created:  svc.Created,
			Metadata: svc,
		})
	}
	return resources, nil
}

func (r *CloudRunResource) listJobs(ctx context.Context, project, region string) ([]*ResourceInfo, error) {
	jobs, err := cloudrun.ListJobs(ctx, project, region)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, job := range jobs {
		resources = append(resources, &ResourceInfo{
			Name:     job.Name,
			Path:     "jobs://" + job.Name,
			Type:     "job",
			Modified: job.Updated,
			Created:  job.Created,
			Metadata: job,
		})
	}
	return resources, nil
}

// listJobsOrExecutionsByPattern handles wildcard job-name patterns.
// When execution is "*" it lists executions for every matching job;
// otherwise it returns the matching jobs themselves.
func (r *CloudRunResource) listJobsOrExecutionsByPattern(ctx context.Context, project, region, namePattern, execution string, showAll bool) ([]*ResourceInfo, error) {
	jobs, err := cloudrun.ListJobs(ctx, project, region)
	if err != nil {
		return nil, err
	}

	// Filter jobs by name pattern.
	var matched []*cloudrun.JobInfo
	for _, job := range jobs {
		ok, _ := path.Match(namePattern, job.Name)
		if ok {
			matched = append(matched, job)
		}
	}

	if execution != "*" {
		// Return the filtered job list.
		r.lastScheme = "jobs"
		var resources []*ResourceInfo
		for _, job := range matched {
			resources = append(resources, &ResourceInfo{
				Name:     job.Name,
				Path:     "jobs://" + job.Name,
				Type:     "job",
				Modified: job.Updated,
				Created:  job.Created,
				Metadata: job,
			})
		}
		return resources, nil
	}

	// execution == "*": list executions for every matching job.
	r.lastScheme = "jobs-executions"
	var resources []*ResourceInfo
	for _, job := range matched {
		execs, err := r.listExecutions(ctx, project, region, job.Name, showAll)
		if err != nil {
			return nil, err
		}
		resources = append(resources, execs...)
	}
	return resources, nil
}

func (r *CloudRunResource) listExecutions(ctx context.Context, project, region, jobName string, showAll bool) ([]*ResourceInfo, error) {
	executions, err := cloudrun.ListExecutions(ctx, project, region, jobName)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, exec := range executions {
		// By default only show active (Running/Pending) executions; -a shows all
		if !showAll && (exec.Status == "Succeeded" || exec.Status == "Failed") {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     exec.Name,
			Path:     "jobs://" + jobName + "/" + exec.Name,
			Type:     "execution",
			Modified: exec.StartTime,
			Created:  exec.StartTime,
			Metadata: exec,
		})
	}
	return resources, nil
}

func (r *CloudRunResource) listWorkerPools(ctx context.Context, project, region string) ([]*ResourceInfo, error) {
	pools, err := cloudrun.ListWorkerPools(ctx, project, region)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, pool := range pools {
		resources = append(resources, &ResourceInfo{
			Name:     pool.Name,
			Path:     "worker://" + pool.Name,
			Type:     "worker-pool",
			Modified: pool.Updated,
			Created:  pool.Created,
			Metadata: pool,
		})
	}
	return resources, nil
}

// Remove deletes Cloud Run job executions.
// Supports:
//   - jobs://job-name/execution-name  → delete a specific execution
//   - jobs://job-name/*               → delete all completed/failed executions
func (r *CloudRunResource) Remove(ctx context.Context, p string, opts *RemoveOptions) error {
	parsed := parseCloudRunPath(p)
	if parsed.scheme != "jobs" || parsed.name == "" {
		return fmt.Errorf("rm only supports Cloud Run job executions (jobs://job-name/execution-name)")
	}
	if parsed.execution == "" {
		return fmt.Errorf("rm requires an execution name: jobs://job-name/execution-name or jobs://job-name/*")
	}

	var project, region string
	if opts != nil {
		project = opts.Project
		region = opts.Region
	}
	if project == "" {
		return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}
	if region == "" {
		return fmt.Errorf("region is required (use --region flag or set defaults.region in config)")
	}

	// Single execution deletion
	if parsed.execution != "*" && !strings.ContainsAny(parsed.execution, "*?") {
		if opts == nil || !opts.Force {
			fmt.Printf("Remove execution %s? (y/N): ", parsed.execution)
			var response string
			fmt.Scanln(&response)
			if response != "y" && response != "Y" {
				fmt.Println("Cancelled.")
				return nil
			}
		}
		if err := cloudrun.DeleteExecution(ctx, project, region, parsed.name, parsed.execution); err != nil {
			return err
		}
		fmt.Printf("Deleted: %s\n", parsed.execution)
		return nil
	}

	// Wildcard: list executions and delete non-running ones
	executions, err := cloudrun.ListExecutions(ctx, project, region, parsed.name)
	if err != nil {
		return err
	}

	// Filter: only delete completed/failed executions (skip Running/Pending)
	var toDelete []*cloudrun.ExecutionInfo
	for _, exec := range executions {
		if exec.Status == "Running" || exec.Status == "Pending" {
			continue
		}
		if parsed.execution != "*" {
			if ok, _ := path.Match(parsed.execution, exec.Name); !ok {
				continue
			}
		}
		toDelete = append(toDelete, exec)
	}

	if len(toDelete) == 0 {
		fmt.Println("No completed/failed executions found to remove.")
		return nil
	}

	// Show what will be deleted
	fmt.Printf("Found %d completed/failed execution(s) to remove:\n", len(toDelete))
	for _, exec := range toDelete {
		fmt.Printf("  - %s (%s)\n", exec.Name, exec.Status)
	}
	fmt.Println()

	// Confirm unless force
	if opts == nil || !opts.Force {
		fmt.Printf("Remove all %d execution(s)? (y/N): ", len(toDelete))
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	// Delete executions
	for _, exec := range toDelete {
		if opts != nil && opts.Verbose {
			fmt.Printf("Deleting %s...\n", exec.Name)
		}
		if err := cloudrun.DeleteExecution(ctx, project, region, parsed.name, exec.Name); err != nil {
			return err
		}
		fmt.Printf("Deleted: %s\n", exec.Name)
	}

	return nil
}

// Cancel cancels running Cloud Run job executions.
// Supports:
//   - jobs://job-name/execution-name  → cancel a specific execution
//   - jobs://job-name/*               → cancel all running/pending executions
func (r *CloudRunResource) Cancel(ctx context.Context, p string, opts *RemoveOptions) error {
	parsed := parseCloudRunPath(p)
	if parsed.scheme != "jobs" || parsed.name == "" {
		return fmt.Errorf("cancel only supports Cloud Run job executions (jobs://job-name/execution-name)")
	}
	if parsed.execution == "" {
		return fmt.Errorf("cancel requires an execution name: jobs://job-name/execution-name or jobs://job-name/*")
	}

	var project, region string
	if opts != nil {
		project = opts.Project
		region = opts.Region
	}
	if project == "" {
		return fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}
	if region == "" {
		return fmt.Errorf("region is required (use --region flag or set defaults.region in config)")
	}

	// Single execution cancellation
	if parsed.execution != "*" && !strings.ContainsAny(parsed.execution, "*?") {
		if opts == nil || !opts.Force {
			fmt.Printf("Cancel execution %s? (y/N): ", parsed.execution)
			var response string
			fmt.Scanln(&response)
			if response != "y" && response != "Y" {
				fmt.Println("Cancelled.")
				return nil
			}
		}
		if err := cloudrun.CancelExecution(ctx, project, region, parsed.name, parsed.execution); err != nil {
			return err
		}
		fmt.Printf("Cancelled: %s\n", parsed.execution)
		return nil
	}

	// Wildcard: list executions and cancel running/pending ones
	executions, err := cloudrun.ListExecutions(ctx, project, region, parsed.name)
	if err != nil {
		return err
	}

	var toCancel []*cloudrun.ExecutionInfo
	for _, exec := range executions {
		if exec.Status != "Running" && exec.Status != "Pending" {
			continue
		}
		if parsed.execution != "*" {
			if ok, _ := path.Match(parsed.execution, exec.Name); !ok {
				continue
			}
		}
		toCancel = append(toCancel, exec)
	}

	if len(toCancel) == 0 {
		fmt.Println("No running/pending executions found to cancel.")
		return nil
	}

	fmt.Printf("Found %d running/pending execution(s) to cancel:\n", len(toCancel))
	for _, exec := range toCancel {
		fmt.Printf("  - %s (%s)\n", exec.Name, exec.Status)
	}
	fmt.Println()

	if opts == nil || !opts.Force {
		fmt.Printf("Cancel all %d execution(s)? (y/N): ", len(toCancel))
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	for _, exec := range toCancel {
		wg.Add(1)
		go func(e *cloudrun.ExecutionInfo) {
			defer wg.Done()
			start := time.Now()
			err := cloudrun.CancelExecution(ctx, project, region, parsed.name, e.Name)
			elapsed := time.Since(start).Round(time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fmt.Printf("Failed: %s (%v)\n", e.Name, err)
				if firstErr == nil {
					firstErr = err
				}
			} else {
				fmt.Printf("Cancelled: %s (took %s)\n", e.Name, elapsed)
			}
		}(exec)
	}
	wg.Wait()

	return firstErr
}

// Info returns detailed information about a Cloud Run resource.
func (r *CloudRunResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("cio info is not yet supported for Cloud Run resources")
}

// FormatShort formats resource info in short format.
func (r *CloudRunResource) FormatShort(info *ResourceInfo, aliasPath string) string {
	switch v := info.Metadata.(type) {
	case *cloudrun.ServiceInfo:
		return v.FormatShort()
	case *cloudrun.JobInfo:
		return v.FormatShort()
	case *cloudrun.ExecutionInfo:
		return v.FormatShort()
	case *cloudrun.WorkerPoolInfo:
		return v.FormatShort()
	}
	return info.Name
}

// FormatLong formats resource info in long format.
func (r *CloudRunResource) FormatLong(info *ResourceInfo, aliasPath string) string {
	switch v := info.Metadata.(type) {
	case *cloudrun.ServiceInfo:
		return v.FormatServiceLong()
	case *cloudrun.JobInfo:
		return v.FormatJobLong()
	case *cloudrun.ExecutionInfo:
		return v.FormatExecutionLong()
	case *cloudrun.WorkerPoolInfo:
		return v.FormatWorkerPoolLong()
	}
	return info.Name
}

// FormatDetailed formats resource info with full details.
func (r *CloudRunResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}

// FormatLongHeader returns the header for long format listing.
// It is accurate after List() has been called (uses lastScheme).
func (r *CloudRunResource) FormatLongHeader() string {
	switch r.lastScheme {
	case "svc":
		return cloudrun.ServiceLongHeader()
	case "jobs":
		return cloudrun.JobLongHeader()
	case "jobs-executions":
		return cloudrun.ExecutionLongHeader()
	case "worker":
		return cloudrun.WorkerPoolLongHeader()
	}
	return ""
}

// FormatLongHeaderDynamic picks the right header based on first resource type.
// Called from ls.go after list returns.
func FormatLongHeaderDynamic(resources []*ResourceInfo) string {
	if len(resources) == 0 {
		return ""
	}
	switch resources[0].Metadata.(type) {
	case *cloudrun.ServiceInfo:
		return cloudrun.ServiceLongHeader()
	case *cloudrun.JobInfo:
		return cloudrun.JobLongHeader()
	case *cloudrun.ExecutionInfo:
		return cloudrun.ExecutionLongHeader()
	case *cloudrun.WorkerPoolInfo:
		return cloudrun.WorkerPoolLongHeader()
	case *dataflow.JobInfo:
		return dataflow.JobLongHeader()
	}
	return ""
}

