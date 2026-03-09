package cloudrun

import (
	"context"
	"fmt"
	"strings"
	"time"

	runpb "cloud.google.com/go/run/apiv2/runpb"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// ServiceInfo holds information about a Cloud Run service.
type ServiceInfo struct {
	Name    string
	Region  string
	Project string
	URI     string
	Status  string
	Created time.Time
	Updated time.Time
}

// JobInfo holds information about a Cloud Run job.
type JobInfo struct {
	Name    string
	Region  string
	Project string
	Status  string
	Created time.Time
	Updated time.Time
}

// ExecutionInfo holds information about a Cloud Run job execution.
type ExecutionInfo struct {
	Name      string
	JobName   string
	Region    string
	Project   string
	Status    string
	StartTime time.Time
	EndTime   time.Time
	Succeeded int32
	Failed    int32
	Running   int32
}

// WorkerPoolInfo holds information about a Cloud Run worker pool.
type WorkerPoolInfo struct {
	Name    string
	Region  string
	Project string
	URI     string
	Status  string
	Created time.Time
	Updated time.Time
}

// extractShortName extracts the short name from a full resource name.
// e.g. "projects/p/locations/r/services/my-service" → "my-service"
func extractShortName(fullName string) string {
	parts := strings.Split(fullName, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return fullName
}

// serviceStatus returns a human-readable status from a Service proto.
func serviceStatus(svc *runpb.Service) string {
	if svc.TerminalCondition != nil {
		switch svc.TerminalCondition.State {
		case runpb.Condition_CONDITION_SUCCEEDED:
			return "Ready"
		case runpb.Condition_CONDITION_FAILED:
			return "Failed"
		case runpb.Condition_CONDITION_RECONCILING:
			return "Deploying"
		}
	}
	return "Unknown"
}

// jobStatus returns a human-readable status from a Job proto.
func jobStatus(job *runpb.Job) string {
	if job.TerminalCondition != nil {
		switch job.TerminalCondition.State {
		case runpb.Condition_CONDITION_SUCCEEDED:
			return "Ready"
		case runpb.Condition_CONDITION_FAILED:
			return "Failed"
		}
	}
	return "Ready"
}

// executionStatus returns a human-readable status from an Execution proto.
func executionStatus(exec *runpb.Execution) string {
	if exec.CompletionTime != nil {
		if exec.FailedCount > 0 {
			return "Failed"
		}
		return "Succeeded"
	}
	if exec.RunningCount > 0 {
		return "Running"
	}
	return "Pending"
}

// workerPoolStatus returns a human-readable status from a WorkerPool proto.
func workerPoolStatus(wp *runpb.WorkerPool) string {
	if wp.TerminalCondition != nil {
		switch wp.TerminalCondition.State {
		case runpb.Condition_CONDITION_SUCCEEDED:
			return "Ready"
		case runpb.Condition_CONDITION_FAILED:
			return "Failed"
		}
	}
	return "Ready"
}

// ListServices lists all Cloud Run services in the given project/region.
func ListServices(ctx context.Context, project, region string) ([]*ServiceInfo, error) {
	client, err := GetServicesClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Run services client: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", project, region)
	apilog.Logf("[CloudRun] Services.List(%s)", parent)

	it := client.ListServices(ctx, &runpb.ListServicesRequest{Parent: parent})

	var services []*ServiceInfo
	for {
		svc, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list services: %w", err)
		}

		info := &ServiceInfo{
			Name:    extractShortName(svc.Name),
			Region:  region,
			Project: project,
			URI:     svc.Uri,
			Status:  serviceStatus(svc),
		}
		if svc.CreateTime != nil {
			info.Created = svc.CreateTime.AsTime()
		}
		if svc.UpdateTime != nil {
			info.Updated = svc.UpdateTime.AsTime()
		}
		services = append(services, info)
	}
	return services, nil
}

// ListJobs lists all Cloud Run jobs in the given project/region.
func ListJobs(ctx context.Context, project, region string) ([]*JobInfo, error) {
	client, err := GetJobsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Run jobs client: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", project, region)
	apilog.Logf("[CloudRun] Jobs.List(%s)", parent)

	it := client.ListJobs(ctx, &runpb.ListJobsRequest{Parent: parent})

	var jobs []*JobInfo
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list jobs: %w", err)
		}

		info := &JobInfo{
			Name:    extractShortName(job.Name),
			Region:  region,
			Project: project,
			Status:  jobStatus(job),
		}
		if job.CreateTime != nil {
			info.Created = job.CreateTime.AsTime()
		}
		if job.UpdateTime != nil {
			info.Updated = job.UpdateTime.AsTime()
		}
		jobs = append(jobs, info)
	}
	return jobs, nil
}

// ListExecutions lists executions for a specific Cloud Run job.
func ListExecutions(ctx context.Context, project, region, jobName string) ([]*ExecutionInfo, error) {
	client, err := GetExecutionsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Run executions client: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s/jobs/%s", project, region, jobName)
	apilog.Logf("[CloudRun] Executions.List(%s)", parent)

	it := client.ListExecutions(ctx, &runpb.ListExecutionsRequest{Parent: parent})

	var executions []*ExecutionInfo
	for {
		exec, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list executions: %w", err)
		}

		info := &ExecutionInfo{
			Name:      extractShortName(exec.Name),
			JobName:   jobName,
			Region:    region,
			Project:   project,
			Status:    executionStatus(exec),
			Succeeded: exec.SucceededCount,
			Failed:    exec.FailedCount,
			Running:   exec.RunningCount,
		}
		if exec.StartTime != nil {
			info.StartTime = exec.StartTime.AsTime()
		}
		if exec.CompletionTime != nil {
			info.EndTime = exec.CompletionTime.AsTime()
		}
		executions = append(executions, info)
	}
	return executions, nil
}

// DeleteExecution deletes a Cloud Run job execution.
func DeleteExecution(ctx context.Context, project, region, jobName, executionName string) error {
	client, err := GetExecutionsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Run executions client: %w", err)
	}

	name := fmt.Sprintf("projects/%s/locations/%s/jobs/%s/executions/%s", project, region, jobName, executionName)
	apilog.Logf("[CloudRun] Executions.Delete(%s)", name)

	op, err := client.DeleteExecution(ctx, &runpb.DeleteExecutionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("failed to delete execution %s: %w", executionName, err)
	}

	// Wait for the operation to complete.
	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed waiting for deletion of execution %s: %w", executionName, err)
	}
	return nil
}

// CancelExecution cancels a running Cloud Run job execution.
func CancelExecution(ctx context.Context, project, region, jobName, executionName string) error {
	client, err := GetExecutionsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Run executions client: %w", err)
	}

	name := fmt.Sprintf("projects/%s/locations/%s/jobs/%s/executions/%s", project, region, jobName, executionName)
	apilog.Logf("[CloudRun] Executions.Cancel(%s)", name)

	op, err := client.CancelExecution(ctx, &runpb.CancelExecutionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("failed to cancel execution %s: %w", executionName, err)
	}

	// Wait for the operation to complete.
	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed waiting for cancellation of execution %s: %w", executionName, err)
	}
	return nil
}

// ListWorkerPools lists all Cloud Run worker pools in the given project/region.
func ListWorkerPools(ctx context.Context, project, region string) ([]*WorkerPoolInfo, error) {
	client, err := GetWorkerPoolsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Run worker pools client: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", project, region)
	apilog.Logf("[CloudRun] WorkerPools.List(%s)", parent)

	it := client.ListWorkerPools(ctx, &runpb.ListWorkerPoolsRequest{Parent: parent})

	var pools []*WorkerPoolInfo
	for {
		wp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list worker pools: %w", err)
		}

		info := &WorkerPoolInfo{
			Name:    extractShortName(wp.Name),
			Region:  region,
			Project: project,
			Status:  workerPoolStatus(wp),
		}
		if wp.CreateTime != nil {
			info.Created = wp.CreateTime.AsTime()
		}
		if wp.UpdateTime != nil {
			info.Updated = wp.UpdateTime.AsTime()
		}
		pools = append(pools, info)
	}
	return pools, nil
}

// FormatShort formats a service in short format.
func (s *ServiceInfo) FormatShort() string { return s.Name }

// FormatServiceLong formats a service in long format.
func (s *ServiceInfo) FormatServiceLong() string {
	updated := s.Updated.Format("2006-01-02 15:04:05")
	uri := s.URI
	if uri == "" {
		uri = "-"
	}
	return fmt.Sprintf("%-40s %-12s %-20s %s", s.Name, s.Status, updated, uri)
}

// FormatShort formats a job in short format.
func (j *JobInfo) FormatShort() string { return j.Name }

// FormatJobLong formats a job in long format.
func (j *JobInfo) FormatJobLong() string {
	updated := j.Updated.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%-40s %-12s %s", j.Name, j.Status, updated)
}

// FormatShort formats an execution in short format.
func (e *ExecutionInfo) FormatShort() string { return e.Name }

// FormatExecutionLong formats an execution in long format.
func (e *ExecutionInfo) FormatExecutionLong() string {
	start := "-"
	if !e.StartTime.IsZero() {
		start = e.StartTime.Format("2006-01-02 15:04:05")
	}
	end := "-"
	if !e.EndTime.IsZero() {
		end = e.EndTime.Format("2006-01-02 15:04:05")
	}
	return fmt.Sprintf("%-50s %-12s %-20s %-20s %3d/%3d",
		e.Name, e.Status, start, end, e.Succeeded, e.Failed)
}

// FormatShort formats a worker pool in short format.
func (w *WorkerPoolInfo) FormatShort() string { return w.Name }

// FormatWorkerPoolLong formats a worker pool in long format.
func (w *WorkerPoolInfo) FormatWorkerPoolLong() string {
	updated := w.Updated.Format("2006-01-02 15:04:05")
	uri := w.URI
	if uri == "" {
		uri = "-"
	}
	return fmt.Sprintf("%-40s %-12s %-20s %s", w.Name, w.Status, updated, uri)
}

// ServiceLongHeader returns the header for long service listing.
func ServiceLongHeader() string {
	return fmt.Sprintf("%-40s %-12s %-20s %s", "NAME", "STATUS", "UPDATED", "URL")
}

// JobLongHeader returns the header for long job listing.
func JobLongHeader() string {
	return fmt.Sprintf("%-40s %-12s %s", "NAME", "STATUS", "UPDATED")
}

// ExecutionLongHeader returns the header for long execution listing.
func ExecutionLongHeader() string {
	return fmt.Sprintf("%-50s %-12s %-20s %-20s %s", "NAME", "STATUS", "STARTED", "COMPLETED", "OK/FAIL")
}

// WorkerPoolLongHeader returns the header for long worker pool listing.
func WorkerPoolLongHeader() string {
	return fmt.Sprintf("%-40s %-12s %-20s %s", "NAME", "STATUS", "UPDATED", "URL")
}
