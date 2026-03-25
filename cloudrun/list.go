package cloudrun

import (
	"context"
	"fmt"
	"strings"
	"time"

	runpb "cloud.google.com/go/run/apiv2/runpb"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/api/iterator"
)

// ServiceInfo holds information about a Cloud Run service.
type ServiceInfo struct {
	Name    string    `json:"name"`
	Region  string    `json:"region"`
	Project string    `json:"project"`
	URI     string    `json:"uri"`
	Status  string    `json:"status"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
}

// JobInfo holds information about a Cloud Run job.
type JobInfo struct {
	Name           string    `json:"name"`
	Region         string    `json:"region"`
	Project        string    `json:"project"`
	Status         string    `json:"status"`
	Created        time.Time `json:"created"`
	Updated        time.Time `json:"updated"`
	ExecutionCount int32     `json:"execution_count"`
	ActiveExecs    int32     `json:"active_executions"`
}

// ExecutionInfo holds information about a Cloud Run job execution.
type ExecutionInfo struct {
	Name      string    `json:"name"`
	JobName   string    `json:"job_name"`
	Region    string    `json:"region"`
	Project   string    `json:"project"`
	Status    string    `json:"status"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Succeeded int32     `json:"succeeded"`
	Failed    int32     `json:"failed"`
	Running   int32     `json:"running"`
}

// WorkerPoolInfo holds information about a Cloud Run worker pool.
type WorkerPoolInfo struct {
	Name          string    `json:"name"`
	Region        string    `json:"region"`
	Project       string    `json:"project"`
	Status        string    `json:"status"`
	Created       time.Time `json:"created"`
	Updated       time.Time `json:"updated"`
	InstanceCount int32     `json:"instance_count"`
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
			Name:           extractShortName(job.Name),
			Region:         region,
			Project:        project,
			Status:         jobStatus(job),
			ExecutionCount: job.ExecutionCount,
		}
		if job.CreateTime != nil {
			info.Created = job.CreateTime.AsTime()
		}
		if job.UpdateTime != nil {
			info.Updated = job.UpdateTime.AsTime()
		}

		// Count active (running/pending) executions
		if ref := job.LatestCreatedExecution; ref != nil {
			if ref.CompletionStatus == runpb.ExecutionReference_EXECUTION_RUNNING ||
				ref.CompletionStatus == runpb.ExecutionReference_EXECUTION_PENDING {
				// At least the latest is active; list all executions to count active ones
				execs, execErr := ListExecutions(ctx, project, region, info.Name)
				if execErr == nil {
					for _, e := range execs {
						if e.Status == "Running" || e.Status == "Pending" {
							info.ActiveExecs++
						}
					}
				}
			}
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

// DeleteJob deletes a Cloud Run job.
func DeleteJob(ctx context.Context, project, region, jobName string) error {
	client, err := GetJobsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Run jobs client: %w", err)
	}

	name := fmt.Sprintf("projects/%s/locations/%s/jobs/%s", project, region, jobName)
	apilog.Logf("[CloudRun] Jobs.Delete(%s)", name)

	op, err := client.DeleteJob(ctx, &runpb.DeleteJobRequest{Name: name})
	if err != nil {
		return fmt.Errorf("failed to delete job %s: %w", jobName, err)
	}

	// Wait for the operation to complete.
	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed waiting for deletion of job %s: %w", jobName, err)
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
		if wp.Scaling != nil && wp.Scaling.ManualInstanceCount != nil {
			info.InstanceCount = *wp.Scaling.ManualInstanceCount
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
	return fmt.Sprintf("%-55s %-12s %-20s %s", s.Name, s.Status, updated, uri)
}

// FormatShort formats a job in short format.
func (j *JobInfo) FormatShort() string { return j.Name }

// FormatJobLong formats a job in long format.
func (j *JobInfo) FormatJobLong() string {
	updated := j.Updated.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%-55s %-12s %7d %7d  %s", j.Name, j.Status, j.ActiveExecs, j.ExecutionCount, updated)
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
	return fmt.Sprintf("%-55s %-12s %10d  %s", w.Name, w.Status, w.InstanceCount, updated)
}

// ServiceLongHeader returns the header for long service listing.
func ServiceLongHeader() string {
	return fmt.Sprintf("%-55s %-12s %-20s %s", "NAME", "STATUS", "UPDATED", "URL")
}

// JobLongHeader returns the header for long job listing.
func JobLongHeader() string {
	return fmt.Sprintf("%-55s %-12s %7s %7s  %s", "NAME", "STATUS", "ACTIVE", "TOTAL", "UPDATED")
}

// ExecutionLongHeader returns the header for long execution listing.
func ExecutionLongHeader() string {
	return fmt.Sprintf("%-50s %-12s %-20s %-20s %s", "NAME", "STATUS", "STARTED", "COMPLETED", "OK/FAIL")
}

// WorkerPoolLongHeader returns the header for long worker pool listing.
func WorkerPoolLongHeader() string {
	return fmt.Sprintf("%-55s %-12s %10s  %s", "NAME", "STATUS", "INSTANCES", "UPDATED")
}

// UpdateWorkerPoolInstances updates the manual instance count for a worker pool.
func UpdateWorkerPoolInstances(ctx context.Context, project, region, name string, count int32) error {
	client, err := GetWorkerPoolsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Run worker pools client: %w", err)
	}

	fullName := fmt.Sprintf("projects/%s/locations/%s/workerPools/%s", project, region, name)
	apilog.Logf("[CloudRun] WorkerPools.Update(%s, instances=%d)", fullName, count)

	op, err := client.UpdateWorkerPool(ctx, &runpb.UpdateWorkerPoolRequest{
		WorkerPool: &runpb.WorkerPool{
			Name: fullName,
			Scaling: &runpb.WorkerPoolScaling{
				ManualInstanceCount: &count,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"scaling.manual_instance_count"},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update worker pool: %w", err)
	}

	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed waiting for worker pool update: %w", err)
	}

	return nil
}
