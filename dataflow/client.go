package dataflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	df "google.golang.org/api/dataflow/v1b3"
	"google.golang.org/api/option"
)

var (
	svc     *df.Service
	svcOnce sync.Once
	svcErr  error
)

// getService returns the singleton Dataflow API service.
func getService(ctx context.Context) (*df.Service, error) {
	svcOnce.Do(func() {
		svc, svcErr = df.NewService(ctx, option.WithScopes(df.CloudPlatformScope))
	})
	return svc, svcErr
}

// JobInfo holds summary information about a Dataflow job.
type JobInfo struct {
	ID          string
	Name        string
	State       string
	Type        string
	Region      string
	Created     time.Time
	StartTime   time.Time
	StateTime   time.Time // time of last state change
}

// FormatShort returns a short one-line representation.
func (j *JobInfo) FormatShort() string {
	return fmt.Sprintf("%-50s %s", j.Name, j.ID)
}

// FormatLong returns a detailed one-line representation.
func (j *JobInfo) FormatLong() string {
	created := j.Created.In(time.Local).Format("2006-01-02 15:04")
	return fmt.Sprintf("%-12s %-50s %-8s %s  %s", j.State, j.Name, j.Type, created, j.ID)
}

// JobLongHeader returns the header line for long format listing.
func JobLongHeader() string {
	return fmt.Sprintf("%-12s %-50s %-8s %-17s  %s", "STATE", "NAME", "TYPE", "CREATED", "JOB_ID")
}

// ListJobs lists Dataflow jobs in the given project and region.
// filter can be: "all", "active", "terminated" (maps to Dataflow API filter).
func ListJobs(ctx context.Context, projectID, region, filter string) ([]*JobInfo, error) {
	svc, err := getService(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating Dataflow service: %w", err)
	}

	apiFilter := "UNKNOWN"
	switch filter {
	case "active":
		apiFilter = "ACTIVE"
	case "terminated":
		apiFilter = "TERMINATED"
	case "all", "":
		apiFilter = "ALL"
	}

	var jobs []*JobInfo
	call := svc.Projects.Locations.Jobs.List(projectID, region).Filter(apiFilter).PageSize(100)
	err = call.Pages(ctx, func(resp *df.ListJobsResponse) error {
		for _, j := range resp.Jobs {
			info := &JobInfo{
				ID:     j.Id,
				Name:   j.Name,
				State:  simplifyState(j.CurrentState),
				Type:   simplifyType(j.Type),
				Region: region,
			}
			if t, err := time.Parse(time.RFC3339Nano, j.CreateTime); err == nil {
				info.Created = t
			}
			if t, err := time.Parse(time.RFC3339Nano, j.StartTime); err == nil {
				info.StartTime = t
			}
			if t, err := time.Parse(time.RFC3339Nano, j.CurrentStateTime); err == nil {
				info.StateTime = t
			}
			jobs = append(jobs, info)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing Dataflow jobs: %w", err)
	}
	return jobs, nil
}

// simplifyState converts Dataflow API state names to short labels.
func simplifyState(state string) string {
	switch state {
	case "JOB_STATE_RUNNING":
		return "Running"
	case "JOB_STATE_DONE":
		return "Done"
	case "JOB_STATE_FAILED":
		return "Failed"
	case "JOB_STATE_CANCELLED":
		return "Cancelled"
	case "JOB_STATE_DRAINED":
		return "Drained"
	case "JOB_STATE_DRAINING":
		return "Draining"
	case "JOB_STATE_UPDATED":
		return "Updated"
	case "JOB_STATE_CANCELLING":
		return "Cancelling"
	case "JOB_STATE_PENDING":
		return "Pending"
	case "JOB_STATE_QUEUED":
		return "Queued"
	case "JOB_STATE_STOPPED":
		return "Stopped"
	default:
		return state
	}
}

// FindJobByName looks up a Dataflow job by name and returns its info.
// It searches all jobs (active + terminated) and returns the most recent match.
func FindJobByName(ctx context.Context, projectID, region, name string) (*JobInfo, error) {
	jobs, err := ListJobs(ctx, projectID, region, "all")
	if err != nil {
		return nil, err
	}
	var best *JobInfo
	for _, j := range jobs {
		if j.Name == name {
			if best == nil || j.Created.After(best.Created) {
				best = j
			}
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no Dataflow job found with name %q", name)
	}
	return best, nil
}

// simplifyType converts Dataflow API job type names to short labels.
func simplifyType(jobType string) string {
	switch jobType {
	case "JOB_TYPE_BATCH":
		return "Batch"
	case "JOB_TYPE_STREAMING":
		return "Stream"
	default:
		return jobType
	}
}
