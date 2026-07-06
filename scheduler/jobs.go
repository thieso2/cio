package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/thieso2/cio/apilog"
	cloudscheduler "google.golang.org/api/cloudscheduler/v1"
	"google.golang.org/api/googleapi"
)

// JobInfo holds information about a Cloud Scheduler job.
type JobInfo struct {
	Name        string    `json:"name"`
	State       string    `json:"state"`
	Schedule    string    `json:"schedule"`
	TimeZone    string    `json:"time_zone"`
	Target      string    `json:"target"`
	NextRun     time.Time `json:"next_run,omitempty"`
	LastAttempt time.Time `json:"last_attempt,omitempty"`
	Description string    `json:"description,omitempty"`
	raw         *cloudscheduler.Job
}

func jobInfoFrom(job *cloudscheduler.Job) *JobInfo {
	info := &JobInfo{
		Name:        shortJobName(job.Name),
		State:       job.State,
		Schedule:    job.Schedule,
		TimeZone:    job.TimeZone,
		Target:      targetSummary(job),
		Description: job.Description,
		raw:         job,
	}
	if t, err := time.Parse(time.RFC3339, job.ScheduleTime); err == nil {
		info.NextRun = t
	}
	if t, err := time.Parse(time.RFC3339, job.LastAttemptTime); err == nil {
		info.LastAttempt = t
	}
	return info
}

// shortJobName extracts the job name from projects/P/locations/L/jobs/NAME.
func shortJobName(full string) string {
	if idx := strings.LastIndex(full, "/"); idx != -1 {
		return full[idx+1:]
	}
	return full
}

func targetSummary(job *cloudscheduler.Job) string {
	switch {
	case job.HttpTarget != nil:
		method := job.HttpTarget.HttpMethod
		if method == "" {
			method = "POST"
		}
		return fmt.Sprintf("HTTP %s %s", method, job.HttpTarget.Uri)
	case job.PubsubTarget != nil:
		return "Pub/Sub " + shortJobName(job.PubsubTarget.TopicName)
	case job.AppEngineHttpTarget != nil:
		return "App Engine " + job.AppEngineHttpTarget.RelativeUri
	}
	return "-"
}

// FormatShort formats a job in short format.
func (j *JobInfo) FormatShort() string { return j.Name }

// FormatLong formats a job in long format.
func (j *JobInfo) FormatLong() string {
	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s",
		j.Name, j.State, j.Schedule, j.TimeZone,
		formatRunTime(j.NextRun), formatRunTime(j.LastAttempt), j.Target)
}

// JobLongHeader returns the header for long job listing.
func JobLongHeader() string {
	return "NAME\tSTATE\tSCHEDULE\tTIMEZONE\tNEXT RUN\tLAST RUN\tTARGET"
}

func formatRunTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// FormatDetailed formats a job with full details.
func (j *JobInfo) FormatDetailed() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Name:         %s\n", j.Name)
	fmt.Fprintf(&b, "State:        %s\n", j.State)
	fmt.Fprintf(&b, "Schedule:     %s\n", j.Schedule)
	fmt.Fprintf(&b, "Time Zone:    %s\n", j.TimeZone)
	fmt.Fprintf(&b, "Target:       %s\n", j.Target)
	if !j.NextRun.IsZero() {
		fmt.Fprintf(&b, "Next Run:     %s\n", formatRunTime(j.NextRun))
	}
	if !j.LastAttempt.IsZero() {
		fmt.Fprintf(&b, "Last Attempt: %s\n", formatRunTime(j.LastAttempt))
	}
	if j.Description != "" {
		fmt.Fprintf(&b, "Description:  %s\n", j.Description)
	}
	if j.raw != nil {
		if j.raw.RetryConfig != nil {
			fmt.Fprintf(&b, "Retries:      %d (max)\n", j.raw.RetryConfig.RetryCount)
		}
		if j.raw.AttemptDeadline != "" {
			fmt.Fprintf(&b, "Deadline:     %s\n", j.raw.AttemptDeadline)
		}
		if j.raw.Status != nil && j.raw.Status.Code != 0 {
			fmt.Fprintf(&b, "Last Status:  error code %d\n", j.raw.Status.Code)
		}
	}
	return b.String()
}

func locationParent(project, region string) string {
	return fmt.Sprintf("projects/%s/locations/%s", project, region)
}

func jobName(project, region, name string) string {
	return locationParent(project, region) + "/jobs/" + name
}

// ListJobs lists all Cloud Scheduler jobs in a project region.
func ListJobs(ctx context.Context, project, region string) ([]*JobInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Scheduler service: %w", err)
	}

	apilog.Logf("[Scheduler] Jobs.List(%s, %s)", project, region)

	var jobs []*JobInfo
	err = svc.Projects.Locations.Jobs.List(locationParent(project, region)).Pages(ctx, func(resp *cloudscheduler.ListJobsResponse) error {
		for _, job := range resp.Jobs {
			jobs = append(jobs, jobInfoFrom(job))
		}
		return nil
	})
	if err != nil {
		return nil, wrapAccessErr(err, project, "list scheduler jobs")
	}
	return jobs, nil
}

// wrapAccessErr turns the verbose googleapi 403 (usually: Cloud Scheduler API
// not enabled in the project) into a one-line, actionable error.
func wrapAccessErr(err error, project, action string) error {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == 403 {
		return fmt.Errorf("failed to %s: permission denied in project %s — is the Cloud Scheduler API enabled there? (use --project, or: gcloud services enable cloudscheduler.googleapis.com --project %s)", action, project, project)
	}
	return fmt.Errorf("failed to %s: %w", action, err)
}

// GetJob returns details about a specific Cloud Scheduler job.
func GetJob(ctx context.Context, project, region, name string) (*JobInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Scheduler service: %w", err)
	}

	apilog.Logf("[Scheduler] Jobs.Get(%s, %s, %s)", project, region, name)
	job, err := svc.Projects.Locations.Jobs.Get(jobName(project, region, name)).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get scheduler job %s: %w", name, err)
	}
	return jobInfoFrom(job), nil
}

// PauseJob pauses (disables) a Cloud Scheduler job.
func PauseJob(ctx context.Context, project, region, name string) error {
	svc, err := GetService(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Scheduler service: %w", err)
	}

	apilog.Logf("[Scheduler] Jobs.Pause(%s, %s, %s)", project, region, name)
	_, err = svc.Projects.Locations.Jobs.Pause(jobName(project, region, name), &cloudscheduler.PauseJobRequest{}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("failed to pause scheduler job %s: %w", name, err)
	}
	return nil
}

// ResumeJob resumes (enables) a paused Cloud Scheduler job.
func ResumeJob(ctx context.Context, project, region, name string) error {
	svc, err := GetService(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Scheduler service: %w", err)
	}

	apilog.Logf("[Scheduler] Jobs.Resume(%s, %s, %s)", project, region, name)
	_, err = svc.Projects.Locations.Jobs.Resume(jobName(project, region, name), &cloudscheduler.ResumeJobRequest{}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("failed to resume scheduler job %s: %w", name, err)
	}
	return nil
}
