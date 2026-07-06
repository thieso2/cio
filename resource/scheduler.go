package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/scheduler"
)

const TypeScheduler Type = "scheduler"

// SchedulerResource implements Resource for Cloud Scheduler jobs.
type SchedulerResource struct {
	formatter PathFormatter
	region    string // default region when ListOptions carries none (e.g. info)
}

func CreateSchedulerResource(formatter PathFormatter, region string) *SchedulerResource {
	return &SchedulerResource{formatter: formatter, region: region}
}

func (r *SchedulerResource) Type() Type { return TypeScheduler }

// parseSchedulerPath parses scheduler://name (name may be a wildcard pattern).
func parseSchedulerPath(path string) string {
	return strings.Trim(strings.TrimPrefix(path, "scheduler://"), "/")
}

func (r *SchedulerResource) resolveRegion(region string) (string, error) {
	if region == "" {
		region = r.region
	}
	if region == "" {
		return "", fmt.Errorf("region is required for Cloud Scheduler (use --region flag or set defaults.region in config)")
	}
	return region, nil
}

func (r *SchedulerResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project, region string
	if opts != nil {
		project = opts.ProjectID
		region = opts.Region
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for Cloud Scheduler (use --project flag or set defaults.project_id in config)")
	}
	region, err := r.resolveRegion(region)
	if err != nil {
		return nil, err
	}

	name := parseSchedulerPath(path)

	jobs, err := scheduler.ListJobs(ctx, project, region)
	if err != nil {
		return nil, err
	}

	var resources []*ResourceInfo
	for _, job := range jobs {
		if name != "" && !resolver.MatchPattern(job.Name, name) {
			continue
		}
		if opts != nil && opts.ActiveOnly && job.State != "ENABLED" {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     job.Name,
			Path:     "scheduler://" + job.Name,
			Type:     "job",
			Modified: job.LastAttempt,
			Metadata: job,
		})
	}
	return resources, nil
}

// InfoWithProject returns detailed info about a Cloud Scheduler job.
func (r *SchedulerResource) InfoWithProject(ctx context.Context, path, project string) (*ResourceInfo, error) {
	name := parseSchedulerPath(path)
	if name == "" {
		return nil, fmt.Errorf("job name required for info")
	}
	region, err := r.resolveRegion("")
	if err != nil {
		return nil, err
	}

	job, err := scheduler.GetJob(ctx, project, region, name)
	if err != nil {
		return nil, err
	}
	return &ResourceInfo{
		Name:     job.Name,
		Path:     path,
		Type:     "job",
		Modified: job.LastAttempt,
		Metadata: job,
	}, nil
}

func (r *SchedulerResource) FormatShort(info *ResourceInfo, _ string) string {
	return metaShort(info)
}

func (r *SchedulerResource) FormatLong(info *ResourceInfo, _ string) string {
	return metaLong(info)
}

func (r *SchedulerResource) FormatDetailed(info *ResourceInfo, _ string) string {
	if job, ok := info.Metadata.(*scheduler.JobInfo); ok {
		return job.FormatDetailed()
	}
	return r.FormatLong(info, "")
}

func (r *SchedulerResource) FormatLongHeader() string {
	return scheduler.JobLongHeader()
}

// MatchSchedulerJobs lists jobs matching a scheduler:// path pattern.
func MatchSchedulerJobs(ctx context.Context, path, project, region string) ([]*scheduler.JobInfo, error) {
	name := parseSchedulerPath(path)

	jobs, err := scheduler.ListJobs(ctx, project, region)
	if err != nil {
		return nil, err
	}

	if name == "" {
		return jobs, nil
	}

	var matched []*scheduler.JobInfo
	for _, job := range jobs {
		if resolver.MatchPattern(job.Name, name) {
			matched = append(matched, job)
		}
	}
	return matched, nil
}

// PauseSchedulerJobs pauses (disables) matched jobs in parallel.
func PauseSchedulerJobs(ctx context.Context, project, region string, jobs []*scheduler.JobInfo, force bool) error {
	// Filter to enabled jobs — only those can be paused
	var toPause []*scheduler.JobInfo
	for _, job := range jobs {
		if job.State == "ENABLED" {
			toPause = append(toPause, job)
		}
	}
	if len(toPause) == 0 {
		fmt.Println("No enabled jobs to pause.")
		return nil
	}

	fmt.Printf("Found %d enabled job(s) to pause:\n", len(toPause))
	for _, job := range toPause {
		fmt.Printf("  - %s (%s)\n", job.Name, job.Schedule)
	}
	fmt.Println()

	if !confirm(force, fmt.Sprintf("Pause all %d job(s)? (y/N): ", len(toPause))) {
		return nil
	}

	return parallelScheduler(ctx, project, region, toPause, "Paused", scheduler.PauseJob)
}

// ResumeSchedulerJobs resumes (enables) matched jobs in parallel.
func ResumeSchedulerJobs(ctx context.Context, project, region string, jobs []*scheduler.JobInfo, force bool) error {
	// Filter to paused jobs — only those can be resumed
	var toResume []*scheduler.JobInfo
	for _, job := range jobs {
		if job.State == "PAUSED" {
			toResume = append(toResume, job)
		}
	}
	if len(toResume) == 0 {
		fmt.Println("No paused jobs to resume.")
		return nil
	}

	fmt.Printf("Found %d paused job(s) to resume:\n", len(toResume))
	for _, job := range toResume {
		fmt.Printf("  - %s (%s)\n", job.Name, job.Schedule)
	}
	fmt.Println()

	if !confirm(force, fmt.Sprintf("Resume all %d job(s)? (y/N): ", len(toResume))) {
		return nil
	}

	return parallelScheduler(ctx, project, region, toResume, "Resumed", scheduler.ResumeJob)
}

func parallelScheduler(ctx context.Context, project, region string, jobs []*scheduler.JobInfo, verb string, action func(context.Context, string, string, string) error) error {
	return bulkRun(ctx, jobs,
		func(j *scheduler.JobInfo) string { return j.Name },
		verb,
		func(ctx context.Context, j *scheduler.JobInfo) error {
			return action(ctx, project, region, j.Name)
		})
}
