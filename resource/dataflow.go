package resource

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/thieso2/cio/dataflow"
)

const TypeDataflow Type = "dataflow"

// DataflowResource implements Resource for Dataflow jobs.
type DataflowResource struct {
	formatter PathFormatter
}

// CreateDataflowResource creates a new Dataflow resource handler.
func CreateDataflowResource(formatter PathFormatter) *DataflowResource {
	return &DataflowResource{formatter: formatter}
}

func (r *DataflowResource) Type() Type            { return TypeDataflow }
func (r *DataflowResource) SupportsInfo() bool     { return false }
func (r *DataflowResource) FormatLongHeader() string { return dataflow.JobLongHeader() }

func (r *DataflowResource) ParsePath(p string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeDataflow}, nil
}

// List lists Dataflow jobs. Supports:
//   - dataflow://          → list all active jobs
//   - dataflow://all       → list all jobs (active + terminated)
//   - dataflow://active    → list active jobs
//   - dataflow://terminated → list terminated jobs
//   - dataflow://pattern*  → filter by name pattern
func (r *DataflowResource) List(ctx context.Context, p string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project, region string
	if opts != nil {
		project = opts.ProjectID
		region = opts.Region
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for Dataflow (use --project flag or set defaults.project_id in config)")
	}
	if region == "" {
		return nil, fmt.Errorf("region is required for Dataflow (use --region flag or set defaults.region in config)")
	}

	rest := strings.TrimPrefix(p, "dataflow://")
	rest = strings.TrimRight(rest, "/")

	// Determine filter and optional name pattern.
	// Default is "all" (show active + completed). Use --active to restrict.
	filter := "all"
	if opts != nil && opts.ActiveOnly {
		filter = "active"
	}
	var namePattern string
	switch rest {
	case "":
		// use default filter (all, or active if --active)
	case "active":
		filter = "active"
	case "all":
		filter = "all"
	case "terminated":
		filter = "terminated"
	default:
		// Treat as name or wildcard pattern; fetch all to filter client-side.
		namePattern = rest
		filter = "all"
	}

	jobs, err := dataflow.ListJobs(ctx, project, region, filter)
	if err != nil {
		return nil, err
	}

	var resources []*ResourceInfo
	for _, job := range jobs {
		if namePattern != "" {
			if ok, _ := path.Match(namePattern, job.Name); !ok {
				continue
			}
		}
		resources = append(resources, &ResourceInfo{
			Name:     job.Name,
			Path:     "dataflow://" + job.ID,
			Type:     "dataflow-job",
			Created:  job.Created,
			Modified: job.StateTime,
			Metadata: job,
		})
	}
	return resources, nil
}

func (r *DataflowResource) Remove(_ context.Context, _ string, _ *RemoveOptions) error {
	return fmt.Errorf("removing Dataflow jobs via cio is not supported (use gcloud or the console)")
}

func (r *DataflowResource) Info(_ context.Context, _ string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("cio info is not yet supported for Dataflow jobs")
}

func (r *DataflowResource) FormatShort(info *ResourceInfo, _ string) string {
	if j, ok := info.Metadata.(*dataflow.JobInfo); ok {
		return j.FormatShort()
	}
	return info.Name
}

func (r *DataflowResource) FormatLong(info *ResourceInfo, _ string) string {
	if j, ok := info.Metadata.(*dataflow.JobInfo); ok {
		return j.FormatLong()
	}
	return info.Name
}

func (r *DataflowResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}
