package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/cloudrun"
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
		if p.name == "" {
			return r.listJobs(ctx, project, region)
		}
		return r.listExecutions(ctx, project, region, p.name)
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

func (r *CloudRunResource) listExecutions(ctx context.Context, project, region, jobName string) ([]*ResourceInfo, error) {
	executions, err := cloudrun.ListExecutions(ctx, project, region, jobName)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, exec := range executions {
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

// Remove is not supported for Cloud Run resources.
func (r *CloudRunResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	return fmt.Errorf("removing Cloud Run resources via cio is not supported (use gcloud or the console)")
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
		return cloudrun.JobLongHeader() // or ExecutionLongHeader — resolved at List() time below
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
	}
	return ""
}

