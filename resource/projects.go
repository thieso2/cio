package resource

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/thieso2/cio/resolver"
	"google.golang.org/api/iterator"
)

const TypeProjects Type = "projects"

// ProjectInfo holds information about a GCP project.
type ProjectInfo struct {
	ProjectID   string
	DisplayName string
	State       string
	Created     time.Time
}

func (p *ProjectInfo) FormatShort() string {
	return p.ProjectID
}

func (p *ProjectInfo) FormatLong() string {
	created := p.Created.Format("2006-01-02")
	return fmt.Sprintf("%-40s %-40s %s", p.ProjectID, p.DisplayName, created)
}

func ProjectsLongHeader() string {
	return fmt.Sprintf("%-40s %-40s %s", "PROJECT ID", "NAME", "CREATED")
}

// ProjectsResource implements Resource for GCP projects
type ProjectsResource struct {
	formatter PathFormatter
}

func CreateProjectsResource(formatter PathFormatter) *ProjectsResource {
	return &ProjectsResource{formatter: formatter}
}

func (r *ProjectsResource) Type() Type {
	return TypeProjects
}

func (r *ProjectsResource) SupportsInfo() bool {
	return false
}

func (r *ProjectsResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create projects client: %w", err)
	}
	defer client.Close()

	// Extract optional filter pattern from path (e.g. projects://iom-*)
	pattern := strings.TrimPrefix(path, "projects://")

	it := client.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{})

	var resources []*ResourceInfo
	for {
		p, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list projects: %w", err)
		}
		if p.State != resourcemanagerpb.Project_ACTIVE {
			continue
		}
		if pattern != "" && !resolver.MatchPattern(p.ProjectId, pattern) {
			continue
		}

		info := &ProjectInfo{
			ProjectID:   p.ProjectId,
			DisplayName: p.DisplayName,
			State:       p.State.String(),
		}
		if p.CreateTime != nil {
			info.Created = p.CreateTime.AsTime()
		}

		resources = append(resources, &ResourceInfo{
			Name:     p.ProjectId,
			Path:     "projects://" + p.ProjectId,
			Created:  info.Created,
			Type:     "project",
			Metadata: info,
		})
	}

	sort.Slice(resources, func(i, j int) bool {
		return resources[i].Name < resources[j].Name
	})

	return resources, nil
}

func (r *ProjectsResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	return fmt.Errorf("removing projects is not supported via cio")
}

func (r *ProjectsResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("info is not supported for projects")
}

func (r *ProjectsResource) ParsePath(path string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeProjects}, nil
}

func (r *ProjectsResource) FormatShort(info *ResourceInfo, aliasPath string) string {
	if p, ok := info.Metadata.(*ProjectInfo); ok {
		return p.FormatShort()
	}
	return info.Name
}

func (r *ProjectsResource) FormatLong(info *ResourceInfo, aliasPath string) string {
	if p, ok := info.Metadata.(*ProjectInfo); ok {
		return p.FormatLong()
	}
	return info.Name
}

func (r *ProjectsResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}

func (r *ProjectsResource) FormatHeader() string {
	return ProjectsLongHeader()
}

func (r *ProjectsResource) FormatLongHeader() string {
	return ProjectsLongHeader()
}
