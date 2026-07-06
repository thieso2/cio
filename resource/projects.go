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
	ProjectID   string            `json:"project_id"`
	DisplayName string            `json:"display_name,omitempty"`
	State       string            `json:"state"`
	Number      string            `json:"number,omitempty"`
	Parent      string            `json:"parent,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Created     time.Time         `json:"created"`
	Updated     time.Time         `json:"updated,omitempty"`
}

func (p *ProjectInfo) FormatShort() string {
	return p.ProjectID
}

func (p *ProjectInfo) FormatLong() string {
	created := p.Created.Format("2006-01-02")
	return fmt.Sprintf("%s\t%s\t%s", p.ProjectID, p.DisplayName, created)
}

func ProjectsLongHeader() string {
	return "PROJECT ID\tNAME\tCREATED"
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

func (r *ProjectsResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create projects client: %w", err)
	}
	defer client.Close()

	// Extract optional filter pattern from path (e.g. projects://iom-*)
	pattern := trimProjectsScheme(path)

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

		info := projectInfoFrom(p)

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
	if p, ok := info.Metadata.(*ProjectInfo); ok {
		return p.FormatDetailed()
	}
	return r.FormatLong(info, aliasPath)
}

// trimProjectsScheme strips the projects:// or project:// prefix.
func trimProjectsScheme(path string) string {
	path = strings.TrimPrefix(path, "projects://")
	return strings.TrimPrefix(path, "project://")
}

func projectInfoFrom(p *resourcemanagerpb.Project) *ProjectInfo {
	info := &ProjectInfo{
		ProjectID:   p.ProjectId,
		DisplayName: p.DisplayName,
		State:       p.State.String(),
		Number:      strings.TrimPrefix(p.Name, "projects/"),
		Parent:      p.Parent,
		Labels:      p.Labels,
	}
	if p.CreateTime != nil {
		info.Created = p.CreateTime.AsTime()
	}
	if p.UpdateTime != nil {
		info.Updated = p.UpdateTime.AsTime()
	}
	return info
}

// FormatDetailed formats a project with full details.
func (p *ProjectInfo) FormatDetailed() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Project ID:   %s\n", p.ProjectID)
	if p.DisplayName != "" {
		fmt.Fprintf(&b, "Display Name: %s\n", p.DisplayName)
	}
	if p.Number != "" {
		fmt.Fprintf(&b, "Number:       %s\n", p.Number)
	}
	fmt.Fprintf(&b, "State:        %s\n", p.State)
	if p.Parent != "" {
		fmt.Fprintf(&b, "Parent:       %s\n", p.Parent)
	}
	if !p.Created.IsZero() {
		fmt.Fprintf(&b, "Created:      %s\n", p.Created.Format("2006-01-02 15:04:05"))
	}
	if !p.Updated.IsZero() {
		fmt.Fprintf(&b, "Updated:      %s\n", p.Updated.Format("2006-01-02 15:04:05"))
	}
	if len(p.Labels) > 0 {
		keys := make([]string, 0, len(p.Labels))
		for k := range p.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "Labels:\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "  %s: %s\n", k, p.Labels[k])
		}
	}
	return b.String()
}

// Remove deletes GCP projects. Deletion is a soft delete: the project is shut
// down immediately, enters DELETE_REQUESTED state, and is purged after ~30
// days (recoverable until then via UndeleteProject). Wildcards are allowed;
// matching projects are listed for confirmation first.
func (r *ProjectsResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	target := strings.TrimSuffix(trimProjectsScheme(path), "/")
	if target == "" {
		return fmt.Errorf("project ID or pattern required (e.g. cio rm project://my-project-id)")
	}

	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create projects client: %w", err)
	}
	defer client.Close()

	force := opts != nil && opts.Force

	if resolver.HasWildcard(target) {
		return r.removeMatching(ctx, client, target, force)
	}

	p, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{
		Name: "projects/" + target,
	})
	if err != nil {
		return fmt.Errorf("failed to get project %s: %w", target, err)
	}
	if p.State != resourcemanagerpb.Project_ACTIVE {
		return fmt.Errorf("project %s is not active (state: %s)", target, p.State)
	}

	label := p.ProjectId
	if p.DisplayName != "" && p.DisplayName != p.ProjectId {
		label = fmt.Sprintf("%s (%s)", p.ProjectId, p.DisplayName)
	}
	if !confirm(force, fmt.Sprintf("Delete project %s? It shuts down immediately (recoverable for ~30 days). (y/N): ", label)) {
		return nil
	}

	start := time.Now()
	if err := deleteProject(ctx, client, p.ProjectId); err != nil {
		return fmt.Errorf("failed to delete project %s: %w", p.ProjectId, err)
	}
	fmt.Printf("Delete requested: %s (took %s)\n", p.ProjectId, time.Since(start).Round(time.Millisecond))
	return nil
}

func (r *ProjectsResource) removeMatching(ctx context.Context, client *resourcemanager.ProjectsClient, pattern string, force bool) error {
	it := client.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{})

	var matched []*resourcemanagerpb.Project
	for {
		p, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list projects: %w", err)
		}
		if p.State != resourcemanagerpb.Project_ACTIVE {
			continue
		}
		if !resolver.MatchPattern(p.ProjectId, pattern) {
			continue
		}
		matched = append(matched, p)
	}

	if len(matched) == 0 {
		fmt.Println("No matching projects found.")
		return nil
	}

	fmt.Printf("Found %d project(s) to delete:\n", len(matched))
	for _, p := range matched {
		if p.DisplayName != "" && p.DisplayName != p.ProjectId {
			fmt.Printf("  - %s (%s)\n", p.ProjectId, p.DisplayName)
		} else {
			fmt.Printf("  - %s\n", p.ProjectId)
		}
	}
	fmt.Println()

	if !confirm(force, fmt.Sprintf("Delete all %d project(s)? They shut down immediately (recoverable for ~30 days). (y/N): ", len(matched))) {
		return nil
	}

	return bulkRun(ctx, matched,
		func(p *resourcemanagerpb.Project) string { return p.ProjectId },
		"Delete requested",
		func(ctx context.Context, p *resourcemanagerpb.Project) error {
			return deleteProject(ctx, client, p.ProjectId)
		})
}

func deleteProject(ctx context.Context, client *resourcemanager.ProjectsClient, projectID string) error {
	op, err := client.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{
		Name: "projects/" + projectID,
	})
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	return err
}

// Info returns detailed info about a single project (project://my-project-id).
func (r *ProjectsResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	projectID := trimProjectsScheme(path)
	if projectID == "" {
		return nil, fmt.Errorf("project ID required for info (e.g. project://my-project-id)")
	}

	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create projects client: %w", err)
	}
	defer client.Close()

	p, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{
		Name: "projects/" + projectID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get project %s: %w", projectID, err)
	}

	info := projectInfoFrom(p)
	return &ResourceInfo{
		Name:     info.ProjectID,
		Path:     path,
		Type:     "project",
		Created:  info.Created,
		Metadata: info,
	}, nil
}

func (r *ProjectsResource) FormatHeader() string {
	return ProjectsLongHeader()
}

func (r *ProjectsResource) FormatLongHeader() string {
	return ProjectsLongHeader()
}

// ListProjectIDs returns active project IDs matching the given pattern.
func ListProjectIDs(ctx context.Context, pattern string) ([]string, error) {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create projects client: %w", err)
	}
	defer client.Close()

	it := client.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{})

	var ids []string
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
		ids = append(ids, p.ProjectId)
	}
	sort.Strings(ids)
	return ids, nil
}
