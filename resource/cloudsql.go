package resource

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thieso2/cio/cloudsql"
	"github.com/thieso2/cio/resolver"
)

const TypeCloudSQL Type = "sql"

// CloudSQLResource implements Resource for Cloud SQL instances.
type CloudSQLResource struct {
	formatter PathFormatter
}

func CreateCloudSQLResource(formatter PathFormatter) *CloudSQLResource {
	return &CloudSQLResource{formatter: formatter}
}

func (r *CloudSQLResource) Type() Type { return TypeCloudSQL }

// parseCloudSQLPath parses sql://name or sql://name/databases
func parseCloudSQLPath(path string) (name, sub string) {
	rest := strings.TrimPrefix(path, "sql://")
	parts := strings.SplitN(rest, "/", 2)
	name = parts[0]
	if len(parts) > 1 {
		sub = parts[1]
	}
	return
}

func (r *CloudSQLResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project string
	if opts != nil {
		project = opts.ProjectID
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for Cloud SQL (use --project flag or set defaults.project_id in config)")
	}

	name, sub := parseCloudSQLPath(path)

	// sql://instance/databases → list databases
	if name != "" && sub == "databases" {
		return r.listDatabases(ctx, project, name)
	}

	// sql:// or sql://pattern* → list instances
	instances, err := cloudsql.ListInstances(ctx, project)
	if err != nil {
		return nil, err
	}

	var resources []*ResourceInfo
	for _, inst := range instances {
		if name != "" && !resolver.MatchPattern(inst.Name, name) {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     inst.Name,
			Path:     "sql://" + inst.Name,
			Type:     "instance",
			Created:  inst.Created,
			Modified: inst.Created,
			Metadata: inst,
		})
	}
	return resources, nil
}

func (r *CloudSQLResource) listDatabases(ctx context.Context, project, instance string) ([]*ResourceInfo, error) {
	dbs, err := cloudsql.ListDatabases(ctx, project, instance)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, db := range dbs {
		resources = append(resources, &ResourceInfo{
			Name:     db.Name,
			Path:     "sql://" + instance + "/databases/" + db.Name,
			Type:     "database",
			Metadata: db,
		})
	}
	return resources, nil
}

func (r *CloudSQLResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	name, _ := parseCloudSQLPath(path)
	if name == "" {
		return fmt.Errorf("instance name required for delete")
	}
	var project string
	if opts != nil {
		project = opts.Project
	}
	if project == "" {
		return fmt.Errorf("project ID is required")
	}

	// Wildcard: match and delete multiple
	if strings.ContainsAny(name, "*?") {
		return r.removeMatching(ctx, project, name, opts)
	}

	if !confirm(opts != nil && opts.Force, fmt.Sprintf("Delete Cloud SQL instance %s? (y/N): ", name)) {
		return nil
	}

	start := time.Now()
	if err := cloudsql.DeleteInstance(ctx, project, name); err != nil {
		return err
	}
	fmt.Printf("Deleted: %s (took %s)\n", name, time.Since(start).Round(time.Millisecond))
	return nil
}

func (r *CloudSQLResource) removeMatching(ctx context.Context, project, pattern string, opts *RemoveOptions) error {
	instances, err := cloudsql.ListInstances(ctx, project)
	if err != nil {
		return err
	}

	var matched []*cloudsql.InstanceInfo
	for _, inst := range instances {
		if resolver.MatchPattern(inst.Name, pattern) {
			matched = append(matched, inst)
		}
	}

	if len(matched) == 0 {
		fmt.Println("No matching instances found.")
		return nil
	}

	fmt.Printf("Found %d instance(s) to delete:\n", len(matched))
	for _, inst := range matched {
		fmt.Printf("  - %s (%s)\n", inst.Name, inst.State)
	}
	fmt.Println()

	if !confirm(opts != nil && opts.Force, fmt.Sprintf("Delete all %d instance(s)? (y/N): ", len(matched))) {
		return nil
	}

	return bulkRun(ctx, matched,
		func(i *cloudsql.InstanceInfo) string { return i.Name },
		"Deleted",
		func(ctx context.Context, i *cloudsql.InstanceInfo) error {
			return cloudsql.DeleteInstance(ctx, project, i.Name)
		})
}

// InfoWithProject returns detailed info about a Cloud SQL instance.
func (r *CloudSQLResource) InfoWithProject(ctx context.Context, path, project string) (*ResourceInfo, error) {
	name, _ := parseCloudSQLPath(path)
	if name == "" {
		return nil, fmt.Errorf("instance name required for info")
	}

	inst, err := cloudsql.GetInstance(ctx, project, name)
	if err != nil {
		return nil, err
	}
	return &ResourceInfo{
		Name:     inst.Name,
		Path:     path,
		Type:     "instance",
		Created:  inst.Created,
		Modified: inst.Created,
		Metadata: inst,
	}, nil
}

func (r *CloudSQLResource) FormatShort(info *ResourceInfo, _ string) string {
	return metaShort(info)
}

func (r *CloudSQLResource) FormatLong(info *ResourceInfo, _ string) string {
	return metaLong(info)
}

func (r *CloudSQLResource) FormatDetailed(info *ResourceInfo, _ string) string {
	if inst, ok := info.Metadata.(*cloudsql.InstanceInfo); ok {
		return inst.FormatDetailed()
	}
	return r.FormatLong(info, "")
}

func (r *CloudSQLResource) FormatHeader() string {
	return cloudsql.InstanceLongHeader()
}

func (r *CloudSQLResource) FormatLongHeader() string {
	return cloudsql.InstanceLongHeader()
}

// MatchCloudSQLInstances lists instances matching a pattern.
func MatchCloudSQLInstances(ctx context.Context, path, project string) ([]*cloudsql.InstanceInfo, error) {
	name, _ := parseCloudSQLPath(path)

	instances, err := cloudsql.ListInstances(ctx, project)
	if err != nil {
		return nil, err
	}

	if name == "" {
		return instances, nil
	}

	var matched []*cloudsql.InstanceInfo
	for _, inst := range instances {
		if resolver.MatchPattern(inst.Name, name) {
			matched = append(matched, inst)
		}
	}
	return matched, nil
}

// StopCloudSQLInstances stops matched instances in parallel.
func StopCloudSQLInstances(ctx context.Context, project string, instances []*cloudsql.InstanceInfo, force bool) error {
	// Filter to running instances
	var toStop []*cloudsql.InstanceInfo
	for _, inst := range instances {
		if inst.State == "RUNNABLE" {
			toStop = append(toStop, inst)
		}
	}
	if len(toStop) == 0 {
		fmt.Println("No running instances to stop.")
		return nil
	}

	fmt.Printf("Found %d running instance(s) to stop:\n", len(toStop))
	for _, inst := range toStop {
		fmt.Printf("  - %s (%s, %s)\n", inst.Name, inst.DatabaseVersion, inst.Tier)
	}
	fmt.Println()

	if !confirm(force, fmt.Sprintf("Stop all %d instance(s)? (y/N): ", len(toStop))) {
		return nil
	}

	return parallelCloudSQL(ctx, project, toStop, "Stopped", cloudsql.StopInstance)
}

// StartCloudSQLInstances starts matched instances in parallel.
func StartCloudSQLInstances(ctx context.Context, project string, instances []*cloudsql.InstanceInfo, force bool) error {
	// Filter to stopped instances
	var toStart []*cloudsql.InstanceInfo
	for _, inst := range instances {
		if inst.State == "SUSPENDED" || inst.State == "STOPPED" {
			toStart = append(toStart, inst)
		}
	}
	if len(toStart) == 0 {
		fmt.Println("No stopped instances to start.")
		return nil
	}

	fmt.Printf("Found %d stopped instance(s) to start:\n", len(toStart))
	for _, inst := range toStart {
		fmt.Printf("  - %s (%s, %s)\n", inst.Name, inst.DatabaseVersion, inst.Tier)
	}
	fmt.Println()

	if !confirm(force, fmt.Sprintf("Start all %d instance(s)? (y/N): ", len(toStart))) {
		return nil
	}

	return parallelCloudSQL(ctx, project, toStart, "Started", cloudsql.StartInstance)
}

func parallelCloudSQL(ctx context.Context, project string, instances []*cloudsql.InstanceInfo, verb string, action func(context.Context, string, string) error) error {
	return bulkRun(ctx, instances,
		func(i *cloudsql.InstanceInfo) string { return i.Name },
		verb,
		func(ctx context.Context, i *cloudsql.InstanceInfo) error {
			return action(ctx, project, i.Name)
		})
}
