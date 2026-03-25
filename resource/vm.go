package resource

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/thieso2/cio/compute"
)

const TypeVM Type = "vm"

// VMResource implements Resource for Compute Engine VM instances.
type VMResource struct {
	formatter  PathFormatter
	listingMode string // set by List() to drive FormatLongHeader()
}

// CreateVMResource creates a new VM resource handler.
func CreateVMResource(formatter PathFormatter) *VMResource {
	return &VMResource{formatter: formatter}
}

func (r *VMResource) Type() Type           { return TypeVM }
func (r *VMResource) SupportsInfo() bool   { return false }

func (r *VMResource) FormatLongHeader() string {
	if r.listingMode == "zones" {
		return fmt.Sprintf("%-30s %s", "ZONE", "INSTANCES")
	}
	return compute.InstanceLongHeader()
}

func (r *VMResource) ParsePath(p string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeVM}, nil
}

// parseVMPath splits vm://[zone][/name] into zone and name.
//
// The first segment is always the zone (or * for all zones).
// The second segment is the instance name or pattern.
//
//	vm://                          → zone="", name=""    (list zones)
//	vm://europe-west3-a            → zone="europe-west3-a", name=""
//	vm://europe-west3-a/my-vm      → zone="europe-west3-a", name="my-vm"
//	vm://*/iomp*                   → zone="*", name="iomp*"
//	vm://europe-west3-a/web-*      → zone="europe-west3-a", name="web-*"
func parseVMPath(p string) (zone, name string) {
	rest := strings.TrimPrefix(p, "vm://")
	rest = strings.TrimRight(rest, "/")
	if rest == "" {
		return "", ""
	}
	parts := strings.SplitN(rest, "/", 2)
	zone = parts[0]
	if len(parts) > 1 {
		name = parts[1]
	}
	return
}

// List lists VM zones or instances.
//
//	vm://                     → list zones (with instance counts)
//	vm://zone                 → list instances in zone
//	vm://*/                   → list all instances across all zones
//	vm://zone/pattern*        → instances matching pattern in zone
//	vm://*/pattern*           → instances matching pattern across all zones
func (r *VMResource) List(ctx context.Context, p string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project string
	if opts != nil {
		project = opts.ProjectID
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for VM (use --project flag or set defaults.project_id in config)")
	}

	zone, namePattern := parseVMPath(p)

	// vm:// with no zone → list zones
	if zone == "" {
		return r.listZones(ctx, project)
	}

	// Resolve zone: "*" means all zones
	queryZone := zone
	if zone == "*" {
		queryZone = ""
	}

	instances, err := compute.ListInstances(ctx, project, queryZone)
	if err != nil {
		return nil, err
	}

	r.listingMode = "instances"
	var resources []*ResourceInfo
	for _, inst := range instances {
		if namePattern != "" {
			if ok, _ := path.Match(namePattern, inst.Name); !ok {
				continue
			}
		}
		vmPath := "vm://" + inst.Zone + "/" + inst.Name
		resources = append(resources, &ResourceInfo{
			Name:     inst.Name,
			Path:     vmPath,
			Type:     "vm",
			Created:  inst.Created,
			Modified: inst.Created,
			Location: inst.Zone,
			Metadata: inst,
		})
	}
	return resources, nil
}

// listZones lists zones that have instances, with counts.
func (r *VMResource) listZones(ctx context.Context, project string) ([]*ResourceInfo, error) {
	instances, err := compute.ListInstances(ctx, project, "")
	if err != nil {
		return nil, err
	}

	// Count instances per zone
	zoneCounts := make(map[string]int)
	for _, inst := range instances {
		zoneCounts[inst.Zone]++
	}

	// Sort zone names
	var zones []string
	for z := range zoneCounts {
		zones = append(zones, z)
	}
	sort.Strings(zones)

	r.listingMode = "zones"
	var resources []*ResourceInfo
	for _, z := range zones {
		resources = append(resources, &ResourceInfo{
			Name:     z,
			Path:     "vm://" + z,
			Type:     "zone",
			Location: z,
			Metadata: &zoneInfo{Name: z, InstanceCount: zoneCounts[z]},
		})
	}
	return resources, nil
}

// zoneInfo is used as Metadata for zone listings.
type zoneInfo struct {
	Name          string `json:"name"`
	InstanceCount int    `json:"instance_count"`
}

// MatchVMInstances resolves a vm:// path to matching instances.
// Shared by Remove, Stop, and Tail.
func MatchVMInstances(ctx context.Context, p, project string) ([]*compute.InstanceInfo, error) {
	if project == "" {
		return nil, fmt.Errorf("project ID is required (use --project flag or set defaults.project_id in config)")
	}

	zone, name := parseVMPath(p)
	if zone == "" && name == "" {
		return nil, fmt.Errorf("zone and instance name are required: vm://zone/instance-name or vm://*/pattern*")
	}
	if name == "" {
		return nil, fmt.Errorf("instance name or pattern is required: vm://%s/instance-name", zone)
	}

	queryZone := zone
	if zone == "*" {
		queryZone = ""
	}

	instances, err := compute.ListInstances(ctx, project, queryZone)
	if err != nil {
		return nil, err
	}

	var matched []*compute.InstanceInfo
	for _, inst := range instances {
		if strings.ContainsAny(name, "*?") {
			if ok, _ := path.Match(name, inst.Name); ok {
				matched = append(matched, inst)
			}
		} else if inst.Name == name {
			matched = append(matched, inst)
		}
	}
	return matched, nil
}

// Remove stops and deletes VM instances in parallel.
func (r *VMResource) Remove(ctx context.Context, p string, opts *RemoveOptions) error {
	var project string
	if opts != nil {
		project = opts.Project
	}

	matched, err := MatchVMInstances(ctx, p, project)
	if err != nil {
		return err
	}

	if len(matched) == 0 {
		zone, name := parseVMPath(p)
		if zone == "*" {
			zone = "all zones"
		}
		fmt.Printf("No matching instances found for %q in project %s (%s).\n", name, project, zone)
		return nil
	}

	fmt.Printf("Found %d matching instance(s):\n", len(matched))
	for _, inst := range matched {
		fmt.Printf("  - %-40s %-12s %s\n", inst.Name, inst.Status, inst.Zone)
	}
	fmt.Println()

	if opts == nil || !opts.Force {
		fmt.Printf("Stop and delete %d instance(s)? (y/N): ", len(matched))
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	// Stop running instances in parallel
	running := filterByStatus(matched, "RUNNING")
	if len(running) > 0 {
		fmt.Printf("Stopping %d running instance(s)...\n", len(running))
		errs := parallelStop(ctx, project, running)
		for _, e := range errs {
			fmt.Printf("  error: %v\n", e)
		}
	}

	// Delete all instances in parallel
	fmt.Printf("Deleting %d instance(s)...\n", len(matched))
	errs := parallelDelete(ctx, project, matched)
	for _, e := range errs {
		fmt.Printf("  error: %v\n", e)
	}

	return nil
}

// StopVMInstances stops matched instances in parallel.
func StopVMInstances(ctx context.Context, project string, matched []*compute.InstanceInfo, force bool) error {
	if len(matched) == 0 {
		fmt.Println("No matching instances found.")
		return nil
	}

	fmt.Printf("Found %d matching instance(s):\n", len(matched))
	for _, inst := range matched {
		fmt.Printf("  - %-40s %-12s %s\n", inst.Name, inst.Status, inst.Zone)
	}
	fmt.Println()

	running := filterByStatus(matched, "RUNNING")
	if len(running) == 0 {
		fmt.Println("No running instances to stop.")
		return nil
	}

	if !force {
		fmt.Printf("Stop %d running instance(s)? (y/N): ", len(running))
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	fmt.Printf("Stopping %d instance(s)...\n", len(running))
	errs := parallelStop(ctx, project, running)
	for _, e := range errs {
		fmt.Printf("  error: %v\n", e)
	}
	return nil
}

func filterByStatus(instances []*compute.InstanceInfo, status string) []*compute.InstanceInfo {
	var result []*compute.InstanceInfo
	for _, inst := range instances {
		if inst.Status == status {
			result = append(result, inst)
		}
	}
	return result
}

func parallelStop(ctx context.Context, project string, instances []*compute.InstanceInfo) []error {
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup

	for _, inst := range instances {
		wg.Add(1)
		go func(inst *compute.InstanceInfo) {
			defer wg.Done()
			fmt.Printf("  stopping %s (%s)...\n", inst.Name, inst.Zone)
			if err := compute.StopInstance(ctx, project, inst.Zone, inst.Name); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", inst.Name, err))
				mu.Unlock()
				return
			}
			fmt.Printf("  stopped  %s\n", inst.Name)
		}(inst)
	}
	wg.Wait()
	return errs
}

func parallelDelete(ctx context.Context, project string, instances []*compute.InstanceInfo) []error {
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup

	for _, inst := range instances {
		wg.Add(1)
		go func(inst *compute.InstanceInfo) {
			defer wg.Done()
			fmt.Printf("  deleting %s (%s)...\n", inst.Name, inst.Zone)
			if err := compute.DeleteInstance(ctx, project, inst.Zone, inst.Name); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", inst.Name, err))
				mu.Unlock()
				return
			}
			fmt.Printf("  deleted  %s\n", inst.Name)
		}(inst)
	}
	wg.Wait()
	return errs
}

func (r *VMResource) Info(_ context.Context, _ string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("cio info is not yet supported for VM instances")
}

func (r *VMResource) FormatShort(info *ResourceInfo, _ string) string {
	if zi, ok := info.Metadata.(*zoneInfo); ok {
		return fmt.Sprintf("%s (%d instances)", zi.Name, zi.InstanceCount)
	}
	if inst, ok := info.Metadata.(*compute.InstanceInfo); ok {
		return inst.FormatShort()
	}
	return info.Name
}

func (r *VMResource) FormatLong(info *ResourceInfo, _ string) string {
	if zi, ok := info.Metadata.(*zoneInfo); ok {
		return fmt.Sprintf("%-30s %d", zi.Name, zi.InstanceCount)
	}
	if inst, ok := info.Metadata.(*compute.InstanceInfo); ok {
		return inst.FormatLong()
	}
	return info.Name
}

func (r *VMResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}
