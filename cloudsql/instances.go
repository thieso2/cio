package cloudsql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thieso2/cio/apilog"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

// InstanceInfo holds information about a Cloud SQL instance.
type InstanceInfo struct {
	Name             string
	State            string
	DatabaseVersion  string
	Region           string
	Tier             string
	IP               string
	DiskSizeGB       int64
	AvailabilityType string
	ConnectionName   string
	Created          time.Time
	raw              *sqladmin.DatabaseInstance
}

// DatabaseInfo holds information about a database in a Cloud SQL instance.
type DatabaseInfo struct {
	Name    string
	Charset string
}

func instanceInfoFrom(inst *sqladmin.DatabaseInstance) *InstanceInfo {
	info := &InstanceInfo{
		Name:            inst.Name,
		State:           inst.State,
		DatabaseVersion: shortVersion(inst.DatabaseVersion),
		Region:          inst.Region,
		ConnectionName:  inst.ConnectionName,
		raw:             inst,
	}
	if inst.Settings != nil {
		info.Tier = inst.Settings.Tier
		info.DiskSizeGB = inst.Settings.DataDiskSizeGb
		info.AvailabilityType = inst.Settings.AvailabilityType
	}
	if len(inst.IpAddresses) > 0 {
		// Prefer PRIVATE, then PRIMARY
		for _, ip := range inst.IpAddresses {
			if ip.Type == "PRIVATE" {
				info.IP = ip.IpAddress
				break
			}
		}
		if info.IP == "" {
			info.IP = inst.IpAddresses[0].IpAddress
		}
	}
	if inst.CreateTime != "" {
		if t, err := time.Parse(time.RFC3339, inst.CreateTime); err == nil {
			info.Created = t
		}
	}
	return info
}

func shortVersion(v string) string {
	// POSTGRES_15 -> PG15, MYSQL_8_0 -> MY8.0
	v = strings.ReplaceAll(v, "POSTGRES_", "PG")
	v = strings.ReplaceAll(v, "MYSQL_", "MY")
	v = strings.ReplaceAll(v, "SQLSERVER_", "SS")
	return v
}

// FormatShort formats an instance in short format.
func (i *InstanceInfo) FormatShort() string { return i.Name }

// FormatLong formats an instance in long format.
func (i *InstanceInfo) FormatLong() string {
	created := i.Created.Format("2006-01-02 15:04:05")
	ip := i.IP
	if ip == "" {
		ip = "-"
	}
	ha := "ZONAL"
	if i.AvailabilityType == "REGIONAL" {
		ha = "HA"
	}
	return fmt.Sprintf("%-55s %-12s %-8s %-20s %-12s %-16s %-6s %s",
		i.Name, i.State, i.DatabaseVersion, i.Tier, i.Region, ip, ha, created)
}

// InstanceLongHeader returns the header for long instance listing.
func InstanceLongHeader() string {
	return fmt.Sprintf("%-55s %-12s %-8s %-20s %-12s %-16s %-6s %s",
		"NAME", "STATE", "VERSION", "TIER", "REGION", "IP", "HA", "CREATED")
}

// FormatDetailed formats an instance with full details.
func (i *InstanceInfo) FormatDetailed() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Name:             %s\n", i.Name)
	fmt.Fprintf(&b, "State:            %s\n", i.State)
	fmt.Fprintf(&b, "Database Version: %s\n", i.DatabaseVersion)
	fmt.Fprintf(&b, "Tier:             %s\n", i.Tier)
	fmt.Fprintf(&b, "Region:           %s\n", i.Region)
	fmt.Fprintf(&b, "Availability:     %s\n", i.AvailabilityType)
	fmt.Fprintf(&b, "Disk Size:        %d GB\n", i.DiskSizeGB)
	fmt.Fprintf(&b, "Connection Name:  %s\n", i.ConnectionName)
	if i.IP != "" {
		fmt.Fprintf(&b, "IP:               %s\n", i.IP)
	}
	if i.raw != nil {
		for _, ip := range i.raw.IpAddresses {
			fmt.Fprintf(&b, "  %-8s %s\n", ip.Type, ip.IpAddress)
		}
	}
	fmt.Fprintf(&b, "Created:          %s\n", i.Created.Format("2006-01-02 15:04:05"))
	return b.String()
}

// ListInstances lists all Cloud SQL instances in a project.
func ListInstances(ctx context.Context, project string) ([]*InstanceInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud SQL service: %w", err)
	}

	apilog.Logf("[CloudSQL] Instances.List(%s)", project)

	var instances []*InstanceInfo
	err = svc.Instances.List(project).Pages(ctx, func(resp *sqladmin.InstancesListResponse) error {
		for _, inst := range resp.Items {
			instances = append(instances, instanceInfoFrom(inst))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list instances: %w", err)
	}
	return instances, nil
}

// GetInstance returns details about a specific Cloud SQL instance.
func GetInstance(ctx context.Context, project, name string) (*InstanceInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud SQL service: %w", err)
	}

	apilog.Logf("[CloudSQL] Instances.Get(%s, %s)", project, name)
	inst, err := svc.Instances.Get(project, name).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get instance %s: %w", name, err)
	}
	return instanceInfoFrom(inst), nil
}

// StopInstance stops a Cloud SQL instance by setting activation policy to NEVER.
func StopInstance(ctx context.Context, project, name string) error {
	svc, err := GetService(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud SQL service: %w", err)
	}

	apilog.Logf("[CloudSQL] Instances.Patch(%s, %s, ActivationPolicy=NEVER)", project, name)
	op, err := svc.Instances.Patch(project, name, &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{ActivationPolicy: "NEVER"},
	}).Do()
	if err != nil {
		return fmt.Errorf("failed to stop instance %s: %w", name, err)
	}
	return waitForOperation(ctx, svc, project, op.Name)
}

// StartInstance starts a Cloud SQL instance by setting activation policy to ALWAYS.
func StartInstance(ctx context.Context, project, name string) error {
	svc, err := GetService(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud SQL service: %w", err)
	}

	apilog.Logf("[CloudSQL] Instances.Patch(%s, %s, ActivationPolicy=ALWAYS)", project, name)
	op, err := svc.Instances.Patch(project, name, &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{ActivationPolicy: "ALWAYS"},
	}).Do()
	if err != nil {
		return fmt.Errorf("failed to start instance %s: %w", name, err)
	}
	return waitForOperation(ctx, svc, project, op.Name)
}

// DeleteInstance deletes a Cloud SQL instance.
func DeleteInstance(ctx context.Context, project, name string) error {
	svc, err := GetService(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Cloud SQL service: %w", err)
	}

	apilog.Logf("[CloudSQL] Instances.Delete(%s, %s)", project, name)
	op, err := svc.Instances.Delete(project, name).Do()
	if err != nil {
		return fmt.Errorf("failed to delete instance %s: %w", name, err)
	}
	return waitForOperation(ctx, svc, project, op.Name)
}

// ListDatabases lists databases in a Cloud SQL instance.
func ListDatabases(ctx context.Context, project, instance string) ([]*DatabaseInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud SQL service: %w", err)
	}

	apilog.Logf("[CloudSQL] Databases.List(%s, %s)", project, instance)
	resp, err := svc.Databases.List(project, instance).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	var dbs []*DatabaseInfo
	for _, db := range resp.Items {
		dbs = append(dbs, &DatabaseInfo{
			Name:    db.Name,
			Charset: db.Charset,
		})
	}
	return dbs, nil
}

func waitForOperation(ctx context.Context, svc *sqladmin.Service, project, opName string) error {
	for {
		op, err := svc.Operations.Get(project, opName).Do()
		if err != nil {
			return fmt.Errorf("failed to poll operation: %w", err)
		}
		if op.Status == "DONE" {
			if op.Error != nil && len(op.Error.Errors) > 0 {
				return fmt.Errorf("operation failed: %s", op.Error.Errors[0].Message)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
