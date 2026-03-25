package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	computeapi "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// InstanceInfo holds information about a Compute Engine VM instance.
type InstanceInfo struct {
	Name        string    `json:"name"`
	Zone        string    `json:"zone"`
	MachineType string    `json:"machine_type"`
	Status      string    `json:"status"`
	InternalIP  string    `json:"internal_ip,omitempty"`
	ExternalIP  string    `json:"external_ip,omitempty"`
	Created     time.Time `json:"created"`
}

// FormatShort formats an instance in short format (zone/name).
func (i *InstanceInfo) FormatShort() string {
	return fmt.Sprintf("%-20s %s", shortZone(i.Zone), i.Name)
}

// FormatLong formats an instance in long format.
func (i *InstanceInfo) FormatLong() string {
	created := i.Created.Format("2006-01-02 15:04:05")
	zone := shortZone(i.Zone)
	machineType := shortMachineType(i.MachineType)
	ip := i.ExternalIP
	if ip == "" {
		ip = i.InternalIP
	}
	if ip == "" {
		ip = "-"
	}
	return fmt.Sprintf("%-55s %-12s %-22s %-20s %-16s %s", i.Name, i.Status, machineType, zone, ip, created)
}

// InstanceLongHeader returns the header for long instance listing.
func InstanceLongHeader() string {
	return fmt.Sprintf("%-55s %-12s %-22s %-20s %-16s %s", "NAME", "STATUS", "MACHINE_TYPE", "ZONE", "IP", "CREATED")
}

// shortZone extracts the zone name from a full zone URL.
// e.g. "projects/p/zones/us-central1-a" → "us-central1-a"
func shortZone(zone string) string {
	if idx := strings.LastIndex(zone, "/"); idx != -1 {
		return zone[idx+1:]
	}
	return zone
}

// shortMachineType extracts the machine type from a full URL.
// e.g. "projects/p/zones/z/machineTypes/e2-medium" → "e2-medium"
func shortMachineType(mt string) string {
	if idx := strings.LastIndex(mt, "/"); idx != -1 {
		return mt[idx+1:]
	}
	return mt
}

// ListInstances lists VM instances in the given project and zone.
// If zone is empty, lists instances across all zones (aggregated list).
func ListInstances(ctx context.Context, project, zone string) ([]*InstanceInfo, error) {
	client, err := GetInstancesClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Compute Engine client: %w", err)
	}

	if zone != "" {
		return listInstancesInZone(ctx, client, project, zone)
	}
	return listInstancesAllZones(ctx, client, project)
}

func listInstancesInZone(ctx context.Context, client *computeapi.InstancesClient, project, zone string) ([]*InstanceInfo, error) {
	apilog.Logf("[Compute] Instances.List(project=%s, zone=%s)", project, zone)

	it := client.List(ctx, &computepb.ListInstancesRequest{
		Project: project,
		Zone:    zone,
	})

	var instances []*InstanceInfo
	for {
		inst, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list instances: %w", err)
		}
		instances = append(instances, protoToInstanceInfo(inst))
	}
	return instances, nil
}

func listInstancesAllZones(ctx context.Context, client *computeapi.InstancesClient, project string) ([]*InstanceInfo, error) {
	apilog.Logf("[Compute] Instances.AggregatedList(project=%s)", project)

	it := client.AggregatedList(ctx, &computepb.AggregatedListInstancesRequest{
		Project: project,
	})

	var instances []*InstanceInfo
	for {
		pair, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list instances: %w", err)
		}
		if pair.Value != nil && pair.Value.Instances != nil {
			for _, inst := range pair.Value.Instances {
				instances = append(instances, protoToInstanceInfo(inst))
			}
		}
	}
	return instances, nil
}

func protoToInstanceInfo(inst *computepb.Instance) *InstanceInfo {
	info := &InstanceInfo{
		Name:        inst.GetName(),
		Zone:        shortZone(inst.GetZone()),
		MachineType: inst.GetMachineType(),
		Status:      inst.GetStatus(),
	}
	if inst.CreationTimestamp != nil {
		if t, err := time.Parse(time.RFC3339, inst.GetCreationTimestamp()); err == nil {
			info.Created = t
		}
	}
	// Extract IPs from network interfaces
	for _, ni := range inst.GetNetworkInterfaces() {
		if info.InternalIP == "" && ni.GetNetworkIP() != "" {
			info.InternalIP = ni.GetNetworkIP()
		}
		for _, ac := range ni.GetAccessConfigs() {
			if info.ExternalIP == "" && ac.GetNatIP() != "" {
				info.ExternalIP = ac.GetNatIP()
			}
		}
	}
	return info
}

// StopInstance stops a VM instance.
func StopInstance(ctx context.Context, project, zone, name string) error {
	client, err := GetInstancesClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Compute Engine client: %w", err)
	}

	apilog.Logf("[Compute] Instances.Stop(project=%s, zone=%s, instance=%s)", project, zone, name)
	op, err := client.Stop(ctx, &computepb.StopInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: name,
	})
	if err != nil {
		return fmt.Errorf("failed to stop instance %s: %w", name, err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for stop of %s: %w", name, err)
	}
	return nil
}

// DeleteInstance deletes a VM instance.
func DeleteInstance(ctx context.Context, project, zone, name string) error {
	client, err := GetInstancesClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Compute Engine client: %w", err)
	}

	apilog.Logf("[Compute] Instances.Delete(project=%s, zone=%s, instance=%s)", project, zone, name)
	op, err := client.Delete(ctx, &computepb.DeleteInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: name,
	})
	if err != nil {
		return fmt.Errorf("failed to delete instance %s: %w", name, err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for deletion of %s: %w", name, err)
	}
	return nil
}

// GetSerialPortOutput fetches serial port output from a VM instance.
func GetSerialPortOutput(ctx context.Context, project, zone, name string, start int64) (string, int64, error) {
	client, err := GetInstancesClient(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create Compute Engine client: %w", err)
	}

	port := int32(1)
	apilog.Logf("[Compute] Instances.GetSerialPortOutput(project=%s, zone=%s, instance=%s, start=%d)", project, zone, name, start)
	resp, err := client.GetSerialPortOutput(ctx, &computepb.GetSerialPortOutputInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: name,
		Port:     &port,
		Start:    &start,
	})
	if err != nil {
		return "", 0, fmt.Errorf("failed to get serial port output: %w", err)
	}
	return resp.GetContents(), resp.GetNext(), nil
}
