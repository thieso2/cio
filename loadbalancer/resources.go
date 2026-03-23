package loadbalancer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thieso2/cio/apilog"
	compute "google.golang.org/api/compute/v1"
)

// extractName gets the short name from a full GCP resource URL.
func extractName(url string) string {
	if idx := strings.LastIndex(url, "/"); idx >= 0 {
		return url[idx+1:]
	}
	return url
}

func parseTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

// UrlMapInfo holds information about a URL map (load balancer).
type UrlMapInfo struct {
	Name           string
	DefaultService string
	HostRuleCount  int
	Created        time.Time
}

func (u *UrlMapInfo) FormatShort() string { return u.Name }

func (u *UrlMapInfo) FormatLong() string {
	created := u.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%-55s %-40s %5d  %s", u.Name, u.DefaultService, u.HostRuleCount, created)
}

func UrlMapLongHeader() string {
	return fmt.Sprintf("%-55s %-40s %5s  %s", "NAME", "DEFAULT SERVICE", "HOSTS", "CREATED")
}

// ForwardingRuleInfo holds information about a forwarding rule.
type ForwardingRuleInfo struct {
	Name                string
	IPAddress           string
	Protocol            string
	PortRange           string
	Target              string
	LoadBalancingScheme string
	Created             time.Time
}

func (f *ForwardingRuleInfo) FormatShort() string { return f.Name }

func (f *ForwardingRuleInfo) FormatLong() string {
	created := f.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%-55s %-16s %-6s %-12s %-20s %s",
		f.Name, f.IPAddress, f.Protocol, f.PortRange, f.LoadBalancingScheme, created)
}

func ForwardingRuleLongHeader() string {
	return fmt.Sprintf("%-55s %-16s %-6s %-12s %-20s %s",
		"NAME", "IP", "PROTO", "PORTS", "SCHEME", "CREATED")
}

// BackendServiceInfo holds information about a backend service.
type BackendServiceInfo struct {
	Name         string
	Protocol     string
	BackendCount int
	TimeoutSec   int64
	Created      time.Time
}

func (b *BackendServiceInfo) FormatShort() string { return b.Name }

func (b *BackendServiceInfo) FormatLong() string {
	created := b.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%-55s %-8s %8d %8d  %s",
		b.Name, b.Protocol, b.BackendCount, b.TimeoutSec, created)
}

func BackendServiceLongHeader() string {
	return fmt.Sprintf("%-55s %-8s %8s %8s  %s",
		"NAME", "PROTOCOL", "BACKENDS", "TIMEOUT", "CREATED")
}

// ListUrlMaps lists all URL maps in a project.
func ListUrlMaps(ctx context.Context, project string) ([]*UrlMapInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute service: %w", err)
	}

	apilog.Logf("[LoadBalancer] UrlMaps.List(%s)", project)

	var result []*UrlMapInfo
	err = svc.UrlMaps.List(project).Pages(ctx, func(list *compute.UrlMapList) error {
		for _, um := range list.Items {
			result = append(result, &UrlMapInfo{
				Name:           um.Name,
				DefaultService: extractName(um.DefaultService),
				HostRuleCount:  len(um.HostRules),
				Created:        parseTime(um.CreationTimestamp),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list URL maps: %w", err)
	}
	return result, nil
}

// ListForwardingRules lists all global forwarding rules in a project.
func ListForwardingRules(ctx context.Context, project string) ([]*ForwardingRuleInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute service: %w", err)
	}

	apilog.Logf("[LoadBalancer] GlobalForwardingRules.List(%s)", project)

	var result []*ForwardingRuleInfo
	err = svc.GlobalForwardingRules.List(project).Pages(ctx, func(list *compute.ForwardingRuleList) error {
		for _, fr := range list.Items {
			result = append(result, &ForwardingRuleInfo{
				Name:                fr.Name,
				IPAddress:           fr.IPAddress,
				Protocol:            fr.IPProtocol,
				PortRange:           fr.PortRange,
				Target:              extractName(fr.Target),
				LoadBalancingScheme: fr.LoadBalancingScheme,
				Created:             parseTime(fr.CreationTimestamp),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list forwarding rules: %w", err)
	}
	return result, nil
}

// ListBackendServices lists all backend services in a project.
func ListBackendServices(ctx context.Context, project string) ([]*BackendServiceInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute service: %w", err)
	}

	apilog.Logf("[LoadBalancer] BackendServices.List(%s)", project)

	var result []*BackendServiceInfo
	err = svc.BackendServices.List(project).Pages(ctx, func(list *compute.BackendServiceList) error {
		for _, bs := range list.Items {
			result = append(result, &BackendServiceInfo{
				Name:         bs.Name,
				Protocol:     bs.Protocol,
				BackendCount: len(bs.Backends),
				TimeoutSec:   bs.TimeoutSec,
				Created:      parseTime(bs.CreationTimestamp),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list backend services: %w", err)
	}
	return result, nil
}
