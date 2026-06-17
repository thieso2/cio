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
	Name           string    `json:"name"`
	DefaultService string    `json:"default_service"`
	HostRuleCount  int       `json:"host_rule_count"`
	Created        time.Time `json:"created"`
}

func (u *UrlMapInfo) FormatShort() string { return u.Name }

func (u *UrlMapInfo) FormatLong() string {
	created := u.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%s\t%s\t%d\t%s", u.Name, u.DefaultService, u.HostRuleCount, created)
}

func UrlMapLongHeader() string {
	return "NAME\tDEFAULT SERVICE\tHOSTS\tCREATED"
}

// ForwardingRuleInfo holds information about a forwarding rule.
type ForwardingRuleInfo struct {
	Name                string    `json:"name"`
	IPAddress           string    `json:"ip_address"`
	Protocol            string    `json:"protocol"`
	PortRange           string    `json:"port_range"`
	Target              string    `json:"target"`
	LoadBalancingScheme string    `json:"load_balancing_scheme"`
	Created             time.Time `json:"created"`
}

func (f *ForwardingRuleInfo) FormatShort() string { return f.Name }

func (f *ForwardingRuleInfo) FormatLong() string {
	created := f.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s",
		f.Name, f.IPAddress, f.Protocol, f.PortRange, f.LoadBalancingScheme, created)
}

func ForwardingRuleLongHeader() string {
	return "NAME\tIP\tPROTO\tPORTS\tSCHEME\tCREATED"
}

// BackendServiceInfo holds information about a backend service.
type BackendServiceInfo struct {
	Name         string    `json:"name"`
	Protocol     string    `json:"protocol"`
	BackendCount int       `json:"backend_count"`
	TimeoutSec   int64     `json:"timeout_sec"`
	Created      time.Time `json:"created"`
}

func (b *BackendServiceInfo) FormatShort() string { return b.Name }

func (b *BackendServiceInfo) FormatLong() string {
	created := b.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%s\t%s\t%d\t%d\t%s",
		b.Name, b.Protocol, b.BackendCount, b.TimeoutSec, created)
}

func BackendServiceLongHeader() string {
	return "NAME\tPROTOCOL\tBACKENDS\tTIMEOUT\tCREATED"
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
