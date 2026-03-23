package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/loadbalancer"
	"github.com/thieso2/cio/resolver"
)

const TypeLoadBalancer Type = "lb"

type LoadBalancerResource struct {
	formatter  PathFormatter
	subType    string // "url-maps", "forwarding-rules", "backends"
}

func CreateLoadBalancerResource(formatter PathFormatter) *LoadBalancerResource {
	return &LoadBalancerResource{formatter: formatter}
}

func (r *LoadBalancerResource) Type() Type        { return TypeLoadBalancer }
func (r *LoadBalancerResource) SupportsInfo() bool { return false }

func parseLBPath(path string) (subType, name string) {
	rest := strings.TrimPrefix(path, "lb://")
	parts := strings.SplitN(rest, "/", 2)
	subType = parts[0]
	if len(parts) > 1 {
		name = parts[1]
	}
	return
}

func (r *LoadBalancerResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project string
	if opts != nil {
		project = opts.ProjectID
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for load balancer resources")
	}

	subType, name := parseLBPath(path)
	r.subType = subType

	switch subType {
	case "", "url-maps":
		return r.listUrlMaps(ctx, project, name)
	case "forwarding-rules":
		return r.listForwardingRules(ctx, project, name)
	case "backends":
		return r.listBackendServices(ctx, project, name)
	default:
		return nil, fmt.Errorf("unknown lb sub-type: %s (use url-maps, forwarding-rules, or backends)", subType)
	}
}

func (r *LoadBalancerResource) listUrlMaps(ctx context.Context, project, pattern string) ([]*ResourceInfo, error) {
	items, err := loadbalancer.ListUrlMaps(ctx, project)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, item := range items {
		if pattern != "" && !resolver.MatchPattern(item.Name, pattern) {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     item.Name,
			Path:     "lb://url-maps/" + item.Name,
			Type:     "url-map",
			Created:  item.Created,
			Modified: item.Created,
			Metadata: item,
		})
	}
	return resources, nil
}

func (r *LoadBalancerResource) listForwardingRules(ctx context.Context, project, pattern string) ([]*ResourceInfo, error) {
	items, err := loadbalancer.ListForwardingRules(ctx, project)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, item := range items {
		if pattern != "" && !resolver.MatchPattern(item.Name, pattern) {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     item.Name,
			Path:     "lb://forwarding-rules/" + item.Name,
			Type:     "forwarding-rule",
			Created:  item.Created,
			Modified: item.Created,
			Metadata: item,
		})
	}
	return resources, nil
}

func (r *LoadBalancerResource) listBackendServices(ctx context.Context, project, pattern string) ([]*ResourceInfo, error) {
	items, err := loadbalancer.ListBackendServices(ctx, project)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, item := range items {
		if pattern != "" && !resolver.MatchPattern(item.Name, pattern) {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     item.Name,
			Path:     "lb://backends/" + item.Name,
			Type:     "backend-service",
			Created:  item.Created,
			Modified: item.Created,
			Metadata: item,
		})
	}
	return resources, nil
}

func (r *LoadBalancerResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	return fmt.Errorf("removing load balancer resources is not supported via cio")
}

func (r *LoadBalancerResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("use 'cio ls -l lb://' for load balancer details")
}

func (r *LoadBalancerResource) ParsePath(path string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeLoadBalancer}, nil
}

func (r *LoadBalancerResource) FormatShort(info *ResourceInfo, _ string) string {
	switch v := info.Metadata.(type) {
	case *loadbalancer.UrlMapInfo:
		return v.FormatShort()
	case *loadbalancer.ForwardingRuleInfo:
		return v.FormatShort()
	case *loadbalancer.BackendServiceInfo:
		return v.FormatShort()
	}
	return info.Name
}

func (r *LoadBalancerResource) FormatLong(info *ResourceInfo, _ string) string {
	switch v := info.Metadata.(type) {
	case *loadbalancer.UrlMapInfo:
		return v.FormatLong()
	case *loadbalancer.ForwardingRuleInfo:
		return v.FormatLong()
	case *loadbalancer.BackendServiceInfo:
		return v.FormatLong()
	}
	return info.Name
}

func (r *LoadBalancerResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}

func (r *LoadBalancerResource) FormatHeader() string {
	return r.FormatLongHeader()
}

func (r *LoadBalancerResource) FormatLongHeader() string {
	switch r.subType {
	case "forwarding-rules":
		return loadbalancer.ForwardingRuleLongHeader()
	case "backends":
		return loadbalancer.BackendServiceLongHeader()
	default:
		return loadbalancer.UrlMapLongHeader()
	}
}
