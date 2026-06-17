package resource

import (
	"fmt"

	"github.com/thieso2/cio/resolver"
)

// Factory creates Resource instances based on path type
type Factory struct {
	formatter    PathFormatter
	BillingTable string // BigQuery billing export table for cost:// paths
}

// CreateFactory creates a new resource factory
func CreateFactory(formatter PathFormatter) *Factory {
	return &Factory{
		formatter: formatter,
	}
}

// schemeEntry maps a path-scheme predicate to the resource handler that serves
// it. The registry below is the single source of truth for scheme → handler:
// adding a resource type is one row, and Create() is a table walk instead of a
// hand-written if/else ladder. The match predicates live in resolver, so the
// scheme prefix strings themselves are still owned there.
type schemeEntry struct {
	matchPath func(string) bool
	construct func(f *Factory) Resource
}

var registry = []schemeEntry{
	{resolver.IsBQPath, func(f *Factory) Resource { return CreateBigQueryResource(f.formatter) }},
	{resolver.IsGCSPath, func(f *Factory) Resource { return CreateGCSResource(f.formatter) }},
	{resolver.IsIAMPath, func(f *Factory) Resource { return CreateIAMResource(f.formatter) }},
	{resolver.IsCloudRunPath, func(f *Factory) Resource { return CreateCloudRunResource(f.formatter) }},
	{resolver.IsDataflowPath, func(f *Factory) Resource { return CreateDataflowResource(f.formatter) }},
	{resolver.IsVMPath, func(f *Factory) Resource { return CreateVMResource(f.formatter) }},
	{resolver.IsPubSubPath, func(f *Factory) Resource { return CreatePubSubResource(f.formatter) }},
	{resolver.IsCloudSQLPath, func(f *Factory) Resource { return CreateCloudSQLResource(f.formatter) }},
	{resolver.IsLoadBalancerPath, func(f *Factory) Resource { return CreateLoadBalancerResource(f.formatter) }},
	{resolver.IsCertManagerPath, func(f *Factory) Resource { return CreateCertManagerResource(f.formatter) }},
	{resolver.IsProjectsPath, func(f *Factory) Resource { return CreateProjectsResource(f.formatter) }},
	{resolver.IsCostPath, func(f *Factory) Resource { return CreateCostResource(f.formatter, f.BillingTable) }},
}

// Create creates the appropriate resource handler for the given path
func (f *Factory) Create(path string) (Resource, error) {
	for _, e := range registry {
		if e.matchPath(path) {
			return e.construct(f), nil
		}
	}
	return nil, fmt.Errorf("unknown resource type for path: %s", path)
}
