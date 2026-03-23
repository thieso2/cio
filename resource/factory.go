package resource

import (
	"fmt"

	"github.com/thieso2/cio/resolver"
)

// Factory creates Resource instances based on path type
type Factory struct {
	formatter PathFormatter
}

// CreateFactory creates a new resource factory
func CreateFactory(formatter PathFormatter) *Factory {
	return &Factory{
		formatter: formatter,
	}
}

// Create creates the appropriate resource handler for the given path
func (f *Factory) Create(path string) (Resource, error) {
	if resolver.IsBQPath(path) {
		return CreateBigQueryResource(f.formatter), nil
	}

	if resolver.IsGCSPath(path) {
		return CreateGCSResource(f.formatter), nil
	}

	if resolver.IsIAMPath(path) {
		return CreateIAMResource(f.formatter), nil
	}

	if resolver.IsCloudRunPath(path) {
		return CreateCloudRunResource(f.formatter), nil
	}

	if resolver.IsDataflowPath(path) {
		return CreateDataflowResource(f.formatter), nil
	}

	if resolver.IsVMPath(path) {
		return CreateVMResource(f.formatter), nil
	}

	if resolver.IsPubSubPath(path) {
		return CreatePubSubResource(f.formatter), nil
	}

	if resolver.IsCloudSQLPath(path) {
		return CreateCloudSQLResource(f.formatter), nil
	}

	if resolver.IsLoadBalancerPath(path) {
		return CreateLoadBalancerResource(f.formatter), nil
	}

	if resolver.IsCertManagerPath(path) {
		return CreateCertManagerResource(f.formatter), nil
	}

	if resolver.IsProjectsPath(path) {
		return CreateProjectsResource(f.formatter), nil
	}

	return nil, fmt.Errorf("unknown resource type for path: %s", path)
}

// CreateFromType creates a resource handler for the specified type
func (f *Factory) CreateFromType(resourceType Type) (Resource, error) {
	switch resourceType {
	case TypeGCS:
		return CreateGCSResource(f.formatter), nil
	case TypeBigQuery:
		return CreateBigQueryResource(f.formatter), nil
	case TypeIAM:
		return CreateIAMResource(f.formatter), nil
	case TypeCloudRunService, TypeCloudRunJob, TypeCloudRunWorker:
		return CreateCloudRunResource(f.formatter), nil
	case TypeDataflow:
		return CreateDataflowResource(f.formatter), nil
	case TypeVM:
		return CreateVMResource(f.formatter), nil
	case TypePubSub:
		return CreatePubSubResource(f.formatter), nil
	case TypeCloudSQL:
		return CreateCloudSQLResource(f.formatter), nil
	case TypeLoadBalancer:
		return CreateLoadBalancerResource(f.formatter), nil
	case TypeCertManager:
		return CreateCertManagerResource(f.formatter), nil
	case TypeProjects:
		return CreateProjectsResource(f.formatter), nil
	default:
		return nil, fmt.Errorf("unknown resource type: %s", resourceType)
	}
}
