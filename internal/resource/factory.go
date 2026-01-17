package resource

import (
	"fmt"

	"github.com/thieso2/cio/internal/resolver"
)

// Factory creates Resource instances based on path type
type Factory struct {
	formatter PathFormatter
}

// NewFactory creates a new resource factory
func NewFactory(formatter PathFormatter) *Factory {
	return &Factory{
		formatter: formatter,
	}
}

// Create creates the appropriate resource handler for the given path
func (f *Factory) Create(path string) (Resource, error) {
	if resolver.IsBQPath(path) {
		return NewBigQueryResource(f.formatter), nil
	}

	if resolver.IsGCSPath(path) {
		return NewGCSResource(f.formatter), nil
	}

	return nil, fmt.Errorf("unknown resource type for path: %s", path)
}

// CreateFromType creates a resource handler for the specified type
func (f *Factory) CreateFromType(resourceType Type) (Resource, error) {
	switch resourceType {
	case TypeGCS:
		return NewGCSResource(f.formatter), nil
	case TypeBigQuery:
		return NewBigQueryResource(f.formatter), nil
	default:
		return nil, fmt.Errorf("unknown resource type: %s", resourceType)
	}
}
