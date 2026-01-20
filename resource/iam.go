package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/iam"
)

// IAMResource implements Resource for IAM resources
type IAMResource struct {
	formatter PathFormatter
}

// CreateIAMResource creates a new IAM resource handler
func CreateIAMResource(formatter PathFormatter) *IAMResource {
	return &IAMResource{
		formatter: formatter,
	}
}

// Type returns the resource type
func (r *IAMResource) Type() Type {
	return TypeIAM
}

// SupportsInfo returns whether this resource type supports detailed info
func (r *IAMResource) SupportsInfo() bool {
	return true
}

// List lists IAM resources
func (r *IAMResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	// Parse IAM path
	projectID, resourceType, err := iam.ParseIAMPath(path)
	if err != nil {
		return nil, err
	}

	// Only support service-accounts for now
	if resourceType != "service-accounts" && resourceType != "" {
		return nil, fmt.Errorf("unsupported IAM resource type: %s (only 'service-accounts' is supported)", resourceType)
	}

	// List service accounts
	accounts, err := iam.ListServiceAccounts(ctx, projectID)
	if err != nil {
		return nil, err
	}

	// Convert to ResourceInfo
	var resources []*ResourceInfo
	for _, account := range accounts {
		resources = append(resources, &ResourceInfo{
			Name:     account.Email,
			Path:     fmt.Sprintf("iam://%s/service-accounts/%s", projectID, account.Email),
			Size:     0, // Service accounts don't have size
			Modified: iam.DummyTime,
			IsDir:    false,
			Type:     "service-account",
			// Store service account info for formatting
			Metadata: account,
		})
	}

	return resources, nil
}

// Remove removes IAM resources (not supported)
func (r *IAMResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	return fmt.Errorf("removing IAM resources is not supported via cio (use gcloud or console)")
}

// Info returns detailed information about an IAM resource
func (r *IAMResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	// Parse IAM path
	projectID, _, err := iam.ParseIAMPath(path)
	if err != nil {
		return nil, err
	}

	// Extract service account email from path
	// Format: iam://project-id/service-accounts/email
	parts := strings.Split(path, "/")
	if len(parts) < 5 {
		return nil, fmt.Errorf("invalid IAM path format")
	}

	accountEmail := parts[4]

	// Get service account details
	account, err := iam.GetServiceAccount(ctx, projectID, accountEmail)
	if err != nil {
		return nil, err
	}

	return &ResourceInfo{
		Name:     account.Email,
		Path:     path,
		Size:     0,
		Modified: iam.DummyTime,
		IsDir:    false,
		Type:     "service-account",
		Metadata: account,
	}, nil
}

// ParsePath parses an IAM path
func (r *IAMResource) ParsePath(path string) (*PathComponents, error) {
	projectID, _, err := iam.ParseIAMPath(path)
	if err != nil {
		return nil, err
	}

	return &PathComponents{
		ResourceType: TypeIAM,
		Project:      projectID,
	}, nil
}

// FormatShort formats resource info in short format
func (r *IAMResource) FormatShort(info *ResourceInfo, aliasPath string) string {
	if account, ok := info.Metadata.(*iam.ServiceAccountInfo); ok {
		return account.FormatShort()
	}
	return info.Name
}

// FormatLong formats resource info in long format
func (r *IAMResource) FormatLong(info *ResourceInfo, aliasPath string) string {
	if account, ok := info.Metadata.(*iam.ServiceAccountInfo); ok {
		return account.FormatLong()
	}
	return info.Name
}

// FormatDetailed formats resource info with all details
func (r *IAMResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	if account, ok := info.Metadata.(*iam.ServiceAccountInfo); ok {
		return account.FormatDetailed(aliasPath)
	}
	return info.Name
}

// FormatHeader returns the header for long format listing
func (r *IAMResource) FormatHeader() string {
	return iam.FormatHeader()
}

// FormatLongHeader returns the header line for long format listing
func (r *IAMResource) FormatLongHeader() string {
	return iam.FormatHeader()
}
