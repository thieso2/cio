package iam

import (
	"context"
	"fmt"
	"time"
)

// ServiceAccountInfo represents information about a service account.
type ServiceAccountInfo struct {
	Email       string
	Name        string
	DisplayName string
	Description string
	Disabled    bool
	ProjectID   string
}

// FormatShort formats service account info in short format (just email).
func (sa *ServiceAccountInfo) FormatShort() string {
	return sa.Email
}

// FormatShortWithAlias formats service account info with alias path.
func (sa *ServiceAccountInfo) FormatShortWithAlias(aliasPath string) string {
	return sa.Email
}

// FormatLong formats service account info in long format (email, displayName, disabled).
func (sa *ServiceAccountInfo) FormatLong() string {
	disabled := "False"
	if sa.Disabled {
		disabled = "True"
	}

	displayName := sa.DisplayName
	if displayName == "" {
		displayName = "-"
	}

	return fmt.Sprintf("%-60s %-30s %s", sa.Email, displayName, disabled)
}

// FormatDetailed formats service account info with all details.
func (sa *ServiceAccountInfo) FormatDetailed(aliasPath string) string {
	output := fmt.Sprintf("Service Account: %s\n", sa.Email)

	if sa.DisplayName != "" {
		output += fmt.Sprintf("Display Name: %s\n", sa.DisplayName)
	}

	if sa.Description != "" {
		output += fmt.Sprintf("Description: %s\n", sa.Description)
	}

	output += fmt.Sprintf("Disabled: %t\n", sa.Disabled)
	output += fmt.Sprintf("Project: %s\n", sa.ProjectID)
	output += fmt.Sprintf("Name: %s\n", sa.Name)

	return output
}

// ListServiceAccounts lists all service accounts in a project.
func ListServiceAccounts(ctx context.Context, projectID string) ([]*ServiceAccountInfo, error) {
	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create IAM client: %w", err)
	}

	// Construct project resource name
	projectResource := fmt.Sprintf("projects/%s", projectID)

	// List service accounts
	resp, err := client.Projects.ServiceAccounts.List(projectResource).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to list service accounts: %w", err)
	}

	var accounts []*ServiceAccountInfo
	for _, sa := range resp.Accounts {
		accounts = append(accounts, &ServiceAccountInfo{
			Email:       sa.Email,
			Name:        sa.Name,
			DisplayName: sa.DisplayName,
			Description: sa.Description,
			Disabled:    sa.Disabled,
			ProjectID:   projectID,
		})
	}

	return accounts, nil
}

// GetServiceAccount retrieves details about a specific service account.
func GetServiceAccount(ctx context.Context, projectID, accountEmail string) (*ServiceAccountInfo, error) {
	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create IAM client: %w", err)
	}

	// Construct service account resource name
	resourceName := fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, accountEmail)

	// Get service account
	sa, err := client.Projects.ServiceAccounts.Get(resourceName).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get service account: %w", err)
	}

	return &ServiceAccountInfo{
		Email:       sa.Email,
		Name:        sa.Name,
		DisplayName: sa.DisplayName,
		Description: sa.Description,
		Disabled:    sa.Disabled,
		ProjectID:   projectID,
	}, nil
}

// FormatHeader returns the header for long format listing.
func FormatHeader() string {
	return fmt.Sprintf("%-60s %-30s %s", "EMAIL", "DISPLAY_NAME", "DISABLED")
}

// ParseIAMPath parses an IAM path and returns the project ID and resource type.
// Expected format: iam://project-id/service-accounts
func ParseIAMPath(path string) (projectID string, resourceType string, err error) {
	// Remove iam:// prefix
	if len(path) < 6 || path[:6] != "iam://" {
		return "", "", fmt.Errorf("invalid IAM path format, expected iam://")
	}

	path = path[6:] // Remove "iam://"

	// Split by /
	parts := []rune(path)
	slashIdx := -1
	for i, r := range parts {
		if r == '/' {
			slashIdx = i
			break
		}
	}

	if slashIdx == -1 {
		// No slash, just project ID
		return path, "", nil
	}

	projectID = path[:slashIdx]
	resourceType = path[slashIdx+1:]

	return projectID, resourceType, nil
}

// DummyTime is a placeholder time for service accounts (they don't have modification times)
var DummyTime = time.Now()

// ServiceAccountKeyInfo represents information about a service account key.
type ServiceAccountKeyInfo struct {
	Name            string
	KeyID           string
	KeyType         string // USER_MANAGED or SYSTEM_MANAGED
	ValidAfterTime  time.Time
	ValidBeforeTime time.Time
	KeyAlgorithm    string
	Disabled        bool
}

// ListServiceAccountKeys lists all keys for a service account.
func ListServiceAccountKeys(ctx context.Context, projectID, accountEmail string) ([]*ServiceAccountKeyInfo, error) {
	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create IAM client: %w", err)
	}

	// Construct service account resource name
	resourceName := fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, accountEmail)

	// List keys
	resp, err := client.Projects.ServiceAccounts.Keys.List(resourceName).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to list service account keys: %w", err)
	}

	var keys []*ServiceAccountKeyInfo
	for _, key := range resp.Keys {
		// Extract key ID from name (format: projects/{project}/serviceAccounts/{account}/keys/{keyid})
		keyID := ""
		if len(key.Name) > 0 {
			parts := []rune(key.Name)
			lastSlash := -1
			for i := len(parts) - 1; i >= 0; i-- {
				if parts[i] == '/' {
					lastSlash = i
					break
				}
			}
			if lastSlash != -1 && lastSlash < len(parts)-1 {
				keyID = string(parts[lastSlash+1:])
			}
		}

		validAfter, _ := time.Parse(time.RFC3339, key.ValidAfterTime)
		validBefore, _ := time.Parse(time.RFC3339, key.ValidBeforeTime)

		keys = append(keys, &ServiceAccountKeyInfo{
			Name:            key.Name,
			KeyID:           keyID,
			KeyType:         key.KeyType,
			ValidAfterTime:  validAfter,
			ValidBeforeTime: validBefore,
			KeyAlgorithm:    key.KeyAlgorithm,
			Disabled:        key.Disabled,
		})
	}

	return keys, nil
}

// UsageInfo represents a resource where a service account has permissions.
type UsageInfo struct {
	ResourceType string // "storage", "bigquery", "compute", etc.
	ResourceName string // bucket name, dataset name, instance name, etc.
	Roles        []string
}

// GetServiceAccountUsage finds all resources where a service account has IAM permissions.
// This uses the Cloud Asset Inventory API to search across all resources.
func GetServiceAccountUsage(ctx context.Context, projectID, accountEmail string) ([]*UsageInfo, error) {
	// TODO: Implement using Cloud Asset Inventory API
	// For now, return a placeholder implementation that checks common resource types

	var usage []*UsageInfo

	// This is a simplified implementation. A full implementation would use:
	// - cloudasset.googleapis.com/v1 API
	// - SearchAllIamPolicies method
	// - Filter by the service account email

	// For now, we'll return an empty list with a note
	// Users can implement this by enabling Cloud Asset Inventory API

	return usage, nil
}
