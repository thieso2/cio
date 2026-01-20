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
