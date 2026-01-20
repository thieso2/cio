package fuse

import (
	"context"
	"encoding/json"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/iam"
	bqpkg "github.com/thieso2/cio/bigquery"
	storagepkg "github.com/thieso2/cio/storage"
)

// fetchBucketIAMPolicy fetches the IAM policy for a GCS bucket
func fetchBucketIAMPolicy(ctx context.Context, bucketName string) (*iam.Policy, error) {
	client, err := storagepkg.GetClient(ctx)
	if err != nil {
		return nil, err
	}

	policy, err := client.Bucket(bucketName).IAM().Policy(ctx)
	if err != nil {
		return nil, err
	}

	return policy, nil
}

// fetchDatasetIAMPolicy fetches the IAM policy (access entries) for a BigQuery dataset
func fetchDatasetIAMPolicy(ctx context.Context, projectID, datasetID string) ([]*bigquery.AccessEntry, error) {
	client, err := bqpkg.GetClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	metadata, err := client.Dataset(datasetID).Metadata(ctx)
	if err != nil {
		return nil, err
	}

	return metadata.Access, nil
}

// formatGCSPolicyAsJSON converts a GCS IAM policy to formatted JSON
func formatGCSPolicyAsJSON(policy *iam.Policy) ([]byte, error) {
	// Convert policy to a simple structure
	type binding struct {
		Role    string   `json:"role"`
		Members []string `json:"members"`
	}

	roles := policy.Roles()
	bindings := make([]binding, 0, len(roles))
	for _, role := range roles {
		members := policy.Members(role)
		bindings = append(bindings, binding{
			Role:    string(role),
			Members: members,
		})
	}

	result := map[string]interface{}{
		"version":  "1.0",
		"type":     "gcs_iam_policy",
		"bindings": bindings,
	}

	return json.MarshalIndent(result, "", "  ")
}

// formatBQAccessAsJSON converts BigQuery access entries to formatted JSON
func formatBQAccessAsJSON(entries []*bigquery.AccessEntry) ([]byte, error) {
	// Convert access entries to a simple structure
	type accessEntry struct {
		Role       string `json:"role,omitempty"`
		Entity     string `json:"entity,omitempty"`
		EntityType string `json:"entity_type"`
	}

	accessList := make([]accessEntry, 0, len(entries))
	for _, entry := range entries {
		ae := accessEntry{}

		// Determine entity type and entity
		if entry.Role != "" {
			ae.Role = string(entry.Role)
		}

		if entry.Entity != "" {
			ae.Entity = string(entry.Entity)
		}

		if string(entry.EntityType) != "" {
			ae.EntityType = string(entry.EntityType)
		} else if entry.Entity != "" {
			// Infer entity type from entity string
			entityStr := string(entry.Entity)
			switch {
			case strings.HasPrefix(entityStr, "user:"):
				ae.EntityType = "user"
			case strings.HasPrefix(entityStr, "serviceAccount:"):
				ae.EntityType = "serviceAccount"
			case strings.HasPrefix(entityStr, "group:"):
				ae.EntityType = "group"
			case strings.HasPrefix(entityStr, "domain:"):
				ae.EntityType = "domain"
			case entityStr == "allUsers" || entityStr == "allAuthenticatedUsers":
				ae.EntityType = "special"
			default:
				ae.EntityType = "unknown"
			}
		}

		accessList = append(accessList, ae)
	}

	result := map[string]interface{}{
		"version": "1.0",
		"type":    "bigquery_access",
		"access":  accessList,
	}

	return json.MarshalIndent(result, "", "  ")
}

// extractGCSRoles extracts a map of roles to members from a GCS policy
func extractGCSRoles(policy *iam.Policy) map[string][]string {
	rolesMap := make(map[string][]string)

	for _, role := range policy.Roles() {
		members := policy.Members(role)
		// Sanitize role name (remove roles/ prefix)
		cleanRole := sanitizeRoleName(string(role))
		rolesMap[cleanRole] = members
	}

	return rolesMap
}

// extractGCSMembers extracts a map of members to roles from a GCS policy
func extractGCSMembers(policy *iam.Policy) map[string][]string {
	membersMap := make(map[string][]string)

	for _, role := range policy.Roles() {
		cleanRole := sanitizeRoleName(string(role))
		memberList := policy.Members(role)
		for _, member := range memberList {
			cleanMember := sanitizeMemberName(member)
			membersMap[cleanMember] = append(membersMap[cleanMember], cleanRole)
		}
	}

	return membersMap
}

// extractBQRoles extracts a map of roles to entities from BigQuery access entries
func extractBQRoles(entries []*bigquery.AccessEntry) map[string][]string {
	roles := make(map[string][]string)

	for _, entry := range entries {
		if entry.Role == "" {
			continue
		}

		role := string(entry.Role)
		entity := string(entry.Entity)
		if entity == "" {
			continue
		}

		cleanEntity := sanitizeMemberName(entity)
		roles[role] = append(roles[role], cleanEntity)
	}

	return roles
}

// extractBQMembers extracts a map of entities to roles from BigQuery access entries
func extractBQMembers(entries []*bigquery.AccessEntry) map[string][]string {
	members := make(map[string][]string)

	for _, entry := range entries {
		if entry.Role == "" || entry.Entity == "" {
			continue
		}

		role := string(entry.Role)
		entity := string(entry.Entity)
		cleanEntity := sanitizeMemberName(entity)

		members[cleanEntity] = append(members[cleanEntity], role)
	}

	return members
}

// sanitizeMemberName sanitizes member/entity names for use as filesystem names
// Replaces : with @ to avoid path issues
func sanitizeMemberName(member string) string {
	// Replace : with @ (e.g., "user:alice@example.com" -> "user@alice@example.com")
	return strings.ReplaceAll(member, ":", "@")
}

// sanitizeRoleName sanitizes role names by removing the roles/ prefix
func sanitizeRoleName(role string) string {
	// Remove "roles/" prefix if present
	return strings.TrimPrefix(role, "roles/")
}

// unsanitizeMemberName reverses sanitizeMemberName for display purposes
func unsanitizeMemberName(sanitized string) string {
	// Find the first @ and replace it with :
	// "user@alice@example.com" -> "user:alice@example.com"
	parts := strings.SplitN(sanitized, "@", 2)
	if len(parts) == 2 {
		return parts[0] + ":" + parts[1]
	}
	return sanitized
}
