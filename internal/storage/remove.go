package storage

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// RemoveObject removes a single object from GCS
func RemoveObject(ctx context.Context, client *storage.Client, bucket, object string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, object)

	obj := client.Bucket(bucket).Object(object)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	// Always log deletions
	fmt.Printf("Deleted: %s\n", formatter(fullGCSPath))
	return nil
}

// RemoveDirectory removes all objects with a given prefix
func RemoveDirectory(ctx context.Context, client *storage.Client, bucket, prefix string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	// List all objects with the prefix
	bkt := client.Bucket(bucket)
	query := &storage.Query{
		Prefix: prefix,
	}

	deleteCount := 0
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, attrs.Name)

		// Delete the object
		obj := bkt.Object(attrs.Name)
		if err := obj.Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete %s: %w", formatter(fullGCSPath), err)
		}

		// Always log deletions line-by-line
		fmt.Printf("Deleted: %s\n", formatter(fullGCSPath))
		deleteCount++
	}

	if deleteCount == 0 {
		return fmt.Errorf("no objects found with prefix gs://%s/%s", bucket, prefix)
	}

	if deleteCount > 1 {
		fmt.Printf("\nTotal: %d objects deleted\n", deleteCount)
	}
	return nil
}

// RemoveWithPattern removes objects matching a wildcard pattern
func RemoveWithPattern(ctx context.Context, client *storage.Client, bucket, pattern string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	// Extract prefix and wildcard pattern
	prefix, wildcardPattern := splitPattern(pattern)

	// List all objects with the prefix
	bkt := client.Bucket(bucket)
	query := &storage.Query{
		Prefix: prefix,
	}

	deleteCount := 0
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Check if object matches the pattern
		if !matchesPattern(attrs.Name, wildcardPattern) {
			continue
		}

		fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, attrs.Name)

		// Delete the object
		obj := bkt.Object(attrs.Name)
		if err := obj.Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete %s: %w", formatter(fullGCSPath), err)
		}

		// Always log deletions line-by-line
		fmt.Printf("Deleted: %s\n", formatter(fullGCSPath))
		deleteCount++
	}

	if deleteCount == 0 {
		return fmt.Errorf("no objects found matching pattern: %s", pattern)
	}

	if deleteCount > 1 {
		fmt.Printf("\nTotal: %d objects deleted\n", deleteCount)
	}
	return nil
}

// splitPattern splits a path with wildcards into prefix and pattern
// Example: "logs/2024/*.log" -> ("logs/2024/", "*.log")
func splitPattern(path string) (prefix, pattern string) {
	lastSlash := strings.LastIndex(path, "/")
	if lastSlash == -1 {
		return "", path
	}

	prefix = path[:lastSlash+1]
	pattern = path[lastSlash+1:]
	return prefix, pattern
}

// matchesPattern checks if a filename matches a wildcard pattern
// Supports * (any characters) and ? (single character)
func matchesPattern(name, pattern string) bool {
	// Extract just the filename from the full path
	lastSlash := strings.LastIndex(name, "/")
	filename := name
	if lastSlash != -1 {
		filename = name[lastSlash+1:]
	}

	return wildcardMatch(filename, pattern)
}

// wildcardMatch implements simple wildcard matching
// * matches any sequence of characters
// ? matches any single character
func wildcardMatch(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	if pattern == "*" {
		return true
	}

	// Simple implementation for common cases
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return text == pattern
	}

	// Handle * wildcard
	if strings.Contains(pattern, "*") {
		parts := strings.Split(pattern, "*")
		if len(parts) == 2 {
			// Pattern like "*.log" or "test*"
			prefix := parts[0]
			suffix := parts[1]

			if prefix != "" && !strings.HasPrefix(text, prefix) {
				return false
			}
			if suffix != "" && !strings.HasSuffix(text, suffix) {
				return false
			}
			return true
		}
	}

	// For more complex patterns, use a simple character-by-character match
	return complexWildcardMatch(text, pattern)
}

// complexWildcardMatch handles more complex wildcard patterns
func complexWildcardMatch(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	if pattern == "*" {
		return true
	}

	i, j := 0, 0
	starIdx, matchIdx := -1, 0

	for i < len(text) {
		if j < len(pattern) && (pattern[j] == '?' || pattern[j] == text[i]) {
			i++
			j++
		} else if j < len(pattern) && pattern[j] == '*' {
			starIdx = j
			matchIdx = i
			j++
		} else if starIdx != -1 {
			j = starIdx + 1
			matchIdx++
			i = matchIdx
		} else {
			return false
		}
	}

	for j < len(pattern) && pattern[j] == '*' {
		j++
	}

	return j == len(pattern)
}
