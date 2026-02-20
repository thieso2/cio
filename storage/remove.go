package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// Default parallelism constants
const (
	// DefaultConcurrentDeletes is the default number of concurrent delete operations
	DefaultConcurrentDeletes = 50
)

// RemoveObject removes a single object from GCS
func RemoveObject(ctx context.Context, client *storage.Client, bucket, object string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, object)

	obj := client.Bucket(bucket).Object(object)
	apilog.Logf("[GCS] Object.Delete(gs://%s/%s)", bucket, object)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	// Always log deletions
	fmt.Printf("Deleted: %s\n", formatter(fullGCSPath))
	return nil
}

// RemoveDirectory removes all objects with a given prefix
func RemoveDirectory(ctx context.Context, client *storage.Client, bucket, prefix string, verbose bool, formatter PathFormatter, maxWorkers int) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	// List all objects with the prefix first to get total count
	bkt := client.Bucket(bucket)
	query := &storage.Query{
		Prefix: prefix,
	}

	// First pass: collect all objects to delete
	var objectsToDelete []string
	apilog.Logf("[GCS] Objects.List(bucket=%s, prefix=%q) for delete", bucket, prefix)
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}
		objectsToDelete = append(objectsToDelete, attrs.Name)
	}

	totalCount := len(objectsToDelete)
	if totalCount == 0 {
		return fmt.Errorf("no objects found with prefix gs://%s/%s", bucket, prefix)
	}

	// Second pass: delete in parallel with progress counter
	return deleteObjectsParallel(ctx, client, bucket, objectsToDelete, totalCount, formatter, maxWorkers)
}

// RemoveWithPattern removes objects matching a wildcard pattern
func RemoveWithPattern(ctx context.Context, client *storage.Client, bucket, pattern string, verbose bool, formatter PathFormatter, maxWorkers int) error {
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

	// First pass: collect all matching objects
	var objectsToDelete []string
	apilog.Logf("[GCS] Objects.List(bucket=%s, prefix=%q) for delete", bucket, prefix)
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

		objectsToDelete = append(objectsToDelete, attrs.Name)
	}

	totalCount := len(objectsToDelete)
	if totalCount == 0 {
		return fmt.Errorf("no objects found matching pattern: %s", pattern)
	}

	// Second pass: delete in parallel with progress counter
	return deleteObjectsParallel(ctx, client, bucket, objectsToDelete, totalCount, formatter, maxWorkers)
}

// deleteObjectsParallel deletes objects in parallel with controlled concurrency
func deleteObjectsParallel(ctx context.Context, client *storage.Client, bucket string, objectsToDelete []string, totalCount int, formatter PathFormatter, maxWorkers int) error {
	// Create a semaphore to limit concurrent deletes
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var completedCount int32

	// Channel for completed deletions (for progress tracking)
	type deletion struct {
		objectName string
		err        error
	}
	deletions := make(chan deletion, totalCount)

	// Start progress reporter goroutine
	done := make(chan struct{})
	go func() {
		for d := range deletions {
			count := atomic.AddInt32(&completedCount, 1)

			if d.err != nil {
				fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, d.objectName)
				fmt.Printf("Failed %d/%d: %s - %v\n", count, totalCount, formatter(fullGCSPath), d.err)

				// Store first error
				mu.Lock()
				if firstErr == nil {
					firstErr = d.err
				}
				mu.Unlock()
			} else {
				fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, d.objectName)
				fmt.Printf("Deleted %d/%d: %s\n", count, totalCount, formatter(fullGCSPath))
			}
		}
		close(done)
	}()

	// Delete objects in parallel
	bkt := client.Bucket(bucket)
	for _, objectName := range objectsToDelete {
		wg.Add(1)

		// Acquire semaphore
		sem <- struct{}{}

		go func(objName string) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Delete the object
			obj := bkt.Object(objName)
			err := obj.Delete(ctx)

			// Send result to progress reporter
			deletions <- deletion{objectName: objName, err: err}
		}(objectName)
	}

	// Wait for all deletions to complete
	wg.Wait()
	close(deletions)

	// Wait for progress reporter to finish
	<-done

	if firstErr != nil {
		return fmt.Errorf("deletion failed: %w", firstErr)
	}

	if totalCount > 1 {
		fmt.Printf("\nTotal: %d objects deleted\n", totalCount)
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
