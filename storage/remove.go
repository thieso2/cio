package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// Default parallelism constants
const (
	// DefaultConcurrentDeletes is the default number of concurrent delete operations
	DefaultConcurrentDeletes = 50
)

// formatSize returns a human-readable byte size string.
func formatSize(bytes int64) string {
	switch {
	case bytes >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(bytes)/(1<<30))
	case bytes >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1<<20))
	case bytes >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(bytes)/(1<<10))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

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

	fmt.Printf("Deleted: %s\n", formatter(fullGCSPath))
	return nil
}

// RemoveDirectory removes all objects with a given prefix.
// Enumeration and deletion run concurrently via a worker pool.
func RemoveDirectory(ctx context.Context, client *storage.Client, bucket, prefix string, verbose bool, formatter PathFormatter, maxWorkers int) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	enumerate := func(ctx context.Context, send func(name string, size int64)) error {
		bkt := client.Bucket(bucket)
		query := &storage.Query{Prefix: prefix}
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
			send(attrs.Name, attrs.Size)
		}
		return nil
	}

	return deleteObjectsStream(ctx, client, bucket, enumerate,
		fmt.Sprintf("no objects found with prefix gs://%s/%s", bucket, prefix),
		formatter, maxWorkers)
}

// RemoveWithPattern removes objects matching a wildcard pattern.
// Enumeration and deletion run concurrently via a worker pool.
func RemoveWithPattern(ctx context.Context, client *storage.Client, bucket, pattern string, verbose bool, formatter PathFormatter, maxWorkers int) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	// Directory wildcard pattern (e.g. "logs/2024-??/"): wildcards appear before
	// the trailing slash, so splitPattern would produce an invalid GCS prefix.
	// Instead we use ListWithPattern to find matching directory prefixes first.
	isDirPattern := strings.HasSuffix(pattern, "/") && strings.ContainsAny(pattern, "*?")

	enumerate := func(ctx context.Context, send func(name string, size int64)) error {
		if isDirPattern {
			matchingDirs, err := ListWithPattern(ctx, bucket, pattern, DefaultListOptions())
			if err != nil {
				return err
			}
			if len(matchingDirs) == 0 {
				return fmt.Errorf("no directories found matching pattern: %s", pattern)
			}
			bkt := client.Bucket(bucket)
			for _, dir := range matchingDirs {
				if !dir.IsPrefix {
					continue
				}
				dirPrefix := strings.TrimPrefix(dir.Path, "gs://"+bucket+"/")
				query := &storage.Query{Prefix: dirPrefix}
				apilog.Logf("[GCS] Objects.List(bucket=%s, prefix=%q) for delete", bucket, dirPrefix)
				it := bkt.Objects(ctx, query)
				for {
					attrs, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return fmt.Errorf("failed to list objects in %s: %w", dirPrefix, err)
					}
					send(attrs.Name, attrs.Size)
				}
			}
		} else {
			prefix, wildcardPattern := splitPattern(pattern)
			bkt := client.Bucket(bucket)
			query := &storage.Query{Prefix: prefix}
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
				if !matchesPattern(attrs.Name, wildcardPattern) {
					continue
				}
				send(attrs.Name, attrs.Size)
			}
		}
		return nil
	}

	return deleteObjectsStream(ctx, client, bucket, enumerate,
		fmt.Sprintf("no objects found matching pattern: %s", pattern),
		formatter, maxWorkers)
}

// workItem carries the object name and its byte size through the delete pipeline.
type workItem struct {
	name string
	size int64
}

// deleteObjectsStream runs enumeration concurrently with a fixed worker pool.
// enumerate calls send() for each object (name + size) to delete;
// deleteObjectsStream manages the worker goroutines, progress reporting, and
// error collection.
// notFoundMsg is returned when enumerate completes with zero objects sent.
func deleteObjectsStream(
	ctx context.Context,
	client *storage.Client,
	bucket string,
	enumerate func(ctx context.Context, send func(name string, size int64)) error,
	notFoundMsg string,
	formatter PathFormatter,
	maxWorkers int,
) error {
	workCh := make(chan workItem, maxWorkers*4)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var enumErr error
	var completedCount int32
	var failedCount int32
	var enumeratedCount int32
	var enumDone int32
	var lastPath string
	var completedBytes int64
	var enumeratedBytes int64

	// Fixed worker pool: workers drain workCh until it is closed.
	bkt := client.Bucket(bucket)
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workCh {
				err := bkt.Object(item.name).Delete(ctx)
				atomic.AddInt32(&completedCount, 1)
				atomic.AddInt64(&completedBytes, item.size)
				mu.Lock()
				lastPath = item.name
				if err != nil {
					atomic.AddInt32(&failedCount, 1)
					if firstErr == nil {
						firstErr = err
					}
				}
				mu.Unlock()
			}
		}()
	}

	// Progress reporter: one \r line updated every 200ms.
	stop := make(chan struct{})
	reporterDone := make(chan struct{})
	printProgress := func(final bool) {
		deleted := atomic.LoadInt32(&completedCount)
		enumed := atomic.LoadInt32(&enumeratedCount)
		failed := atomic.LoadInt32(&failedCount)
		delBytes := atomic.LoadInt64(&completedBytes)
		enumBytes := atomic.LoadInt64(&enumeratedBytes)
		done := atomic.LoadInt32(&enumDone) == 1
		mu.Lock()
		path := lastPath
		mu.Unlock()
		if path == "" {
			return
		}
		fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, path)
		suffix := "\r"
		if final {
			suffix = "\n"
		}
		if done {
			if failed > 0 {
				fmt.Printf("\rDeleted %d/%d (%s/%s, %d failed): %s%s",
					deleted, enumed, formatSize(delBytes), formatSize(enumBytes), failed, formatter(fullGCSPath), suffix)
			} else {
				fmt.Printf("\rDeleted %d/%d (%s/%s): %s%s",
					deleted, enumed, formatSize(delBytes), formatSize(enumBytes), formatter(fullGCSPath), suffix)
			}
		} else {
			if failed > 0 {
				fmt.Printf("\rDeleted %d (%s) found %d (%s), %d failed: %s%s",
					deleted, formatSize(delBytes), enumed, formatSize(enumBytes), failed, formatter(fullGCSPath), suffix)
			} else {
				fmt.Printf("\rDeleted %d (%s) found %d (%s): %s%s",
					deleted, formatSize(delBytes), enumed, formatSize(enumBytes), formatter(fullGCSPath), suffix)
			}
		}
	}
	go func() {
		defer close(reporterDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				printProgress(false)
			case <-stop:
				return
			}
		}
	}()

	// Enumeration goroutine: feeds workCh; closing it signals workers to exit.
	go func() {
		defer close(workCh)
		err := enumerate(ctx, func(name string, size int64) {
			workCh <- workItem{name: name, size: size}
			atomic.AddInt32(&enumeratedCount, 1)
			atomic.AddInt64(&enumeratedBytes, size)
		})
		mu.Lock()
		enumErr = err
		mu.Unlock()
		atomic.StoreInt32(&enumDone, 1)
	}()

	wg.Wait()
	close(stop)
	<-reporterDone

	printProgress(true)

	mu.Lock()
	eerr := enumErr
	derr := firstErr
	mu.Unlock()

	if eerr != nil {
		return eerr
	}
	if atomic.LoadInt32(&enumeratedCount) == 0 {
		return fmt.Errorf("%s", notFoundMsg)
	}
	if derr != nil {
		return fmt.Errorf("deletion failed: %w", derr)
	}

	deleted := atomic.LoadInt32(&completedCount)
	delBytes := atomic.LoadInt64(&completedBytes)
	if deleted > 1 {
		fmt.Printf("Total: %d objects deleted (%s)\n", deleted, formatSize(delBytes))
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
