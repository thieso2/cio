package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// MetadataCacheTTL is how long metadata is cached on disk
	// Aggressive caching: metadata rarely changes, so cache for 24 hours
	// Use --clean-cache flag to force refresh if needed
	MetadataCacheTTL = 24 * time.Hour

	// RowCountCacheTTL is specifically for table row counts which may change more frequently
	RowCountCacheTTL = 1 * time.Hour

	// ListCacheTTL is for list operations (buckets, tables, objects) which change more often
	ListCacheTTL = 30 * time.Minute

	// NegativeCacheTTL is for caching "not found" errors to avoid repeated API calls
	NegativeCacheTTL = 5 * time.Minute

	// Special marker to indicate a "not found" error is cached (must be valid JSON)
	notFoundMarker = `{"error": "not_found"}`
)

// isDotFile checks if the cache key represents a dot file (like .DS_Store, .m, .me, etc.)
func isDotFile(cacheKey string) bool {
	// Check for common dot file patterns in cache keys
	// Examples: "gcs:object:bucket/.DS_Store", "bq:table:rows:project.dataset..meta"
	parts := strings.Split(cacheKey, "/")
	for _, part := range parts {
		if len(part) > 0 && part[0] == '.' && part != ".meta" {
			return true
		}
	}
	// Also check path components separated by colons (for BQ paths)
	parts = strings.Split(cacheKey, ":")
	for _, part := range parts {
		if len(part) > 0 && part[0] == '.' && part != ".meta" {
			return true
		}
	}
	return false
}

// MetadataCache provides persistent disk caching for Google Cloud resource metadata
// Supports GCS (buckets, objects), BigQuery (datasets, tables), and other GCP services
type MetadataCache struct {
	mu       sync.RWMutex
	cacheDir string
	enabled  bool
	flight   singleflight.Group // Deduplicates concurrent requests for the same key
}

// cachedMetadata wraps metadata with cache timestamp
type cachedMetadata struct {
	Data      json.RawMessage `json:"data"`
	CachedAt  time.Time       `json:"cached_at"`
	ExpiresAt time.Time       `json:"expires_at"`
}

var (
	globalMetaCache *MetadataCache
	metaCacheMu     sync.Once
)

// GetMetadataCache returns the global metadata cache instance
func GetMetadataCache() *MetadataCache {
	metaCacheMu.Do(func() {
		cacheDir := filepath.Join(os.TempDir(), "cio-meta-cache")
		globalMetaCache = &MetadataCache{
			cacheDir: cacheDir,
			enabled:  true,
		}
		// Create cache directory
		os.MkdirAll(cacheDir, 0755)
	})
	return globalMetaCache
}

// getCachePath returns the cache file path for a given key
func (c *MetadataCache) getCachePath(bucketName, objectName string, isBucket bool) string {
	if isBucket {
		return filepath.Join(c.cacheDir, fmt.Sprintf("bucket_%s.json", bucketName))
	}
	// Use safe filename
	safeName := filepath.Base(objectName)
	if len(safeName) > 200 {
		safeName = safeName[:200]
	}
	return filepath.Join(c.cacheDir, fmt.Sprintf("object_%s_%s.json", bucketName, safeName))
}

// getBQCachePath returns the cache file path for BigQuery resources
func (c *MetadataCache) getBQCachePath(projectID, datasetID, tableID string) string {
	if tableID != "" {
		return filepath.Join(c.cacheDir, fmt.Sprintf("bq_table_%s_%s_%s.json", projectID, datasetID, tableID))
	}
	if datasetID != "" {
		return filepath.Join(c.cacheDir, fmt.Sprintf("bq_dataset_%s_%s.json", projectID, datasetID))
	}
	return filepath.Join(c.cacheDir, fmt.Sprintf("bq_project_%s.json", projectID))
}

// GetWithTTL is a generic cache method that works for all Google Cloud resource types
// The cacheKey should uniquely identify the resource (e.g., "gcs:bucket:name", "bq:table:project.dataset.table")
// The generator function is called only on cache miss and should return the metadata as JSON bytes
// The ttl parameter specifies how long to cache the data
// Uses singleflight to deduplicate concurrent requests for the same key
func (c *MetadataCache) GetWithTTL(ctx context.Context, cacheKey string, ttl time.Duration, generator func() ([]byte, error)) ([]byte, error) {
	start := time.Now()
	if !c.enabled {
		return generator()
	}

	// Use singleflight to deduplicate concurrent requests
	// Only one goroutine will execute the function for a given key, others wait for the result
	result, err, shared := c.flight.Do(cacheKey, func() (interface{}, error) {
		// Generate cache file path from key
		// Replace colons, slashes, and dots with underscores for safe filenames
		safeKey := cacheKey
		safeKey = strings.ReplaceAll(safeKey, ":", "_")
		safeKey = strings.ReplaceAll(safeKey, "/", "_")
		safeKey = strings.ReplaceAll(safeKey, ".", "_")
		if len(safeKey) > 200 {
			safeKey = safeKey[:200]
		}
		cachePath := filepath.Join(c.cacheDir, fmt.Sprintf("%s.json", safeKey))

		// Try to read from cache
		c.mu.RLock()
		data, readErr := os.ReadFile(cachePath)
		c.mu.RUnlock()

		if readErr == nil {
			var cached cachedMetadata
			if json.Unmarshal(data, &cached) == nil {
				// Check if cache is still valid
				if time.Now().Before(cached.ExpiresAt) {
					// Check if this is a cached "not found" error
					if string(cached.Data) == notFoundMarker {
						// Skip logging for dot files (like .DS_Store, .m, .me, etc.)
						if !isDotFile(cacheKey) {
							logGC("CacheHit", start, "key", cacheKey, "notFound", true)
						}
						return nil, syscall.ENOENT
					}

					// Skip logging for dot files
					if !isDotFile(cacheKey) {
						logGC("CacheHit", start, "key", cacheKey)
					}
					// Re-prettify the JSON data before returning
					var metadata map[string]interface{}
					if json.Unmarshal(cached.Data, &metadata) == nil {
						if prettyData, marshalErr := json.MarshalIndent(metadata, "", "  "); marshalErr == nil {
							return prettyData, nil
						}
					}
					// Fallback to raw data if prettification fails
					// Convert json.RawMessage to []byte for type compatibility
					return []byte(cached.Data), nil
				} else {
					// Skip logging for dot files
					if !isDotFile(cacheKey) {
						logGC("CacheExpired", start, "key", cacheKey)
					}
				}
			}
		}

		// Cache miss or expired - generate new metadata
		// Skip logging for dot files
		if !isDotFile(cacheKey) {
			logGC("CacheMiss", start, "key", cacheKey)
		}
		metadata, genErr := generator()
		if genErr != nil {
			// Check if this is a "not found" error (404)
			errStr := genErr.Error()
			is404 := strings.Contains(errStr, "404") || strings.Contains(errStr, "notFound") || strings.Contains(errStr, "Not found")

			if is404 {
				// Cache the "not found" error with shorter TTL
				c.mu.Lock()
				cached := cachedMetadata{
					Data:      []byte(notFoundMarker),
					CachedAt:  time.Now(),
					ExpiresAt: time.Now().Add(NegativeCacheTTL),
				}
				if cacheData, marshalErr := json.Marshal(cached); marshalErr == nil {
					os.WriteFile(cachePath, cacheData, 0644)
					// Skip logging for dot files
					if !isDotFile(cacheKey) {
						logGC("CacheSave", start, "key", cacheKey, "notFound", true, "ttl", NegativeCacheTTL)
					}
				}
				c.mu.Unlock()
			}
			return nil, genErr
		}

		// Save to cache
		c.mu.Lock()
		cached := cachedMetadata{
			Data:      metadata,
			CachedAt:  time.Now(),
			ExpiresAt: time.Now().Add(ttl),
		}

		if cacheData, marshalErr := json.Marshal(cached); marshalErr == nil {
			os.WriteFile(cachePath, cacheData, 0644)
			// Skip logging for dot files
			if !isDotFile(cacheKey) {
				logGC("CacheSave", start, "key", cacheKey, "ttl", ttl)
			}
		}
		c.mu.Unlock()

		return metadata, nil
	})

	// Log if this request was deduplicated (shared result from another goroutine)
	// Skip logging for dot files
	if shared && !isDotFile(cacheKey) {
		logGC("CacheShared", start, "key", cacheKey)
	}

	if err != nil {
		return nil, err
	}

	// Type assert the result back to []byte
	if result == nil {
		return nil, nil
	}
	return result.([]byte), nil
}

// Get is a convenience wrapper around GetWithTTL that uses the default MetadataCacheTTL
func (c *MetadataCache) Get(ctx context.Context, cacheKey string, generator func() ([]byte, error)) ([]byte, error) {
	return c.GetWithTTL(ctx, cacheKey, MetadataCacheTTL, generator)
}

// GetBucketMetadata gets bucket metadata from cache or generates it
// Uses the generic Get() method with a bucket-specific cache key
func (c *MetadataCache) GetBucketMetadata(ctx context.Context, bucketName string, generator func() ([]byte, error)) ([]byte, error) {
	cacheKey := fmt.Sprintf("gcs:bucket:%s", bucketName)
	return c.Get(ctx, cacheKey, generator)
}

// GetObjectMetadata gets object metadata from cache or generates it
// Uses the generic Get() method with an object-specific cache key
func (c *MetadataCache) GetObjectMetadata(ctx context.Context, bucketName, objectName string, generator func() ([]byte, error)) ([]byte, error) {
	cacheKey := fmt.Sprintf("gcs:object:%s/%s", bucketName, objectName)
	return c.Get(ctx, cacheKey, generator)
}

// InvalidateBucket invalidates all cached metadata for a bucket
func (c *MetadataCache) InvalidateBucket(bucketName string) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove bucket metadata cache
	bucketPath := c.getCachePath(bucketName, "", true)
	os.Remove(bucketPath)

	// Remove all object metadata for this bucket
	pattern := filepath.Join(c.cacheDir, fmt.Sprintf("object_%s_*.json", bucketName))
	matches, err := filepath.Glob(pattern)
	if err == nil {
		for _, match := range matches {
			os.Remove(match)
		}
	}
}

// InvalidateAll clears all cached metadata
func (c *MetadataCache) InvalidateAll() {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove all cache files
	os.RemoveAll(c.cacheDir)
	os.MkdirAll(c.cacheDir, 0755)
}

// GetTableMetadata gets BigQuery table metadata from cache or generates it
// Uses the generic Get() method with a table-specific cache key
func (c *MetadataCache) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string, generator func() ([]byte, error)) ([]byte, error) {
	cacheKey := fmt.Sprintf("bq:table:%s.%s.%s", projectID, datasetID, tableID)
	return c.Get(ctx, cacheKey, generator)
}

// InvalidateDataset invalidates all cached metadata for a BigQuery dataset
func (c *MetadataCache) InvalidateDataset(projectID, datasetID string) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove dataset metadata cache
	datasetPath := c.getBQCachePath(projectID, datasetID, "")
	os.Remove(datasetPath)

	// Remove all table metadata for this dataset
	pattern := filepath.Join(c.cacheDir, fmt.Sprintf("bq_table_%s_%s_*.json", projectID, datasetID))
	matches, err := filepath.Glob(pattern)
	if err == nil {
		for _, match := range matches {
			os.Remove(match)
		}
	}
}
