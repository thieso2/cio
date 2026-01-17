package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// MetadataCacheTTL is how long metadata is cached on disk
	MetadataCacheTTL = 5 * time.Minute
)

// MetadataCache provides persistent disk caching for Google Cloud resource metadata
// Supports GCS (buckets, objects), BigQuery (datasets, tables), and other GCP services
type MetadataCache struct {
	mu       sync.RWMutex
	cacheDir string
	enabled  bool
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

// Get is a generic cache method that works for all Google Cloud resource types
// The cacheKey should uniquely identify the resource (e.g., "gcs:bucket:name", "bq:table:project.dataset.table")
// The generator function is called only on cache miss and should return the metadata as JSON bytes
func (c *MetadataCache) Get(ctx context.Context, cacheKey string, generator func() ([]byte, error)) ([]byte, error) {
	start := time.Now()
	if !c.enabled {
		return generator()
	}

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
	data, err := os.ReadFile(cachePath)
	c.mu.RUnlock()

	if err == nil {
		var cached cachedMetadata
		if json.Unmarshal(data, &cached) == nil {
			// Check if cache is still valid
			if time.Now().Before(cached.ExpiresAt) {
				logGC("CacheHit", start, "key", cacheKey)
				// Re-prettify the JSON data before returning
				var metadata map[string]interface{}
				if json.Unmarshal(cached.Data, &metadata) == nil {
					if prettyData, err := json.MarshalIndent(metadata, "", "  "); err == nil {
						return prettyData, nil
					}
				}
				// Fallback to raw data if prettification fails
				return cached.Data, nil
			} else {
				logGC("CacheExpired", start, "key", cacheKey)
			}
		}
	}

	// Cache miss or expired - generate new metadata
	logGC("CacheMiss", start, "key", cacheKey)
	metadata, err := generator()
	if err != nil {
		return nil, err
	}

	// Save to cache
	c.mu.Lock()
	defer c.mu.Unlock()

	cached := cachedMetadata{
		Data:      metadata,
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(MetadataCacheTTL),
	}

	if cacheData, err := json.Marshal(cached); err == nil {
		os.WriteFile(cachePath, cacheData, 0644)
		logGC("CacheSave", start, "key", cacheKey)
	}

	return metadata, nil
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
