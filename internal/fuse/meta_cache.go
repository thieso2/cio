package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// MetadataCacheTTL is how long metadata is cached on disk
	MetadataCacheTTL = 5 * time.Minute
)

// MetadataCache provides persistent disk caching for GCS metadata
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

// GetBucketMetadata gets bucket metadata from cache or generates it
func (c *MetadataCache) GetBucketMetadata(ctx context.Context, bucketName string, generator func() ([]byte, error)) ([]byte, error) {
	if !c.enabled {
		return generator()
	}

	cachePath := c.getCachePath(bucketName, "", true)

	// Try to read from cache
	c.mu.RLock()
	data, err := os.ReadFile(cachePath)
	c.mu.RUnlock()

	if err == nil {
		var cached cachedMetadata
		if json.Unmarshal(data, &cached) == nil {
			// Check if cache is still valid
			if time.Now().Before(cached.ExpiresAt) {
				return cached.Data, nil
			}
		}
	}

	// Cache miss or expired - generate new metadata
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

	if cacheData, err := json.MarshalIndent(cached, "", "  "); err == nil {
		os.WriteFile(cachePath, cacheData, 0644)
	}

	return metadata, nil
}

// GetObjectMetadata gets object metadata from cache or generates it
func (c *MetadataCache) GetObjectMetadata(ctx context.Context, bucketName, objectName string, generator func() ([]byte, error)) ([]byte, error) {
	if !c.enabled {
		return generator()
	}

	cachePath := c.getCachePath(bucketName, objectName, false)

	// Try to read from cache
	c.mu.RLock()
	data, err := os.ReadFile(cachePath)
	c.mu.RUnlock()

	if err == nil {
		var cached cachedMetadata
		if json.Unmarshal(data, &cached) == nil {
			// Check if cache is still valid
			if time.Now().Before(cached.ExpiresAt) {
				return cached.Data, nil
			}
		}
	}

	// Cache miss or expired - generate new metadata
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

	if cacheData, err := json.MarshalIndent(cached, "", "  "); err == nil {
		os.WriteFile(cachePath, cacheData, 0644)
	}

	return metadata, nil
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
