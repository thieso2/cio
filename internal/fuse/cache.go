package fuse

import (
	"strings"
	"sync"
	"time"
)

// CacheEntry represents a cached value with expiration time
type CacheEntry struct {
	Data      interface{}
	ExpiresAt time.Time
}

// CacheManager provides thread-safe caching for FUSE operations
// to reduce API calls and improve performance.
type CacheManager struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
	ttl     time.Duration
}

// NewCacheManager creates a new cache manager with the specified TTL
func NewCacheManager(ttl time.Duration) *CacheManager {
	if ttl == 0 {
		ttl = 60 * time.Second // Default 60 seconds
	}
	return &CacheManager{
		entries: make(map[string]*CacheEntry),
		ttl:     ttl,
	}
}

// Get retrieves a value from the cache if it exists and hasn't expired
func (c *CacheManager) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry.Data, true
}

// Set stores a value in the cache with the default TTL
func (c *CacheManager) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL stores a value in the cache with a custom TTL
func (c *CacheManager) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &CacheEntry{
		Data:      value,
		ExpiresAt: time.Now().Add(ttl),
	}
}

// Invalidate removes a specific entry from the cache
func (c *CacheManager) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, key)
}

// InvalidatePrefix removes all entries with keys starting with the given prefix
func (c *CacheManager) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		if strings.HasPrefix(key, prefix) {
			delete(c.entries, key)
		}
	}
}

// Clear removes all entries from the cache
func (c *CacheManager) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*CacheEntry)
}

// CleanExpired removes all expired entries from the cache
// This should be called periodically to prevent memory growth
func (c *CacheManager) CleanExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.entries {
		if now.After(entry.ExpiresAt) {
			delete(c.entries, key)
		}
	}
}

// Size returns the number of entries in the cache
func (c *CacheManager) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}
