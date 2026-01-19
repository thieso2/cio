package fuse

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gcLogger is the global logger for Google Cloud API calls (set by Mount)
var gcLogger *log.Logger

// logCache controls whether cache operations are logged (default: false)
var logCache bool

// logCacheHits controls whether cache hits are logged (default: false)
var logCacheHits bool

// EnableGCLogging enables Google Cloud API call logging (GCS, BigQuery, etc.)
func EnableGCLogging() {
	gcLogger = log.New(os.Stderr, "[GC] ", log.LstdFlags|log.Lmicroseconds)
}

// EnableCacheLogging enables logging of cache operations
func EnableCacheLogging() {
	logCache = true
}

// EnableCacheHitLogging enables logging of cache hits in addition to API calls
func EnableCacheHitLogging() {
	logCacheHits = true
	logCache = true // Cache hits require cache logging to be enabled
}

// getOperationIcon returns an emoji icon for the operation type
func getOperationIcon(operation string) string {
	switch {
	case strings.HasPrefix(operation, "BQ:"):
		return "📊" // BigQuery API call
	case strings.HasPrefix(operation, "GCS:"):
		return "📦" // GCS API call
	case operation == "Lookup":
		return "🔍" // Lookup operation
	case operation == "CacheHit":
		return "⚡" // Cache hit (fast)
	case operation == "CacheMiss":
		return "❓" // Cache miss
	case operation == "CacheSave":
		return "💾" // Cache save
	case operation == "CacheExpired":
		return "⏰" // Cache expired
	case operation == "CacheShared":
		return "🔄" // Cache shared (deduplicated)
	case operation == "BufferHit":
		return "⚡" // Read-ahead buffer hit (fast, like cache hit)
	case operation == "BufferMiss":
		return "❓" // Read-ahead buffer miss (will fetch from GCS)
	case operation == "BufferSave":
		return "💾" // Read-ahead buffer save (data buffered)
	default:
		return "📡" // Generic operation
	}
}

// isCacheOperation checks if an operation is cache-related (including read-ahead buffer)
func isCacheOperation(operation string) bool {
	return operation == "CacheHit" || operation == "CacheMiss" ||
	       operation == "CacheSave" || operation == "CacheExpired" ||
	       operation == "CacheShared" ||
	       operation == "BufferHit" || operation == "BufferMiss" ||
	       operation == "BufferSave"
}

// logGC logs a Google Cloud operation with timing if logging is enabled
func logGC(operation string, start time.Time, args ...interface{}) {
	if gcLogger != nil {
		// Filter cache operations unless --log-cache is enabled
		if isCacheOperation(operation) && !logCache {
			return
		}

		// Skip cache/buffer hit/expired logging unless explicitly enabled (requires --log-cache)
		if !logCacheHits && (operation == "CacheHit" || operation == "CacheExpired" ||
		                      operation == "BufferHit") {
			return
		}

		elapsed := time.Since(start)
		icon := getOperationIcon(operation)
		gcLogger.Printf("%s %s (%.3fms) %v", icon, operation, float64(elapsed.Microseconds())/1000.0, args)
	}
}

// MountOptions contains configuration for mounting the FUSE filesystem
type MountOptions struct {
	ProjectID     string
	Debug         bool
	ReadOnly      bool
	MountOpts     []string // Raw FUSE mount options (e.g., ["allow_other", "default_permissions"])
	LogGC         bool     // Enable Google Cloud API call logging with timing (GCS, BigQuery, etc.)
	LogCache      bool     // Enable logging of cache operations (requires LogGC=true)
	LogCacheHits  bool     // Enable logging of cache hits (requires LogGC=true and LogCache=true)
	CleanCache    bool     // Clear metadata cache on startup
	ReadAheadSize int      // Read-ahead buffer size in bytes (0 = use default 5MB)
}

// Server wraps the FUSE server and provides lifecycle management
type Server struct {
	server     *fuse.Server
	mountpoint string
}

// Mount creates and mounts a new FUSE filesystem at the specified mountpoint
func Mount(mountpoint string, opts MountOptions) (*Server, error) {
	// Validate mountpoint exists
	if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
		return nil, fmt.Errorf("mountpoint does not exist: %s", mountpoint)
	}

	// Enable Google Cloud API logging if requested
	if opts.LogGC {
		EnableGCLogging()
		// Enable cache logging if requested
		if opts.LogCache {
			EnableCacheLogging()
		}
		// Enable cache hit logging if requested (requires cache logging)
		if opts.LogCacheHits {
			EnableCacheHitLogging()
		}
	}

	// Clean metadata cache if requested
	if opts.CleanCache {
		cache := GetMetadataCache()
		cache.InvalidateAll()
		if opts.LogGC {
			log.Println("[GC] Metadata cache cleared on startup")
		}
	}

	// Configure read-ahead buffer size
	if opts.ReadAheadSize > 0 {
		SetReadAheadBufferSize(opts.ReadAheadSize)
		if opts.LogGC {
			log.Printf("[GC] Read-ahead buffer size set to %d bytes (%.1f MB)", opts.ReadAheadSize, float64(opts.ReadAheadSize)/(1024*1024))
		}
	} else if opts.LogGC {
		log.Printf("[GC] Using default read-ahead buffer size: %d bytes (%.1f MB)", DefaultReadAheadSize, float64(DefaultReadAheadSize)/(1024*1024))
	}

	// Create root node
	root := &RootNode{
		projectID: opts.ProjectID,
	}

	// Configure FUSE options
	attrTimeout := 60 * time.Second
	entryTimeout := 60 * time.Second
	fuseOpts := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: fuse.MountOptions{
			Name:          "cio",
			FsName:        "cio-gcp",
			DisableXAttrs: true,
			// macFUSE-specific options for better compatibility
			Options: []string{"local", "volname=CIO-GCP"},
		},
	}

	// Add debug logging if requested
	if opts.Debug {
		fuseOpts.Debug = true
	}

	// Parse and apply mount options
	for _, opt := range opts.MountOpts {
		switch opt {
		case "allow_other":
			fuseOpts.MountOptions.AllowOther = true
		case "default_permissions":
			// Let the kernel do permission checks
			fuseOpts.MountOptions.Options = append(fuseOpts.MountOptions.Options, "default_permissions")
		case "ro", "read_only":
			fuseOpts.MountOptions.Options = append(fuseOpts.MountOptions.Options, "ro")
		default:
			// Pass through unknown options
			fuseOpts.MountOptions.Options = append(fuseOpts.MountOptions.Options, opt)
		}
	}

	// Mount the filesystem
	server, err := fs.Mount(mountpoint, root, fuseOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount filesystem: %w", err)
	}

	return &Server{
		server:     server,
		mountpoint: mountpoint,
	}, nil
}

// Wait blocks until the filesystem is unmounted
func (s *Server) Wait() {
	s.server.Wait()
}

// Unmount unmounts the filesystem
func (s *Server) Unmount() error {
	return s.server.Unmount()
}
