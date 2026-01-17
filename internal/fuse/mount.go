package fuse

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gcsLogger is the global logger for GCS API calls (set by Mount)
var gcsLogger *log.Logger

// EnableGCSLogging enables GCS API call logging
func EnableGCSLogging() {
	gcsLogger = log.New(os.Stderr, "[GCS] ", log.LstdFlags|log.Lmicroseconds)
}

// logGCS logs a GCS operation with timing if logging is enabled
func logGCS(operation string, start time.Time, args ...interface{}) {
	if gcsLogger != nil {
		elapsed := time.Since(start)
		gcsLogger.Printf("%s (%.3fms) %v", operation, float64(elapsed.Microseconds())/1000.0, args)
	}
}

// MountOptions contains configuration for mounting the FUSE filesystem
type MountOptions struct {
	ProjectID  string
	Debug      bool
	ReadOnly   bool
	MountOpts  []string // Raw FUSE mount options (e.g., ["allow_other", "default_permissions"])
	LogGCS     bool     // Enable GCS API call logging with timing
	CleanCache bool     // Clear metadata cache on startup
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

	// Enable GCS logging if requested
	if opts.LogGCS {
		EnableGCSLogging()
	}

	// Clean metadata cache if requested
	if opts.CleanCache {
		cache := GetMetadataCache()
		cache.InvalidateAll()
		if opts.LogGCS {
			log.Println("[GCS] Metadata cache cleared on startup")
		}
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
