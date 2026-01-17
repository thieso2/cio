package fuse

import (
	"fmt"
	"os"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// MountOptions contains configuration for mounting the FUSE filesystem
type MountOptions struct {
	ProjectID  string
	Debug      bool
	ReadOnly   bool
	MountOpts  []string // Raw FUSE mount options (e.g., ["allow_other", "default_permissions"])
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
