package fuse

import (
	"errors"
	"syscall"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// MapGCPError converts GCP API errors to appropriate syscall.Errno values
// for use in FUSE operations. This provides meaningful error codes to
// filesystem operations.
func MapGCPError(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	// Handle storage-specific errors
	if errors.Is(err, storage.ErrObjectNotExist) {
		return syscall.ENOENT
	}
	if errors.Is(err, storage.ErrBucketNotExist) {
		return syscall.ENOENT
	}

	// Handle Google API errors
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case 403: // Forbidden
			return syscall.EACCES
		case 404: // Not Found
			return syscall.ENOENT
		case 429: // Too Many Requests
			return syscall.EAGAIN
		case 500, 502, 503: // Server errors
			return syscall.EIO
		case 401: // Unauthorized
			return syscall.EACCES
		}
	}

	// Default to generic I/O error
	return syscall.EIO
}
