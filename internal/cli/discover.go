package cli

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/thieso2/cio/resource"
)

// Discover mode lets a single command fan out across every project matching a
// pattern, using the single-slash syntax `scheme:/project-pattern/rest`. The
// project-iteration skeleton (list projects → for each, build a resource path →
// do the per-command work) used to be copy-pasted into ls/rm/stop/cancel. This
// file is that skeleton's one home: the helpers below, plus the two traversal
// seams discoverProjects and forEachDiscoveredProject.

// parseDiscoverPath checks if path uses discover syntax: scheme:/project-pattern/rest
// Single slash after scheme = discover mode (multi-project).
// Double slash (scheme://) = current project (normal mode).
//
// Examples:
//
//	jobs:/iom-*/sqlmesh*  → discover across projects matching iom-*
//	jobs://sqlmesh*       → current project only
//
// Returns projectPattern, scheme, rest, ok
func parseDiscoverPath(path string) (string, string, string, bool) {
	// Find scheme:/ but NOT scheme://
	idx := strings.Index(path, ":/")
	if idx < 0 {
		return "", "", "", false
	}
	// If followed by another /, it's scheme:// (normal mode)
	if idx+2 < len(path) && path[idx+2] == '/' {
		return "", "", "", false
	}

	scheme := path[:idx]
	after := path[idx+2:] // everything after :/

	// Split into project pattern and rest
	slashIdx := strings.Index(after, "/")
	var projectPattern, rest string
	if slashIdx >= 0 {
		projectPattern = after[:slashIdx]
		rest = after[slashIdx+1:]
	} else {
		projectPattern = after
		rest = ""
	}

	if projectPattern == "" {
		return "", "", "", false
	}

	return projectPattern, scheme, rest, true
}

// buildDiscoverResourcePath constructs a resource path from scheme and rest for discover mode.
// Handles scheme-specific path formats:
//   - bq: bq://project-id[.rest]
//   - iam: iam://project-id[/rest]
//   - vm: vm://[zone/name] — rest without "/" gets prefixed with "*/" for all-zones
//   - others: scheme://[rest]
func buildDiscoverResourcePath(scheme, projectID, rest string) string {
	switch scheme {
	case "bq":
		p := "bq://" + projectID
		if rest != "" {
			p += "." + rest
		}
		return p
	case "iam":
		p := "iam://" + projectID
		if rest != "" {
			p += "/" + rest
		}
		return p
	default:
		p := scheme + "://"
		if rest != "" {
			// VM paths need zone/name format; if rest has no slash,
			// treat it as an instance name pattern across all zones.
			if scheme == "vm" && !strings.Contains(rest, "/") {
				p += "*/" + rest
			} else {
				p += rest
			}
		}
		return p
	}
}

// prefixResourceName prefixes resource names with a CLI-usable discover-mode path.
// Output format: scheme:/project/name (e.g., vm:/iom-dev-dirk/iomp-ingress-t3rt).
func prefixResourceName(info *resource.ResourceInfo, scheme, projectID string) {
	prefix := scheme + ":/" + projectID + "/"
	info.Name = prefix + info.Name
	// Update metadata Name/Email fields so FormatLong uses the prefixed name
	if info.Metadata != nil {
		prefixMetadataName(info.Metadata, prefix)
	}
}

// prefixMetadataName uses reflection to prefix the Name (or Email) field on any metadata struct.
func prefixMetadataName(metadata interface{}, prefix string) {
	v := reflect.ValueOf(metadata)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	// Try Name field first, then Email (for IAM)
	for _, fieldName := range []string{"Name", "Email"} {
		f := v.FieldByName(fieldName)
		if f.IsValid() && f.Kind() == reflect.String && f.CanSet() {
			f.SetString(prefix + f.String())
			return
		}
	}
}

// discoverProjects returns the sorted project IDs matching projectPattern. It is
// the deep core of discover mode: it centralises the empty-result notice and the
// verbose discovery log so the commands don't each repeat them. When nothing
// matches it prints the notice and returns an empty slice.
func discoverProjects(ctx context.Context, projectPattern string) ([]string, error) {
	projectIDs, err := resource.ListProjectIDs(ctx, projectPattern)
	if err != nil {
		return nil, err
	}
	if len(projectIDs) == 0 {
		fmt.Fprintf(os.Stderr, "No projects matching %s\n", projectPattern)
		return nil, nil
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "Discover: %d project(s) matching %s\n", len(projectIDs), projectPattern)
	}
	return projectIDs, nil
}

// forEachDiscoveredProject lists the projects matching projectPattern and invokes
// fn once per project (in sorted order), passing the project id and the resource
// path built for (scheme, projectID, rest). The per-command work is the only
// thing that crosses this seam. A non-nil error from fn stops the traversal.
func forEachDiscoveredProject(ctx context.Context, scheme, projectPattern, rest string, fn func(projectID, resourcePath string) error) error {
	projectIDs, err := discoverProjects(ctx, projectPattern)
	if err != nil {
		return err
	}
	for _, projectID := range projectIDs {
		resourcePath := buildDiscoverResourcePath(scheme, projectID, rest)
		if err := fn(projectID, resourcePath); err != nil {
			return err
		}
	}
	return nil
}
