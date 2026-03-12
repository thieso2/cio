package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
	"github.com/thieso2/cio/storage"
)

// handleIndex serves the main HTML UI
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, "base.html", nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleHealth returns server health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, HealthResponse{
		Status:  "ok",
		Version: "1.0.0",
	})
}

// handleAliases returns the list of configured aliases
func (s *Server) handleAliases(w http.ResponseWriter, r *http.Request) {
	aliases := make([]AliasInfo, 0, len(s.cfg.Mappings))

	for name, fullPath := range s.cfg.Mappings {
		aliasType := "gcs"
		if strings.HasPrefix(fullPath, "bq://") {
			aliasType = "bigquery"
		} else if strings.HasPrefix(fullPath, "iam://") {
			aliasType = "iam"
		}

		aliases = append(aliases, AliasInfo{
			Name:     name,
			FullPath: fullPath,
			Type:     aliasType,
		})
	}

	// Render HTML template for HTMX
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, "alias_list", aliases); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleBrowse lists resources at the specified path
func (s *Server) handleBrowse(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, "path parameter is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Resolve alias to full path
	fullPath, err := s.resolver.Resolve(path)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to resolve path: %v", err), http.StatusBadRequest)
		return
	}

	// Create resource handler
	res, err := s.factory.Create(fullPath)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to create resource handler: %v", err), http.StatusInternalServerError)
		return
	}

	// List resources
	resources, err := res.List(ctx, fullPath, &resource.ListOptions{})
	if err != nil {
		writeError(w, fmt.Sprintf("failed to list resources: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert to response format
	items := make([]ResourceItem, 0, len(resources))
	for _, info := range resources {
		aliasPath := s.resolver.ReverseResolve(info.Path)

		item := ResourceItem{
			Name:      info.Name,
			Type:      string(info.Type),
			Size:      info.Size,
			SizeHuman: formatBytes(info.Size),
			Modified:  info.Modified,
			Path:      info.Path,
			AliasPath: aliasPath,
			IsDir:     info.IsDir,
		}

		// Add additional info for BigQuery
		if info.Rows > 0 {
			item.Additional = fmt.Sprintf("%d rows", info.Rows)
		}

		items = append(items, item)
	}

	// Build breadcrumbs
	breadcrumbs := buildBreadcrumbs(path)

	// Determine resource type
	resType := "gcs"
	if strings.HasPrefix(fullPath, "bq://") {
		resType = "bigquery"
	} else if strings.HasPrefix(fullPath, "iam://") {
		resType = "iam"
	}

	response := BrowseResponse{
		Path:        path,
		AliasPath:   path,
		FullPath:    fullPath,
		Type:        resType,
		Resources:   items,
		Breadcrumbs: breadcrumbs,
	}

	// Render HTML template for HTMX
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, "resource_list", response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleInfo returns detailed information about a resource
func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, "path parameter is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Resolve alias to full path
	fullPath, err := s.resolver.Resolve(path)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to resolve path: %v", err), http.StatusBadRequest)
		return
	}

	// Create resource handler
	res, err := s.factory.Create(fullPath)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to create resource handler: %v", err), http.StatusInternalServerError)
		return
	}

	// Get resource info
	info, err := res.Info(ctx, fullPath)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to get resource info: %v", err), http.StatusInternalServerError)
		return
	}

	aliasPath := s.resolver.ReverseResolve(info.Path)

	// Convert metadata to map[string]string if it exists
	metadata := make(map[string]string)
	if info.Metadata != nil {
		if m, ok := info.Metadata.(map[string]string); ok {
			metadata = m
		}
	}

	response := InfoResponse{
		Name:      info.Name,
		Type:      string(info.Type),
		Size:      info.Size,
		SizeHuman: formatBytes(info.Size),
		Modified:  info.Modified,
		Path:      info.Path,
		AliasPath: aliasPath,
		Metadata:  metadata,
	}

	writeJSON(w, response)
}

// handlePreview returns file content for preview (text files only, max 1MB)
func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, "path parameter is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Resolve alias to full path
	fullPath, err := s.resolver.Resolve(path)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to resolve path: %v", err), http.StatusBadRequest)
		return
	}

	// Only support GCS files for now
	if !strings.HasPrefix(fullPath, "gs://") {
		writeError(w, "preview only supported for GCS files", http.StatusBadRequest)
		return
	}

	// Get file info first
	client, err := storage.GetClient(ctx)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to get storage client: %v", err), http.StatusInternalServerError)
		return
	}

	bucket, object, err := resolver.ParseGCSPath(fullPath)
	if err != nil {
		writeError(w, fmt.Sprintf("invalid GCS path: %v", err), http.StatusBadRequest)
		return
	}

	// Get object attributes
	attrs, err := client.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to get file attributes: %v", err), http.StatusInternalServerError)
		return
	}

	// Check size limit (1MB)
	const maxPreviewSize = 1024 * 1024
	if attrs.Size > maxPreviewSize {
		writeJSON(w, PreviewResponse{
			Name:     attrs.Name,
			Size:     attrs.Size,
			TooLarge: true,
			Error:    "File too large to preview (max 1MB)",
		})
		return
	}

	// Read file content
	reader, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to read file: %v", err), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to read file content: %v", err), http.StatusInternalServerError)
		return
	}

	response := PreviewResponse{
		Name:     attrs.Name,
		Content:  string(content),
		MimeType: attrs.ContentType,
		Size:     attrs.Size,
		TooLarge: false,
	}

	writeJSON(w, response)
}

// handleSearch filters resources by pattern
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	path := r.URL.Query().Get("path")

	if path == "" {
		writeError(w, "path parameter is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Resolve alias to full path
	fullPath, err := s.resolver.Resolve(path)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to resolve path: %v", err), http.StatusBadRequest)
		return
	}

	// Create resource handler
	res, err := s.factory.Create(fullPath)
	if err != nil {
		writeError(w, fmt.Sprintf("failed to create resource handler: %v", err), http.StatusInternalServerError)
		return
	}

	// List all resources
	resources, err := res.List(ctx, fullPath, &resource.ListOptions{})
	if err != nil {
		writeError(w, fmt.Sprintf("failed to list resources: %v", err), http.StatusInternalServerError)
		return
	}

	// Filter by query if provided
	filtered := resources
	if query != "" {
		filtered = make([]*resource.ResourceInfo, 0)
		for _, info := range resources {
			if resolver.MatchPattern(info.Name, query) {
				filtered = append(filtered, info)
			}
		}
	}

	// Convert to response format
	items := make([]ResourceItem, 0, len(filtered))
	for _, info := range filtered {
		aliasPath := s.resolver.ReverseResolve(info.Path)

		item := ResourceItem{
			Name:      info.Name,
			Type:      string(info.Type),
			Size:      info.Size,
			SizeHuman: formatBytes(info.Size),
			Modified:  info.Modified,
			Path:      info.Path,
			AliasPath: aliasPath,
			IsDir:     info.IsDir,
		}

		items = append(items, item)
	}

	writeJSON(w, items)
}

// buildBreadcrumbs creates breadcrumb navigation from a path
func buildBreadcrumbs(path string) []Breadcrumb {
	if !strings.HasPrefix(path, ":") {
		return []Breadcrumb{}
	}

	// Remove : prefix
	path = strings.TrimPrefix(path, ":")

	// Split by / for GCS or . for BigQuery
	var parts []string
	if strings.Contains(path, "/") {
		parts = strings.Split(path, "/")
	} else if strings.Contains(path, ".") {
		parts = strings.Split(path, ".")
	} else {
		parts = []string{path}
	}

	breadcrumbs := make([]Breadcrumb, 0, len(parts))
	currentPath := ":"

	for i, part := range parts {
		if part == "" {
			continue
		}

		if i == 0 {
			currentPath += part
		} else {
			if strings.Contains(path, "/") {
				currentPath += "/" + part
			} else {
				currentPath += "." + part
			}
		}

		breadcrumbs = append(breadcrumbs, Breadcrumb{
			Name: part,
			Path: currentPath,
		})
	}

	return breadcrumbs
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// writeError writes an error response
func writeError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   http.StatusText(statusCode),
		Message: message,
	})
}

// formatBytes formats bytes into human-readable format
func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
