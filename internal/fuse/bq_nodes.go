package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"time"

	cloud_bigquery "cloud.google.com/go/bigquery"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/thieso2/cio/internal/bigquery"
)

// listBQDatasets lists all datasets in a project and returns a DirStream
func listBQDatasets(ctx context.Context, projectID string) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	datasets, err := bigquery.ListDatasets(ctx, projectID)
	if err != nil {
		return nil, MapGCPError(err)
	}

	entries := make([]fuse.DirEntry, 0, len(datasets))
	for _, dataset := range datasets {
		// Extract dataset ID from path (bq://project.dataset)
		_, datasetID, _, err := bigquery.ParseBQPath(dataset.Path)
		if err != nil {
			continue
		}

		entries = append(entries, fuse.DirEntry{
			Name: datasetID,
			Mode: fuse.S_IFDIR,
		})
	}

	logGC("BQ:ListDatasets", start, projectID, len(entries), "datasets")
	return fs.NewListDirStream(entries), 0
}

// DatasetNode represents a BigQuery dataset directory (e.g., /mnt/gcp/bigquery/my-dataset/)
type DatasetNode struct {
	fs.Inode
	projectID string
	datasetID string
}

var _ fs.NodeReaddirer = (*DatasetNode)(nil)
var _ fs.NodeGetattrer = (*DatasetNode)(nil)
var _ fs.NodeLookuper = (*DatasetNode)(nil)

// Readdir lists all tables in the dataset
func (n *DatasetNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	start := time.Now()
	tables, err := bigquery.ListTables(ctx, n.projectID, n.datasetID)
	if err != nil {
		return nil, MapGCPError(err)
	}

	entries := []fuse.DirEntry{
		{Name: ".meta", Mode: fuse.S_IFDIR}, // Metadata directory
	}

	for _, table := range tables {
		// Extract table ID from path (bq://project.dataset.table)
		_, _, tableID, err := bigquery.ParseBQPath(table.Path)
		if err != nil {
			continue
		}

		entries = append(entries, fuse.DirEntry{
			Name: tableID,
			Mode: fuse.S_IFDIR, // Tables are directories (can be queried, have schema, etc.)
		})
	}

	logGC("BQ:ListTables", start, n.datasetID, len(entries)-1, "tables") // -1 for .meta dir
	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the dataset directory
func (n *DatasetNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a table or .meta directory by name
func (n *DatasetNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Special case: .meta directory
	if name == ".meta" {
		metaNode := &BQMetaDirectoryNode{
			projectID: n.projectID,
			datasetID: n.datasetID,
		}
		child := n.NewInode(ctx, metaNode, fs.StableAttr{
			Mode: fuse.S_IFDIR,
		})
		return child, 0
	}

	// Otherwise, it's a table
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR, // Tables are directories
	}
	child := n.NewInode(ctx, &TableNode{
		projectID: n.projectID,
		datasetID: n.datasetID,
		tableID:   name,
	}, stable)
	return child, 0
}

// TableNode represents a BigQuery table directory (e.g., /mnt/gcp/bigquery/dataset/table/)
type TableNode struct {
	fs.Inode
	projectID string
	datasetID string
	tableID   string
}

var _ fs.NodeReaddirer = (*TableNode)(nil)
var _ fs.NodeGetattrer = (*TableNode)(nil)
var _ fs.NodeLookuper = (*TableNode)(nil)

// Readdir lists virtual files in the table directory
func (n *TableNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "schema.json", Mode: fuse.S_IFREG}, // Table schema as JSON
		{Name: "metadata.json", Mode: fuse.S_IFREG}, // Table metadata
	}
	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the table directory
func (n *TableNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds virtual files in the table directory
func (n *TableNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name != "schema.json" && name != "metadata.json" {
		return nil, syscall.ENOENT
	}

	stable := fs.StableAttr{
		Mode: fuse.S_IFREG,
	}
	child := n.NewInode(ctx, &TableMetaFileNode{
		projectID: n.projectID,
		datasetID: n.datasetID,
		tableID:   n.tableID,
		fileName:  name,
	}, stable)
	return child, 0
}

// TableMetaFileNode represents a virtual metadata file for a table
type TableMetaFileNode struct {
	fs.Inode
	projectID string
	datasetID string
	tableID   string
	fileName  string
}

var _ fs.NodeOpener = (*TableMetaFileNode)(nil)
var _ fs.NodeGetattrer = (*TableMetaFileNode)(nil)
var _ fs.NodeReader = (*TableMetaFileNode)(nil)

// Open opens the virtual file for reading
func (n *TableMetaFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Read-only
	if flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Getattr returns attributes for the virtual file
func (n *TableMetaFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444 | fuse.S_IFREG // Read-only file
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 1
	out.Size = 8192 // Approximate size
	return 0
}

// Read returns the content of the virtual file
func (n *TableMetaFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	start := time.Now()

	// Get metadata cache
	cache := GetMetadataCache()

	// Use cache for table metadata
	metadata, err := cache.GetTableMetadata(ctx, n.projectID, n.datasetID, n.tableID, func() ([]byte, error) {
		// Generator function - called only on cache miss
		info, err := bigquery.DescribeTable(ctx, n.projectID, n.datasetID, n.tableID)
		if err != nil {
			logGC("BQ:DescribeTable", start, n.datasetID, n.tableID, "ERROR", err)
			return nil, err
		}

		var content string
		if n.fileName == "schema.json" {
			// Return just the schema in JSON format
			content = formatSchemaAsJSON(info)
		} else {
			// Return full metadata
			content = info.FormatDetailed(fmt.Sprintf("%s.%s.%s", n.projectID, n.datasetID, n.tableID))
		}

		return []byte(content), nil
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Handle offset and length
	if off >= int64(len(metadata)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(metadata)) {
		end = int64(len(metadata))
	}

	logGC("BQ:ReadTableMetadata", start, n.datasetID, n.tableID, n.fileName, "offset", off, "bytes", end-off)
	return fuse.ReadResultData(metadata[off:end]), 0
}

// formatSchemaAsJSON formats a BigQuery schema as JSON
func formatSchemaAsJSON(info *bigquery.BQObjectInfo) string {
	// Convert schema to JSON-serializable format
	type Field struct {
		Name        string   `json:"name"`
		Type        string   `json:"type"`
		Mode        string   `json:"mode,omitempty"`
		Description string   `json:"description,omitempty"`
		Fields      []Field  `json:"fields,omitempty"` // For nested RECORD types
	}

	var convertField func(*cloud_bigquery.FieldSchema) Field
	convertField = func(f *cloud_bigquery.FieldSchema) Field {
		field := Field{
			Name:        f.Name,
			Type:        string(f.Type),
			Description: f.Description,
		}

		// Set mode
		if f.Required {
			field.Mode = "REQUIRED"
		} else if f.Repeated {
			field.Mode = "REPEATED"
		} else {
			field.Mode = "NULLABLE"
		}

		// Convert nested fields for RECORD types
		if len(f.Schema) > 0 {
			field.Fields = make([]Field, len(f.Schema))
			for i, nestedField := range f.Schema {
				field.Fields[i] = convertField(nestedField)
			}
		}

		return field
	}

	// Convert all fields
	fields := make([]Field, len(info.Schema))
	for i, f := range info.Schema {
		fields[i] = convertField(f)
	}

	// Create schema object
	schema := map[string]interface{}{
		"table":       info.Path,
		"description": info.Description,
		"fields":      fields,
	}

	// Marshal to pretty JSON
	jsonBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return "{}"
	}

	return string(jsonBytes)
}

// BQMetaDirectoryNode represents the .meta/ directory in a dataset
type BQMetaDirectoryNode struct {
	fs.Inode
	projectID string
	datasetID string
}

var _ fs.NodeReaddirer = (*BQMetaDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*BQMetaDirectoryNode)(nil)
var _ fs.NodeLookuper = (*BQMetaDirectoryNode)(nil)

// Readdir lists metadata files for all tables
func (n *BQMetaDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	tables, err := bigquery.ListTables(ctx, n.projectID, n.datasetID)
	if err != nil {
		return nil, MapGCPError(err)
	}

	entries := []fuse.DirEntry{
		{Name: "_dataset.json", Mode: fuse.S_IFREG}, // Dataset metadata
	}

	for _, table := range tables {
		_, _, tableID, err := bigquery.ParseBQPath(table.Path)
		if err != nil {
			continue
		}
		entries = append(entries, fuse.DirEntry{
			Name: tableID + ".json",
			Mode: fuse.S_IFREG,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Getattr returns attributes for the .meta directory
func (n *BQMetaDirectoryNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2
	return 0
}

// Lookup finds a metadata file
func (n *BQMetaDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// For now, return ENOENT for metadata files
	// This can be implemented later if needed
	return nil, syscall.ENOENT
}
