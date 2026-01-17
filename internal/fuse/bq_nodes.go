package fuse

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

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
	// Get table metadata
	info, err := bigquery.DescribeTable(ctx, n.projectID, n.datasetID, n.tableID)
	if err != nil {
		logGC("BQ:DescribeTable", start, n.datasetID, n.tableID, "ERROR", err)
		return nil, MapGCPError(err)
	}

	var content string
	if n.fileName == "schema.json" {
		// Return just the schema in JSON format
		content = formatSchemaAsJSON(info)
	} else {
		// Return full metadata
		content = info.FormatDetailed(fmt.Sprintf("%s.%s.%s", n.projectID, n.datasetID, n.tableID))
	}

	// Handle offset and length
	data := []byte(content)
	if off >= int64(len(data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	logGC("BQ:ReadTableMetadata", start, n.datasetID, n.tableID, n.fileName, "offset", off, "bytes", end-off)
	return fuse.ReadResultData(data[off:end]), 0
}

// formatSchemaAsJSON formats a BigQuery schema as JSON
func formatSchemaAsJSON(info *bigquery.BQObjectInfo) string {
	// Simple JSON representation
	return fmt.Sprintf(`{
  "table": "%s",
  "type": "%s",
  "size_bytes": %d,
  "num_rows": %d,
  "created": "%s",
  "modified": "%s",
  "location": "%s",
  "description": "%s"
}`, info.Path, info.Type, info.SizeBytes, info.NumRows,
		info.Created.Format("2006-01-02T15:04:05Z"),
		info.Modified.Format("2006-01-02T15:04:05Z"),
		info.Location, info.Description)
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
