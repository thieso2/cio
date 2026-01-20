package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	cloud_bigquery "cloud.google.com/go/bigquery"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/thieso2/cio/bigquery"
)

// listBQDatasets lists all datasets in a project and returns a DirStream
func listBQDatasets(ctx context.Context, projectID string) (fs.DirStream, syscall.Errno) {
	// Cache dataset list for 30 minutes
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("bq:datasets:%s", projectID)

	datasetData, err := cache.GetWithTTL(ctx, cacheKey, ListCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss or expiry
		apiStart := time.Now()
		datasets, err := bigquery.ListDatasets(ctx, projectID)
		if err != nil {
			logGC("BQ:ListDatasets", apiStart, projectID, "ERROR", err)
			return nil, err
		}

		// Serialize dataset IDs as JSON
		datasetIDs := make([]string, 0, len(datasets))
		for _, dataset := range datasets {
			_, datasetID, _, err := bigquery.ParseBQPath(dataset.Path)
			if err != nil {
				continue
			}
			datasetIDs = append(datasetIDs, datasetID)
		}

		// Log successful API call
		logGC("BQ:ListDatasets", apiStart, projectID, len(datasetIDs), "datasets")
		return json.Marshal(datasetIDs)
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Deserialize cached dataset IDs
	var datasetIDs []string
	if err := json.Unmarshal(datasetData, &datasetIDs); err != nil {
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(datasetIDs))
	for _, datasetID := range datasetIDs {
		entries = append(entries, fuse.DirEntry{
			Name: datasetID,
			Mode: fuse.S_IFDIR,
		})
	}

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
	// Cache table list for 30 minutes
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("bq:tables:%s.%s", n.projectID, n.datasetID)

	tableData, err := cache.GetWithTTL(ctx, cacheKey, ListCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss or expiry
		apiStart := time.Now()
		tables, err := bigquery.ListTables(ctx, n.projectID, n.datasetID)
		if err != nil {
			logGC("BQ:ListTables", apiStart, n.datasetID, "ERROR", err)
			return nil, err
		}

		// Serialize table IDs as JSON
		tableIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			_, _, tableID, err := bigquery.ParseBQPath(table.Path)
			if err != nil {
				continue
			}
			tableIDs = append(tableIDs, tableID)
		}

		// Log successful API call
		logGC("BQ:ListTables", apiStart, n.datasetID, len(tableIDs), "tables")
		return json.Marshal(tableIDs)
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Deserialize cached table IDs
	var tableIDs []string
	if err := json.Unmarshal(tableData, &tableIDs); err != nil {
		return nil, syscall.EIO
	}

	entries := []fuse.DirEntry{
		{Name: ".meta", Mode: fuse.S_IFDIR}, // Metadata directory
	}

	for _, tableID := range tableIDs {
		entries = append(entries, fuse.DirEntry{
			Name: tableID,
			Mode: fuse.S_IFDIR, // Tables are directories (can be queried, have schema, etc.)
		})
	}

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

	// Ignore all other files starting with "." (like .DS_Store, .m, .me, etc.)
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
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
	// Get metadata cache
	cache := GetMetadataCache()

	// Cache row count separately for quick access in Getattr
	// Use RowCountCacheTTL (1 hour) since row counts may change more frequently than schemas
	cacheKey := fmt.Sprintf("bq:table:rows:%s.%s.%s", n.projectID, n.datasetID, n.tableID)
	rowCountData, err := cache.GetWithTTL(ctx, cacheKey, RowCountCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss or expiry
		// This is where we log the actual API call
		apiStart := time.Now()
		info, err := bigquery.DescribeTable(ctx, n.projectID, n.datasetID, n.tableID)
		if err != nil {
			logGC("BQ:GetTableAttr", apiStart, n.datasetID, n.tableID, "ERROR", err)
			return nil, err
		}

		// Log successful API call
		rowCount := info.NumRows
		blocks := (rowCount + 511) / 512
		logGC("BQ:GetTableAttr", apiStart, n.datasetID, n.tableID, "rows", rowCount, "blocks", blocks)

		// Store just the row count as a string
		return []byte(fmt.Sprintf("%d", info.NumRows)), nil
	})

	if err != nil {
		// If metadata fetch fails, still return directory attributes
		out.Size = 0
		out.Blocks = 0
	} else {
		// Parse row count from cached data
		var rowCount uint64
		fmt.Sscanf(string(rowCountData), "%d", &rowCount)

		out.Size = rowCount
		out.Blocks = (rowCount + 511) / 512 // Round up
	}

	out.Mode = 0755 // Directory permissions
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 2

	return 0
}

// Lookup finds virtual files in the table directory
func (n *TableNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Ignore files starting with "." (like .DS_Store)
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	// Only allow schema.json and metadata.json
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
	// Get metadata cache
	cache := GetMetadataCache()

	// Fetch metadata to get the actual size
	metadata, err := cache.GetTableMetadata(ctx, n.projectID, n.datasetID, n.tableID, func() ([]byte, error) {
		// Generator function - called only on cache miss
		apiStart := time.Now()
		info, err := bigquery.DescribeTable(ctx, n.projectID, n.datasetID, n.tableID)
		if err != nil {
			logGC("BQ:DescribeTable", apiStart, n.datasetID, n.tableID, "ERROR", err)
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

		// Log successful API call
		logGC("BQ:DescribeTable", apiStart, n.datasetID, n.tableID, "size", len(content))
		return []byte(content), nil
	})

	if err != nil {
		// If metadata fetch fails, use approximate size
		out.Size = 8192
	} else {
		// Set actual size from cached metadata
		out.Size = uint64(len(metadata))
	}

	out.Mode = 0444 | fuse.S_IFREG // Read-only file
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 1

	return 0
}

// Read returns the content of the virtual file
func (n *TableMetaFileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Get metadata cache
	cache := GetMetadataCache()

	// Use cache for table metadata
	metadata, err := cache.GetTableMetadata(ctx, n.projectID, n.datasetID, n.tableID, func() ([]byte, error) {
		// Generator function - called only on cache miss
		apiStart := time.Now()
		info, err := bigquery.DescribeTable(ctx, n.projectID, n.datasetID, n.tableID)
		if err != nil {
			logGC("BQ:DescribeTable", apiStart, n.datasetID, n.tableID, "ERROR", err)
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

		// Log successful API call
		logGC("BQ:DescribeTable", apiStart, n.datasetID, n.tableID, "size", len(content))
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
	// Cache table list for 30 minutes (same cache as parent DatasetNode)
	cache := GetMetadataCache()
	cacheKey := fmt.Sprintf("bq:tables:%s.%s", n.projectID, n.datasetID)

	tableData, err := cache.GetWithTTL(ctx, cacheKey, ListCacheTTL, func() ([]byte, error) {
		// Generator function - called only on cache miss or expiry
		apiStart := time.Now()
		tables, err := bigquery.ListTables(ctx, n.projectID, n.datasetID)
		if err != nil {
			logGC("BQ:ListTables", apiStart, n.datasetID, ".meta", "ERROR", err)
			return nil, err
		}

		// Serialize table IDs as JSON
		tableIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			_, _, tableID, err := bigquery.ParseBQPath(table.Path)
			if err != nil {
				continue
			}
			tableIDs = append(tableIDs, tableID)
		}

		// Log successful API call
		logGC("BQ:ListTables", apiStart, n.datasetID, ".meta", len(tableIDs), "files")
		return json.Marshal(tableIDs)
	})

	if err != nil {
		return nil, MapGCPError(err)
	}

	// Deserialize cached table IDs
	var tableIDs []string
	if err := json.Unmarshal(tableData, &tableIDs); err != nil {
		return nil, syscall.EIO
	}

	entries := []fuse.DirEntry{
		{Name: "metadata.json", Mode: fuse.S_IFREG}, // Dataset metadata
		{Name: "iam-policy", Mode: fuse.S_IFDIR},    // IAM policy directory
	}

	for _, tableID := range tableIDs {
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
	// Ignore files starting with "." (like .DS_Store)
	if len(name) > 0 && name[0] == '.' {
		return nil, syscall.ENOENT
	}

	// Handle IAM policy directory
	if name == "iam-policy" {
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		child := n.NewInode(ctx, &BQIAMPolicyDirectoryNode{
			projectID: n.projectID,
			datasetID: n.datasetID,
		}, stable)
		return child, 0
	}

	// Handle dataset metadata file
	if name == "metadata.json" {
		// TODO: Implement dataset metadata file
		// For now, return ENOENT
		return nil, syscall.ENOENT
	}

	// Handle table metadata files (table_name.json)
	if strings.HasSuffix(name, ".json") {
		// TODO: Implement table metadata files
		// For now, return ENOENT
		return nil, syscall.ENOENT
	}

	return nil, syscall.ENOENT
}
