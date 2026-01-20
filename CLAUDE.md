# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`cio` (Cloud IO) is a fast Go CLI tool for Google Cloud Storage, BigQuery, and IAM that replaces lengthy `gcloud storage`, `bq`, and `gcloud iam` commands with short, memorable aliases. It maps user-defined aliases to full GCS bucket paths, BigQuery paths, and IAM paths, enabling commands like `cio ls :am` instead of `gcloud storage ls gs://io-spooler-onprem-archived-metrics/` or `cio ls :mydata` instead of `bq ls project-id:dataset`.

**Alias Syntax:**
- Aliases are prefixed with `:` to distinguish them from regular paths
- Created without `:` prefix: `cio map am gs://bucket/` or `cio map mydata bq://project-id.dataset`
- Used with `:` prefix: `cio ls :am/path/` or `cio ls :mydata`

**Key capabilities:**
- Alias-based path resolution for GCS, BigQuery, and IAM
  - GCS: `:am` → `gs://bucket-name/`
  - BigQuery: `:mydata` → `bq://project-id.dataset`
  - IAM: Direct paths like `iam://project-id/service-accounts`
- Familiar Unix-like commands (`ls`, `cp`, `rm` with various flags)
- Wildcard pattern support (`*.log`, `2024-*.csv`) for GCS commands
- BigQuery listing: datasets, tables, and table schemas
- IAM listing: service accounts with metadata
- YAML configuration with environment variable expansion
- Google Application Default Credentials (ADC) authentication
- Singleton client pattern for performance (GCS, BigQuery, and IAM)
- Bidirectional file transfer (local ↔ GCS)
- FUSE filesystem support for GCS, BigQuery, and IAM (experimental)

## Development Commands

### Using Mise (Recommended)
```bash
mise build                  # Build the cio binary
mise test                   # Run all tests
mise test-coverage          # Generate HTML coverage report
mise check                  # Run fmt, vet, and tests
mise fmt                    # Format code
mise vet                    # Run go vet
mise lint                   # Run golangci-lint (if installed)
mise tidy                   # Tidy and verify dependencies
mise clean                  # Remove build artifacts
mise doctor                 # Check development environment
mise auth-setup             # Setup GCP authentication (gcloud auth application-default login)
```

### Running the Tool
```bash
mise run                    # Build and run with arguments (e.g., mise run -- ls am)
mise dev                    # Run with --verbose flag
go run cmd/cio/main.go      # Direct execution
```

### Building
```bash
mise build                  # Local build → ./cio
mise release-build          # Optimized build with stripped symbols
mise install                # Install to $GOPATH/bin
```

### Testing
```bash
mise test                   # Run all tests with verbose output
go test ./...               # Standard test run
go test -v ./internal/resolver  # Test specific package
```

## Architecture

### Core Components

**0. Resource Abstraction Layer (`internal/resource/`)**
- **Unified interface** for all resource types (GCS, BigQuery, IAM)
- `Resource` interface defines common operations:
  - `List()` - list resources at a path
  - `Remove()` - delete resources
  - `Info()` - get detailed information
  - `ParsePath()` - parse resource paths
  - `FormatShort/Long/Detailed()` - format output
- Implementations:
  - `GCSResource` - handles GCS objects and directories
  - `BigQueryResource` - handles BigQuery datasets and tables
  - `IAMResource` - handles IAM service accounts
- `Factory` - creates appropriate resource handler based on path type
- `ResourceInfo` - unified data structure for resource metadata
- Benefits:
  - Single codebase for all resource types
  - Easy to add new resource types (Cloud Storage, Cloud SQL, etc.)
  - Consistent behavior across all commands
  - Simplified CLI command implementations

**1. Alias Resolution Flow (`internal/resolver/`)**
- `Resolver` converts alias paths (with `:` prefix) to full paths using config mappings
- Examples:
  - GCS: `:am/2024/01/` → `gs://io-spooler-onprem-archived-metrics/2024/01/`
  - BigQuery: `:mydata.table1` → `bq://project-id.dataset.table1`
- `ReverseResolve()` converts full paths back to alias format:
  - GCS: `gs://bucket/file` → `:am/file`
  - BigQuery: `bq://project.dataset.table` → `:mydata.table`
- Path parsing:
  - `ParseGCSPath()` splits `gs://bucket/object` into components
  - `ParseBQPath()` splits `bq://project.dataset.table` into components
- Path detection:
  - `IsGCSPath()` checks for `gs://` prefix
  - `IsBQPath()` checks for `bq://` prefix
  - `IsIAMPath()` checks for `iam://` prefix
- **Important**: Input must start with `:` for alias paths (e.g., `:am/path` or `:mydata`)

**2. Configuration System (`internal/config/`)**
- YAML-based config with environment variable expansion (`${VAR}` syntax)
- Config resolution order: `--config` flag → `CIO_CONFIG` env → `~/.config/cio/config.yaml` → `~/.cio/config.yaml`
- Default region: `europe-west3` (per global CLAUDE.md)
- `Config.Validate()` ensures aliases don't contain `/` or `.` and paths start with `gs://`
- Auto-creates config directory on first `Save()`

**3. Storage Client (`internal/storage/`)**
- **Singleton pattern**: `GetClient()` uses `sync.Once` to create GCS client once per process
- Authentication via ADC (Application Default Credentials)
- Operations:
  - `ListBuckets()` - lists all buckets in a GCP project
  - `ListByPath()`, `ListWithPattern()` - list objects in buckets
  - `UploadFile()`, `UploadDirectory()` - upload to GCS
  - `DownloadFile()`, `DownloadDirectory()`, `DownloadWithPattern()` - download from GCS
  - `RemoveObject()`, `RemoveDirectory()`, `RemoveWithPattern()` - delete from GCS
- `Close()` should be called when program exits (handled in `cmd/cio/main.go`)

**3b. BigQuery Client (`internal/bigquery/`)**
- **Singleton pattern**: `GetClient()` uses `sync.Once` to create BigQuery client once per process
- Requires project ID parameter for initialization
- Authentication via ADC (Application Default Credentials)
- Operations:
  - `ListDatasets()` - lists datasets in a project
  - `ListTables()` - lists tables in a dataset
  - `DescribeTable()` - shows table schema and metadata
- `BQObjectInfo` type for formatting BigQuery results
- `Close()` should be called when program exits

**3c. IAM Client (`iam/`)**
- **Singleton pattern**: `GetClient()` uses `sync.Once` to create IAM client once per process
- Authentication via ADC (Application Default Credentials)
- Operations:
  - `ListServiceAccounts()` - lists service accounts in a project
  - `GetServiceAccount()` - gets details about a specific service account
  - `ParseIAMPath()` - parses `iam://project-id/resource-type` paths
- `ServiceAccountInfo` type for formatting service account results
- `Close()` should be called when program exits

**4. CLI Commands (`internal/cli/`)**
- **root.go**: Global flags (`--config`, `--project`, `--region`, `--verbose`)
  - `PersistentPreRunE` loads config and overrides with CLI flags
- **map.go**: Manage alias mappings (`map <alias> <path>`, `map list`, `map show`, `map delete`)
  - Aliases created without `:` but used with it
- **ls.go**: List GCS objects, BigQuery datasets/tables, or IAM service accounts
  - GCS: formatting options (`-l`, `-r`, `--human-readable`, `--max-results`)
  - GCS wildcards: `cio ls ':am/logs/*.log'`
  - BigQuery: lists datasets (`bq://project`) or tables (`bq://project.dataset`)
  - BigQuery wildcards: `cio ls ':mydata.events_*'`
  - BigQuery `-l` shows type, size, and row counts
  - IAM: lists service accounts (`iam://project-id/service-accounts`)
  - IAM `-l` shows email, display name, and disabled status
  - Output uses alias format: `:am/file.txt` or `:mydata.table1`
  - `handleBigQueryList()` function handles BigQuery-specific listing
- **info.go**: Show detailed BigQuery table information
  - Displays table schema with nested RECORD fields
  - Shows description, timestamps, location, size, row count
  - Example: `cio info :mydata.events`
  - Only supports BigQuery tables (not GCS objects)
- **cp.go**: Copy files between local and GCS (`-r` for recursive)
  - Upload: `cio cp file.txt :am/path/`
  - Download: `cio cp :am/file.txt ./local/`
  - Wildcard download: `cio cp ':am/logs/*.log' ./local/`
  - Messages show alias paths: `Uploaded: file.txt → :am/path/file.txt`
- **rm.go**: Remove GCS objects and BigQuery tables/datasets (`-r` recursive, `-f` force)
  - GCS wildcards: `cio rm ':am/temp/*.tmp'`
  - BigQuery tables: `cio rm :mydata.events`
  - BigQuery wildcards: `cio rm ':mydata.temp_*'`
  - BigQuery datasets: `cio rm -r :mydata` (removes all tables)
  - Lists all matching items BEFORE deletion
  - Confirmation prompts unless `-f` is used
  - Displays alias paths in confirmations and output
  - **CRITICAL**: Only deletes when explicitly requested by user

**5. Wildcard Support (`internal/resolver/wildcard.go`)**
- `HasWildcard()` detects `*` and `?` in paths
- `MatchPattern()` implements glob-style pattern matching
- `SplitWildcardPath()` separates base path from pattern
- Pattern matching supports:
  - `*` - matches any sequence of characters
  - `?` - matches any single character

### Data Flow

**GCS Operations:**
```
User Input (e.g., "cio ls :am/2024/")
    ↓
CLI Command (ls.go)
    ↓
Resolver.Resolve(":am/2024/")  →  strips : prefix, reads Config.Mappings
    ↓
"gs://bucket-name/2024/"
    ↓
storage.List(bucket, prefix)  →  uses singleton GCS client
    ↓
Formatter (short/long/human-readable)
    ↓
Resolver.ReverseResolve()  →  converts back to :am/2024/file.txt
    ↓
Output to stdout (with alias format)
```

**BigQuery Operations:**
```
User Input (e.g., "cio ls :mydata" or "cio ls bq://project-id.dataset")
    ↓
CLI Command (ls.go)
    ↓
Resolver.Resolve(":mydata")  →  strips : prefix, reads Config.Mappings
    ↓
"bq://project-id.dataset"
    ↓
handleBigQueryList()  →  detects bq:// path
    ↓
bigquery.ParseBQPath()  →  splits into project/dataset/table components
    ↓
bigquery.ListDatasets() or bigquery.ListTables()  →  uses singleton BigQuery client
    ↓
BQObjectInfo formatting (short/long)
    ↓
Resolver.ReverseResolve()  →  converts back to :mydata.table1
    ↓
Output to stdout (with alias format)
```

## Important Patterns

### CRITICAL SAFETY RULE - Data Deletion

**UNDER NO CIRCUMSTANCES should anything be deleted from GCS or BigQuery unless EXPLICITLY requested by the user.**

This rule applies to:
- GCS objects and directories (`storage.RemoveObject`, `storage.RemoveDirectory`, `storage.RemoveWithPattern`)
- BigQuery tables and datasets (`bigquery.RemoveTable`, `bigquery.RemoveDataset`, `bigquery.RemoveTablesWithPattern`)
- ANY operation that uses the `rm` command or deletion functions

**Rules:**
1. NEVER proactively suggest or execute deletions
2. NEVER delete data as part of "cleanup" or "optimization"
3. ONLY execute `rm` operations when the user explicitly runs `cio rm <path>`
4. ALWAYS show what will be deleted and ask for confirmation (unless `-f` flag is used)
5. List all matching objects/tables BEFORE deletion confirmation
6. When in doubt, DO NOT DELETE - ask the user first

**Example - WRONG:**
```
User: "The temp tables are cluttering the dataset"
Assistant: "Let me clean those up for you" [executes rm command] ❌ NEVER DO THIS
```

**Example - CORRECT:**
```
User: "The temp tables are cluttering the dataset"
Assistant: "You can remove them with: cio rm ':mydata.temp_*'" ✅
[User must explicitly run the command themselves]
```

### Adding New Commands

**Option 1: Using Resource Abstraction (Recommended)**
1. Create command file in `internal/cli/` (e.g., `mycommand.go`)
2. Create Resolver: `resolver := resolver.New(cfg)`
3. Resolve path: `fullPath, err := resolver.Resolve(userPath)`
4. Create factory: `factory := resource.NewFactory(resolver.ReverseResolve)`
5. Get resource handler: `res, err := factory.Create(fullPath)`
6. Call resource methods: `res.List()`, `res.Remove()`, `res.Info()`, etc.
7. Format output: `res.FormatShort()`, `res.FormatLong()`, etc.
8. Register command in `init()` function: `rootCmd.AddCommand(myCmd)`

Example:
```go
// List resources using abstraction
factory := resource.NewFactory(r.ReverseResolve)
res, err := factory.Create(fullPath)
resources, err := res.List(ctx, fullPath, &resource.ListOptions{})
for _, info := range resources {
    aliasPath := r.ReverseResolve(info.Path)
    fmt.Println(res.FormatShort(info, aliasPath))
}
```

**Option 2: Direct Implementation (Legacy)**
1. Create command file in `internal/cli/` (e.g., `cp.go`)
2. Use `GetConfig()` to access global config instance
3. Create Resolver: `resolver := resolver.New(cfg)`
4. Use `resolver.Resolve(aliasPath)` to convert alias to full path
5. Check path type and call appropriate package (storage or bigquery)
6. Register command in `init()` function: `rootCmd.AddCommand(cpCmd)`

### Path Handling
- Always use `resolver.Resolve()` to handle both alias paths and full GCS paths
- Use `resolver.NormalizePath()` to ensure paths end with `/` when needed
- Use `resolver.ParseGCSPath()` to split GCS paths into bucket and object
- Check for wildcards with `resolver.HasWildcard()` before processing paths
- Use `resolver.MatchPattern()` for pattern matching operations
- Validate user input with `resolver/validator.go` functions

### Wildcard Operations
- Check paths for wildcards using `resolver.HasWildcard(path)`
- For listing: use `storage.ListWithPattern()` instead of `storage.List()`
- For removal: use `storage.RemoveWithPattern()` instead of `storage.RemoveObject()`
- For download: use `storage.DownloadWithPattern()` instead of `storage.DownloadFile()`
- Always quote wildcard patterns in shell: `':am/logs/*.log'` not `:am/logs/*.log`
- Wildcard matching happens on the object name after alias resolution

### Alias Path Formatting
- All storage functions accept a `PathFormatter` parameter for output
- Use `resolver.ReverseResolve` as the formatter to convert GCS paths to alias format
- Output will show `:am/file.txt` instead of `gs://bucket-name/file.txt`
- This provides consistent user experience - input and output use same format

### Configuration Changes
- Always call `cfg.Save()` after modifying mappings
- Handle missing config gracefully (returns default config)
- Expand env vars with `os.ExpandEnv()` during config load

### Error Messages
- For missing aliases, suggest: `run 'cio map list' to see available mappings`
- For auth issues, suggest: `run 'mise auth-setup' or 'gcloud auth application-default login'`

## Testing Notes

- Use `context.Background()` for test contexts
- Mock GCS client for unit tests of storage operations
- Test resolver logic without actual GCS calls
- Validate config parsing with malformed YAML examples

## Implemented Features

### Phase 1-3: Completed
- CLI foundation with Cobra
- Configuration management (YAML with env var expansion)
- Alias mapping system (`map` command)
- `ls` command with formatting options (`-l`, `-r`, `--human-readable`)
- Wildcard pattern matching (`*`, `?`) for GCS operations
- Bucket listing: `cio ls-new 'gs://project-id:'` lists all buckets

### Phase 4: BigQuery Support - Completed
- ✅ BigQuery client with singleton pattern
- ✅ BigQuery path format (`bq://project.dataset.table`)
- ✅ Alias support for BigQuery paths (`:mydata.table1`)
- ✅ List datasets: `cio ls bq://project-id`
- ✅ List tables: `cio ls bq://project-id.dataset`
- ✅ List tables with wildcards: `cio ls ':mydata.events_*'`
- ✅ Table metadata: `cio ls -l :mydata.events` (shows size and row counts)
- ✅ Detailed schema: `cio info :mydata.events` (shows full schema with nested fields)
- ✅ Reverse resolve for BigQuery (output shows aliases)
- ✅ Remove tables: `cio rm :mydata.events`
- ✅ Remove with wildcards: `cio rm ':mydata.temp_*'`
- ✅ Remove datasets: `cio rm -r :mydata`

### Phase 5: Copy and Remove - Completed
- ✅ `cp` command (local ↔ GCS, recursive, wildcards)
- ✅ `rm` command for GCS (recursive, force, wildcards)
- ✅ `rm` command for BigQuery (tables, datasets, wildcards)
- ✅ Confirmation prompts with preview of items to be deleted
- ✅ Force flag (`-f`) to skip confirmations

## Usage Examples

### GCS Examples
```bash
# List all buckets in a project
cio ls-new 'gs://my-project-id:'
cio ls-new -l 'gs://my-project-id:'  # With details

# Create alias for GCS bucket
cio map am gs://io-spooler-onprem-archived-metrics/

# List using alias
cio ls :am

# List with details
cio ls -l :am/2024/

# Copy local to GCS
cio cp file.txt :am/2024/01/

# Copy from GCS to local
cio cp :am/2024/01/data.txt ./

# Remove with wildcards (shows preview and asks for confirmation)
cio rm ':am/temp/*.tmp'

# Force remove without confirmation
cio rm -f ':am/old-data/*'
```

### BigQuery Examples
```bash
# Create alias for BigQuery dataset
cio map mydata bq://my-project-id.my-dataset

# List all datasets in a project
cio ls bq://my-project-id

# List tables in a dataset using full path
cio ls bq://my-project-id.my-dataset

# List tables using alias
cio ls :mydata

# List tables with wildcards
cio ls ':mydata.events_*'

# Show table metadata (one line with size and rows)
cio ls -l :mydata.table1

# Show detailed schema information
cio info :mydata.table1

# Remove single table (asks for confirmation)
cio rm :mydata.temp_table

# Remove tables with wildcard (shows preview, asks for confirmation)
cio rm ':mydata.staging_*'

# Remove entire dataset with all tables (requires -r flag)
cio rm -r :mydata

# Force remove without confirmation
cio rm -f :mydata.old_table
```

### IAM Examples
```bash
# List service accounts (short format - email only)
cio ls iam://my-project-id/service-accounts

# List with details (email, display name, disabled status)
cio ls -l iam://my-project-id/service-accounts

# FUSE filesystem - mount and browse service accounts
cio mount ~/gcs
ls ~/gcs/iam/service-accounts/
cat ~/gcs/iam/service-accounts/my-sa@project.iam.gserviceaccount.com/metadata.json
```

## Future Features (Phase 6+)
- BigQuery data operations (query, export, import)
- Web server for file browsing (config: `server.port`, `server.host`, `server.auto_start`)
- Additional commands: `mv`, `cat`, `du`
- GCS to GCS copy operations
- BigQuery table schema display
