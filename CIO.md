# cio — Cloud IO CLI Reference

`cio` is a fast Go CLI for Google Cloud Storage (GCS), BigQuery, and IAM.
It replaces verbose `gcloud storage` / `bq` commands with short aliases and
Unix-style verbs.

## Alias System

Aliases map a short name to a full GCS or BigQuery path.

```
cio map <alias> <path>    # register an alias
cio map list              # show all aliases
cio map show <alias>      # show the full path for one alias
cio map delete <alias>    # remove an alias
```

- Created **without** `:` prefix: `cio map am gs://my-bucket/`
- Used **with** `:` prefix: `cio ls :am/2024/`
- Aliases cannot contain `/` or `.`
- Supported path types: `gs://`, `bq://`

```yaml
# Example config: ~/.config/cio/config.yaml
mappings:
  am:     gs://io-spooler-onprem-archived-metrics/
  mydata: bq://my-project.my-dataset
defaults:
  project_id: my-gcp-project
  region: europe-west3
  parallelism: 50
download:
  parallel_threshold: 10485760   # 10 MB — use chunked download above this size
  chunk_size: 8388608            # 8 MB per chunk
  max_chunks: 8
```

Config resolution order: `--config` flag → `$CIO_CONFIG` → `~/.config/cio/config.yaml` → `~/.cio/config.yaml`

---

## Commands

### `cio ls` — List resources

```
cio ls <path> [flags]
```

**Path formats**

| Input | What it lists |
|---|---|
| `:am` | objects at root of GCS alias |
| `:am/2024/` | objects under prefix |
| `gs://bucket/prefix/` | objects by full GCS path |
| `gs://my-project:` | all buckets in a project |
| `bq://` | all datasets in default project |
| `bq://my-project` | datasets in a specific project |
| `:mydata` | tables in a BigQuery dataset alias |
| `bq://project.dataset` | tables by full BQ path |
| `iam://project/service-accounts` | IAM service accounts |

**Wildcards** (always quote in shell):

```bash
cio ls ':am/logs/*.log'
cio ls ':mydata.events_*'
```

**Flags**

| Flag | Meaning |
|---|---|
| `-l` | long format (size, timestamp, path) |
| `-r` / `-R` | recursive |
| `--human-readable` | sizes as 1.2 MB instead of bytes |
| `-t` | sort by modification time (newest first) |
| `-S` | sort by size (largest first) |
| `--max-results N` | stop after N results |
| `--raw` | one path per line, no headers (for scripting) |
| `-n` / `--no-map` | show full `gs://` paths, suppress alias mapping |

---

### `cio cp` — Copy files

```
cio cp <source> <destination> [flags]
```

**Directions**

```bash
# Upload local → GCS
cio cp file.txt :am/2024/
cio cp -r ./logs/ :am/logs/2024/

# Download GCS → local
cio cp :am/2024/data.csv ./downloads/
cio cp -r :am/logs/2024/ ./local-logs/

# Wildcard download (flattens structure by default)
cio cp ':am/logs/*.log' ./local-logs/

# Wildcard download preserving directory structure
cio cp -r 'gs://bucket/*' /tmp/certs/
```

**Flags**

| Flag | Meaning |
|---|---|
| `-r` | recursive; also preserves directory structure for wildcards |
| `--force-copy` | re-download even if destination file already exists with the correct size |
| `-j N` | limit parallel chunks per file (default 50, range 1–200) |
| `-v` | verbose: show per-file progress and transfer rate |

**Skip-if-exists:** By default `cp` skips a file when a local file already
exists with the same byte size as the GCS object (`Skipped N/M: … already
exists with correct size`). Use `--force-copy` to always re-download.

**Parallel chunked download:** Files ≥ 10 MB are split into up to 8 parallel
chunks automatically. Use `-j 1` to force serial download.

**GCS path conflicts:** GCS allows a plain object `foo` alongside objects
under `foo/…`. `cio cp -r` handles this gracefully:
- Zero-byte marker files (e.g. GCS "directory markers") are replaced with
  real local directories automatically.
- Non-empty conflicting objects are skipped with a warning.

---

### `cio rm` — Remove objects

```
cio rm <path> [flags]
```

```bash
# Remove a single GCS object
cio rm :am/2024/data.csv

# Remove with wildcard (shows preview, asks for confirmation)
cio rm ':am/temp/*.tmp'

# Recursive remove
cio rm -r :am/old-data/

# Remove BigQuery table
cio rm :mydata.temp_table

# Remove BQ tables matching wildcard
cio rm ':mydata.staging_*'

# Remove entire BQ dataset (all tables)
cio rm -r :mydata

# Skip confirmation prompt
cio rm -f ':am/old-data/*'
```

**Flags**

| Flag | Meaning |
|---|---|
| `-r` | recursive |
| `-f` | force — skip confirmation |

Always previews what will be deleted before asking for confirmation (unless `-f`).

---

### `cio du` — Disk usage

```
cio du <path> [flags]
```

```bash
# Per-subdirectory breakdown + grand total
cio du :am/2024/

# Grand total only
cio du -s :am/2024/

# Wildcard: one summary per match + grand total
cio du 'gs://bucket/prefix*/'

# Suppress grand total
cio du --no-summary 'gs://bucket/prefix*/'

# Raw byte counts
cio du --bytes :am/
```

**Flags**

| Flag | Meaning |
|---|---|
| `-s` | summarize — grand total only |
| `--no-summary` | suppress the grand total line |
| `-b` / `--bytes` | raw byte counts instead of human-readable |

Parallelism is controlled by the global `-j` flag (default 50).

---

### `cio cat` — Stream objects to stdout

```
cio cat <path> [<path>…] [flags]
```

```bash
cio cat :am/logs/app.log
cio cat 'gs://bucket/*.log'
cio cat :am/2024/a.csv :am/2024/b.csv
```

Supports aliases, full `gs://` paths, and wildcard patterns.
Multiple paths are concatenated in order.

---

### `cio info` — Detailed resource info

```
cio info <path>
```

Currently supported for **BigQuery tables only**. Displays full schema
(including nested RECORD fields), description, location, size, row count,
and creation/modification timestamps.

```bash
cio info :mydata.events
cio info bq://my-project.my-dataset.my-table
```

---

### `cio query` — BigQuery SQL

```
cio query [SQL] [flags]
```

```bash
# One-shot query
cio query "SELECT COUNT(*) FROM :mydata.events"

# Aliases work in SQL
cio query "SELECT * FROM :mydata.events LIMIT 10"

# Output formats
cio query --format json "SELECT * FROM :mydata.events LIMIT 5"
cio query --format csv  "SELECT id, name FROM :mydata.users"

# Validate without running
cio query --dry-run "SELECT * FROM :mydata.huge_table"

# Read SQL from file
cio query --file analysis.sql

# Interactive shell (history, tab completion, \d <table>, \q to quit)
cio query
```

**Flags**

| Flag | Meaning |
|---|---|
| `-f` / `--format` | `table` (default), `json`, or `csv` |
| `-n` / `--max-results N` | cap rows returned (default 1000) |
| `--dry-run` | validate query without executing |
| `--file` | read SQL from a file |
| `--stats` | show query statistics (default true) |

Interactive shell features: multi-line input (end with `;`), command history
(`~/.config/cio/query_history`), `\d <table>` describe, `\l` list hint,
`\q` quit, Ctrl+D exit.

---

### `cio mount` — FUSE filesystem

Mount GCP resources as a local filesystem for browsing with standard tools.

```
cio mount <mountpoint> [flags]
```

```bash
cio mount ~/gcp
ls ~/gcp/storage/          # GCS buckets
ls ~/gcp/bigquery/         # BigQuery datasets
ls ~/gcp/pubsub/           # Pub/Sub topics

# With specific project
cio mount --project my-project ~/gcp

# Read-only
cio mount --read-only ~/gcp

# Unmount
umount ~/gcp
```

Filesystem layout:
```
<mountpoint>/
  storage/    ← GCS buckets and objects
  bigquery/   ← datasets and tables
  pubsub/     ← topics and subscriptions
```

---

## Global Flags

| Flag | Meaning |
|---|---|
| `--config <file>` | config file path |
| `--project <id>` | GCP project ID (overrides config) |
| `--region <region>` | GCP region (overrides config) |
| `-j N` | parallel operations for cp/rm (1–200, default 50); also `$CIO_PARALLEL` |
| `-v` / `--verbose` | verbose output |

---

## Authentication

`cio` uses **Google Application Default Credentials (ADC)**.

```bash
gcloud auth application-default login
# or, if mise is set up:
mise auth-setup
```

---

## Path Syntax Summary

| Syntax | Meaning |
|---|---|
| `:alias` | root of a registered alias |
| `:alias/path/to/obj` | path within a GCS alias |
| `:alias.table` | BigQuery table within a dataset alias |
| `gs://bucket/` | full GCS bucket root |
| `gs://bucket/prefix/` | full GCS prefix |
| `gs://project:` | list all buckets in a project |
| `bq://` | default project (BigQuery) |
| `bq://project` | specific project |
| `bq://project.dataset` | dataset |
| `bq://project.dataset.table` | table |
| `iam://project/service-accounts` | IAM service accounts |

Wildcards `*` and `?` are supported in GCS and BigQuery paths.
Always quote wildcard paths in the shell: `':alias/logs/*.log'`
