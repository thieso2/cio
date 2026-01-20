package config

const (
	// DefaultRegion is the default GCP region for operations
	DefaultRegion = "europe-west3"

	// DefaultConfigDir is the primary config directory
	DefaultConfigDir = ".config/cio"

	// FallbackConfigDir is the fallback config directory
	FallbackConfigDir = ".cio"

	// ConfigFileName is the name of the config file
	ConfigFileName = "config.yaml"

	// DefaultServerPort is the default port for the web server
	DefaultServerPort = 8080

	// DefaultServerHost is the default host for the web server
	DefaultServerHost = "localhost"

	// DefaultParallelism is the default number of concurrent operations for cp/rm
	DefaultParallelism = 50

	// MinParallelism is the minimum allowed parallelism value
	MinParallelism = 1

	// MaxParallelism is the maximum allowed parallelism value
	MaxParallelism = 200

	// DefaultParallelThreshold is the minimum file size (in bytes) to use parallel chunked download
	// 10MB default - files larger than this will use parallel chunked download
	DefaultParallelThreshold = 10 * 1024 * 1024 // 10MB

	// DefaultChunkSize is the size of each chunk for parallel downloads
	// 8MB per chunk - good balance between memory usage and download speed
	DefaultChunkSize = 8 * 1024 * 1024 // 8MB

	// DefaultMaxChunks is the maximum number of parallel chunks per file
	DefaultMaxChunks = 8

	// MinChunkSize is the minimum allowed chunk size
	MinChunkSize = 1 * 1024 * 1024 // 1MB

	// MaxChunkSize is the maximum allowed chunk size
	MaxChunkSize = 32 * 1024 * 1024 // 32MB

	// MinMaxChunks is the minimum allowed max chunks value
	MinMaxChunks = 1

	// MaxMaxChunks is the maximum allowed max chunks value
	MaxMaxChunks = 32
)

// DownloadConfig holds download-specific configuration
type DownloadConfig struct {
	// ParallelThreshold is the minimum file size (in bytes) to use parallel chunked download
	ParallelThreshold int64 `yaml:"parallel_threshold"`
	// ChunkSize is the size of each chunk for parallel downloads
	ChunkSize int64 `yaml:"chunk_size"`
	// MaxChunks is the maximum number of parallel chunks per file
	MaxChunks int `yaml:"max_chunks"`
}

// Defaults holds default configuration values
type Defaults struct {
	Region      string `yaml:"region"`
	ProjectID   string `yaml:"project_id"`
	Parallelism int    `yaml:"parallelism"`
}

// GetDefaults returns the default configuration values
func GetDefaults() Defaults {
	return Defaults{
		Region:      DefaultRegion,
		ProjectID:   "",
		Parallelism: DefaultParallelism,
	}
}
