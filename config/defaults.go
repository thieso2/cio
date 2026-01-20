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
)

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
