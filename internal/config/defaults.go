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
)

// Defaults holds default configuration values
type Defaults struct {
	Region    string `yaml:"region"`
	ProjectID string `yaml:"project_id"`
}

// GetDefaults returns the default configuration values
func GetDefaults() Defaults {
	return Defaults{
		Region:    DefaultRegion,
		ProjectID: "",
	}
}
