package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ServerConfig holds server-specific configuration
type ServerConfig struct {
	Port      int    `yaml:"port"`
	Host      string `yaml:"host"`
	AutoStart bool   `yaml:"auto_start"`
}

// Config represents the application configuration
type Config struct {
	Mappings map[string]string `yaml:"mappings"`
	Defaults Defaults          `yaml:"defaults"`
	Server   ServerConfig      `yaml:"server"`
	filePath string            // Store the path where config was loaded from
}

// GetFilePath returns the path where the config was loaded from
func (c *Config) GetFilePath() string {
	return c.filePath
}

// Load loads the configuration from the specified path or auto-detects it
func Load(configPath string) (*Config, error) {
	path, err := resolveConfigPath(configPath)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		// If file doesn't exist, return default config
		if os.IsNotExist(err) {
			return getDefaultConfig(path), nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := getDefaultConfig(path)
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Expand environment variables in mappings and defaults
	config.expandEnvVars()

	return config, nil
}

// Save writes the configuration to the file it was loaded from
func (c *Config) Save() error {
	if c.filePath == "" {
		return fmt.Errorf("no config file path set")
	}

	// Ensure the directory exists
	dir := filepath.Dir(c.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(c.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// resolveConfigPath determines which config file to use
func resolveConfigPath(configPath string) (string, error) {
	// 1. Use explicitly provided path
	if configPath != "" {
		return configPath, nil
	}

	// 2. Check CIO_CONFIG environment variable
	if envPath := os.Getenv("CIO_CONFIG"); envPath != "" {
		return envPath, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}

	// 3. Check ~/.config/cio/config.yaml
	primaryPath := filepath.Join(homeDir, DefaultConfigDir, ConfigFileName)
	if _, err := os.Stat(primaryPath); err == nil {
		return primaryPath, nil
	}

	// 4. Check ~/.cio/config.yaml
	fallbackPath := filepath.Join(homeDir, FallbackConfigDir, ConfigFileName)
	if _, err := os.Stat(fallbackPath); err == nil {
		return fallbackPath, nil
	}

	// Return primary path as default (will be created on first save)
	return primaryPath, nil
}

// getDefaultConfig returns a config with default values
func getDefaultConfig(filePath string) *Config {
	defaults := GetDefaults()
	return &Config{
		Mappings: make(map[string]string),
		Defaults: defaults,
		Server: ServerConfig{
			Port:      DefaultServerPort,
			Host:      DefaultServerHost,
			AutoStart: false,
		},
		filePath: filePath,
	}
}

// expandEnvVars expands environment variables in configuration values
func (c *Config) expandEnvVars() {
	// Expand in mappings
	for k, v := range c.Mappings {
		c.Mappings[k] = os.ExpandEnv(v)
	}

	// Expand in defaults
	c.Defaults.ProjectID = os.ExpandEnv(c.Defaults.ProjectID)
	c.Defaults.Region = os.ExpandEnv(c.Defaults.Region)
}

// AddMapping adds or updates a mapping
func (c *Config) AddMapping(alias, path string) {
	if c.Mappings == nil {
		c.Mappings = make(map[string]string)
	}
	c.Mappings[alias] = path
}

// DeleteMapping removes a mapping
func (c *Config) DeleteMapping(alias string) bool {
	if _, exists := c.Mappings[alias]; exists {
		delete(c.Mappings, alias)
		return true
	}
	return false
}

// GetMapping retrieves a mapping by alias
func (c *Config) GetMapping(alias string) (string, bool) {
	path, exists := c.Mappings[alias]
	return path, exists
}

// ListMappings returns all mappings
func (c *Config) ListMappings() map[string]string {
	// Return a copy to prevent external modification
	mappings := make(map[string]string, len(c.Mappings))
	for k, v := range c.Mappings {
		mappings[k] = v
	}
	return mappings
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	for alias, path := range c.Mappings {
		if strings.ContainsAny(alias, "/.") {
			return fmt.Errorf("invalid alias %q: cannot contain '/' or '.'", alias)
		}
		if !strings.HasPrefix(path, "gs://") {
			return fmt.Errorf("invalid path for alias %q: must start with 'gs://'", alias)
		}
	}
	return nil
}
