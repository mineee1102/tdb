package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Connection represents a saved database connection configuration.
type Connection struct {
	Name     string     `yaml:"name"`
	Type     string     `yaml:"type"` // postgres, mysql, sqlite, mssql
	DSN      string     `yaml:"dsn,omitempty"`
	Host     string     `yaml:"host,omitempty"`
	Port     int        `yaml:"port,omitempty"`
	User     string     `yaml:"user,omitempty"`
	Password string     `yaml:"password,omitempty"`
	Database string     `yaml:"database,omitempty"`
	SSLMode  string     `yaml:"ssl_mode,omitempty"`
	SSH      *SSHConfig `yaml:"ssh,omitempty"`
}

// SSHConfig holds SSH tunnel settings for a connection.
type SSHConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	User       string `yaml:"user"`
	Password   string `yaml:"password,omitempty"`
	PrivateKey string `yaml:"private_key,omitempty"`
	Passphrase string `yaml:"passphrase,omitempty"`
	RemoteHost string `yaml:"remote_host,omitempty"` // defaults to DB host
	RemotePort int    `yaml:"remote_port,omitempty"` // defaults to DB port
}

// Config holds the application configuration.
type Config struct {
	Connections []Connection `yaml:"connections"`
	Theme       string       `yaml:"theme,omitempty"`
}

// ConfigDir returns the directory for tdb configuration files (~/.config/tdb).
func ConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		// Fallback to current directory
		return ".tdb"
	}
	return filepath.Join(home, ".config", "tdb")
}

// configFilePath returns the full path to the configuration file.
func configFilePath() string {
	return filepath.Join(ConfigDir(), "connections.yaml")
}

// Load reads the configuration from the config file.
// If the file does not exist, returns an empty config.
func Load() (*Config, error) {
	path := configFilePath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{}, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &cfg, nil
}

// Save writes the configuration to the config file.
func (c *Config) Save() error {
	dir := ConfigDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	path := configFilePath()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	return nil
}

// AddConnection adds a new connection to the configuration.
// Returns an error if a connection with the same name already exists.
func (c *Config) AddConnection(conn Connection) error {
	for _, existing := range c.Connections {
		if existing.Name == conn.Name {
			return fmt.Errorf("connection %q already exists", conn.Name)
		}
	}
	c.Connections = append(c.Connections, conn)
	return nil
}

// RemoveConnection removes a connection by name.
// Returns an error if the connection is not found.
func (c *Config) RemoveConnection(name string) error {
	for i, conn := range c.Connections {
		if conn.Name == name {
			c.Connections = append(c.Connections[:i], c.Connections[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("connection %q not found", name)
}

// BuildDSN constructs a DSN string from individual connection fields.
// If DSN is already set, it returns that directly.
func (conn *Connection) BuildDSN() string {
	if conn.DSN != "" {
		return conn.DSN
	}

	switch conn.Type {
	case "postgres":
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			conn.User, conn.Password, conn.Host, conn.Port, conn.Database)
		if conn.SSLMode != "" {
			dsn += "?sslmode=" + conn.SSLMode
		}
		return dsn

	case "mysql":
		// format: user:password@tcp(host:port)/database
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			conn.User, conn.Password, conn.Host, conn.Port, conn.Database)
		return dsn

	case "sqlite":
		return conn.Database

	case "mssql":
		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
			conn.User, conn.Password, conn.Host, conn.Port, conn.Database)
		return dsn

	default:
		return conn.DSN
	}
}
