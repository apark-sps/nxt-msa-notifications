package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
)

// Config holds all application configuration loaded from environment variables.
// In production, DB credentials are overlaid by the AWS Secrets Manager fetcher.
type Config struct {
	AppEnv   string `env:"APP_ENV" envDefault:"dev"`
	Server   ServerConfig
	Database DatabaseConfig
	AMQP     AMQPConfig
	Stream   StreamConfig
}

type ServerConfig struct {
	Port    string `env:"SERVER_PORT" envDefault:"8085"` // HTTP + WebSocket server port
	PodName string `env:"POD_NAME"`                      // Kubernetes pod name — used as stream consumer name
}

type DatabaseConfig struct {
	SecretName string `env:"DB_SECRET_NAME"`
	Host       string `env:"DB_HOST"`
	HostRO     string `env:"DB_HOST_RO"` // Read-replica host for CountUnread / FindByUser queries
	Port       int    `env:"DB_PORT" envDefault:"5432"`
	Name       string `env:"DB_NAME" envDefault:"n360_dev"`
	User       string `env:"DB_USER"`
	Password   string `env:"DB_PASSWORD"`
	SSLMode    string `env:"DB_SSL_MODE"` // e.g. "disable" or "require"
}

type AMQPConfig struct {
	URI          string `env:"AMQP_URI"`
	PersistQueue string `env:"AMQP_PERSIST_QUEUE"`              // notifications.persist — quorum queue for DB writes
	Exchange     string `env:"AMQP_EXCHANGE"`                   // notifications.events  — topic exchange
	RoutingKey   string `env:"AMQP_ROUTING_KEY" envDefault:"#"` // wildcard to catch all event types
}

type StreamConfig struct {
	URI        string `env:"STREAM_URI"`
	StreamName string `env:"STREAM_NAME"`                            // notifications.broadcast
	MaxAgeSecs int    `env:"STREAM_MAX_AGE_SECS" envDefault:"86400"` // Message retention window in seconds
}

// DSN returns a PostgreSQL connection string for the primary (write) host.
func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode)
}

// DSRRO returns a PostgreSQL connection string for the read-only replica.
func (d DatabaseConfig) DSRRO() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.HostRO, d.Port, d.User, d.Password, d.Name, d.SSLMode)
}

// Load reads all configuration from environment variables with safe defaults.
func Load() (Config, error) {
	var cfg Config
	if err := loadEnv(&cfg); err != nil {
		return cfg, err
	}

	if cfg.Server.PodName == "" {
		if podName, err := os.Hostname(); err == nil {
			cfg.Server.PodName = podName
		}
	}
	if cfg.AMQP.Exchange == "" {
		cfg.AMQP.Exchange = fmt.Sprintf("sps-%s-notifications-exchange-events", cfg.AppEnv)
	}
	if cfg.AMQP.PersistQueue == "" {
		cfg.AMQP.PersistQueue = fmt.Sprintf("sps-%s-notifications-queue-persist", cfg.AppEnv)
	}
	if cfg.Stream.StreamName == "" {
		cfg.Stream.StreamName = fmt.Sprintf("sps-%s-notifications-queue-broadcast", cfg.AppEnv)
	}

	return cfg, nil
}

func loadEnv(ptr any) error {
	val := reflect.ValueOf(ptr).Elem()
	return parseFields(val)
}

func parseFields(val reflect.Value) error {
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structField := typ.Field(i)

		if field.Kind() == reflect.Struct {
			if err := parseFields(field); err != nil {
				return err
			}
			continue
		}

		key := structField.Tag.Get("env")
		if key == "" {
			continue
		}

		envVal := os.Getenv(key)
		if envVal == "" {
			envVal = structField.Tag.Get("envDefault")
		}

		if envVal != "" {
			switch field.Kind() {
			case reflect.String:
				field.SetString(envVal)
			case reflect.Int:
				num, err := strconv.Atoi(envVal)
				if err != nil {
					return fmt.Errorf("invalid int for %s: %w", key, err)
				}
				field.SetInt(int64(num))
			}
		}
	}
	return nil
}
