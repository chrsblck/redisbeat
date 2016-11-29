package beat

import "time"

const (
	DEFAULT_PERIOD             time.Duration = 10 * time.Second
	DEFAULT_HOST               string        = "localhost"
	DEFAULT_PORT               int           = 6379
	DEFAULT_NETWORK            string        = "tcp"
	DEFAULT_MAX_CONN           int           = 10
	DEFAULT_AUTH_REQUIRED      bool          = false
	DEFAULT_AUTH_REQUIRED_PASS string        = ""
	DEFAULT_STATS_SERVER       bool          = true
	DEFAULT_STATS_CLIENT       bool          = true
	DEFAULT_STATS_MEMORY       bool          = true
	DEFAULT_STATS_PERSISTENCE  bool          = true
	DEFAULT_STATS_STATS        bool          = true
	DEFAULT_STATS_REPLICATION  bool          = true
	DEFAULT_STATS_CPU          bool          = true
	DEFAULT_STATS_COMMAND      bool          = true
	DEFAULT_STATS_CLUSTER      bool          = true
	DEFAULT_STATS_KEYSPACE     bool          = true
)

type RedisConfig struct {
	Period  *int64
	Host    *string
	Port    *int
	Network *string
	MaxConn *int
	Auth    struct {
		Required     *bool   `yaml:"required"`
		RequiredPass *string `yaml:"requiredpass"`
	}
	Stats struct {
		Server       *bool `yaml:"server"`
		Clients      *bool `yaml:"clients"`
		Memory       *bool `yaml:"memory"`
		Persistence  *bool `yaml:"persistence"`
		Stats        *bool `yaml:"stats"`
		Replication  *bool `yaml:"replication"`
		Cpu          *bool `yaml:"cpu"`
		Commandstats *bool `yaml:"commandstats"`
		Cluster      *bool `yaml:"cluster"`
		Keyspace     *bool `yaml:"keyspace"`
	}
}

type ConfigSettings struct {
	Input RedisConfig
}
