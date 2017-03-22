package config

import "time"

type Config struct {
	Period  time.Duration
	Host    string
	Port    int
	Network string
	MaxConn int
	Auth    AuthConfig
	Stats   StatsConfig
}

type AuthConfig struct {
	Required     bool   `config:"required"`
	RequiredPass string `config:"requiredpass"`
}

type StatsConfig struct {
	Server       bool `config:"server"`
	Clients      bool `config:"clients"`
	Memory       bool `config:"memory"`
	Persistence  bool `config:"persistence"`
	Stats        bool `config:"stats"`
	Replication  bool `config:"replication"`
	Cpu          bool `config:"cpu"`
	Commandstats bool `config:"commandstats"`
	Cluster      bool `config:"cluster"`
	Keyspace     bool `config:"keyspace"`
}

var DefaultConfig = Config{
	Period:  10 * time.Second,
	Host:    "localhost",
	Port:    6379,
	Network: "tcp",
	MaxConn: 10,
	Auth: AuthConfig{
		Required:     false,
		RequiredPass: "",
	},
	Stats: StatsConfig{
		Server:       true,
		Clients:      true,
		Memory:       true,
		Persistence:  true,
		Stats:        true,
		Replication:  true,
		Cpu:          true,
		Commandstats: true,
		Cluster:      true,
		Keyspace:     true,
	},
}
