package beat

type RedisConfig struct {
	Period  *int64
	Host    *string
	Port    *int
	Network *string
	MaxConn *int
	Auth    struct {
		Required     *bool   `yaml:"required"`
		RequiredPass *string `yaml:"required_pass"`
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
