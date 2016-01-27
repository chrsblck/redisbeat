package beat

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/garyburd/redigo/redis"
)

type Redisbeat struct {
	period   time.Duration
	host     string
	port     int
	network  string
	maxConn  int
	auth     bool
	pass     string
	RbConfig ConfigSettings
	events   publisher.Client

	serverStats      bool
	clientsStats     bool
	memoryStats      bool
	persistenceStats bool
	statsStats       bool
	replicationStats bool
	cpuStats         bool
	commandStats     bool
	clusterStats     bool
	keyspaceStats    bool

	redisPool *redis.Pool
	done      chan struct{}
}

func New() *Redisbeat {
	return &Redisbeat{}
}

func (rb *Redisbeat) Config(b *beat.Beat) error {

	err := cfgfile.Read(&rb.RbConfig, "")
	if err != nil {
		logp.Err("Error reading configuration file: %v", err)
		return err
	}

	if rb.RbConfig.Input.Period != nil {
		rb.period = time.Duration(*rb.RbConfig.Input.Period) * time.Second
	} else {
		rb.period = 1 * time.Second
	}

	if rb.RbConfig.Input.Host != nil {
		rb.host = *rb.RbConfig.Input.Host
	} else {
		rb.host = "localhost"
	}

	if rb.RbConfig.Input.Port != nil {
		rb.port = *rb.RbConfig.Input.Port
	} else {
		rb.port = 6379
	}

	if rb.RbConfig.Input.Network != nil {
		rb.network = *rb.RbConfig.Input.Network
	} else {
		rb.network = "tcp"
	}

	if rb.RbConfig.Input.MaxConn != nil {
		rb.maxConn = *rb.RbConfig.Input.MaxConn
	} else {
		rb.maxConn = 10
	}

	if rb.RbConfig.Input.Auth.Required != nil {
		rb.auth = *rb.RbConfig.Input.Auth.Required
	} else {
		rb.auth = false
	}

	if rb.RbConfig.Input.Auth.RequiredPass != nil {
		rb.pass = *rb.RbConfig.Input.Auth.RequiredPass
	} else {
		rb.pass = ""
	}

	if rb.RbConfig.Input.Stats.Server != nil {
		rb.serverStats = *rb.RbConfig.Input.Stats.Server
	} else {
		rb.serverStats = true
	}

	if rb.RbConfig.Input.Stats.Clients != nil {
		rb.clientsStats = *rb.RbConfig.Input.Stats.Clients
	} else {
		rb.clientsStats = true
	}

	if rb.RbConfig.Input.Stats.Memory != nil {
		rb.memoryStats = *rb.RbConfig.Input.Stats.Memory
	} else {
		rb.memoryStats = true
	}

	if rb.RbConfig.Input.Stats.Persistence != nil {
		rb.persistenceStats = *rb.RbConfig.Input.Stats.Persistence
	} else {
		rb.persistenceStats = true
	}

	if rb.RbConfig.Input.Stats.Stats != nil {
		rb.statsStats = *rb.RbConfig.Input.Stats.Stats
	} else {
		rb.statsStats = true
	}

	if rb.RbConfig.Input.Stats.Replication != nil {
		rb.replicationStats = *rb.RbConfig.Input.Stats.Replication
	} else {
		rb.replicationStats = true
	}

	if rb.RbConfig.Input.Stats.Cpu != nil {
		rb.cpuStats = *rb.RbConfig.Input.Stats.Cpu
	} else {
		rb.cpuStats = true
	}

	if rb.RbConfig.Input.Stats.Commandstats != nil {
		rb.commandStats = *rb.RbConfig.Input.Stats.Commandstats
	} else {
		rb.commandStats = true
	}

	if rb.RbConfig.Input.Stats.Cluster != nil {
		rb.clusterStats = *rb.RbConfig.Input.Stats.Cluster
	} else {
		rb.clusterStats = true
	}

	if rb.RbConfig.Input.Stats.Keyspace != nil {
		rb.keyspaceStats = *rb.RbConfig.Input.Stats.Keyspace
	} else {
		rb.keyspaceStats = true
	}

	logp.Debug("redisbeat", "Init redisbeat")
	logp.Debug("redisbeat", "Period %v\n", rb.period)
	logp.Debug("redisbeat", "Host %v\n", rb.host)
	logp.Debug("redisbeat", "Port %v\n", rb.port)
	logp.Debug("redisbeat", "Network %v\n", rb.network)
	logp.Debug("redisbeat", "Max Connections %v\n", rb.maxConn)
	logp.Debug("redisbeat", "Auth %t\n", rb.auth)
	logp.Debug("redisbeat", "Server statistics %t\n", rb.serverStats)
	logp.Debug("redisbeat", "Client statistics %t\n", rb.clientsStats)
	logp.Debug("redisbeat", "Memory statistics %t\n", rb.memoryStats)
	logp.Debug("redisbeat", "Persistence statistics %t\n", rb.persistenceStats)
	logp.Debug("redisbeat", "Stats statistics %t\n", rb.statsStats)
	logp.Debug("redisbeat", "Replication statistics %t\n", rb.replicationStats)
	logp.Debug("redisbeat", "Cpu statistics %t\n", rb.cpuStats)
	logp.Debug("redisbeat", "Command statistics %t\n", rb.commandStats)
	logp.Debug("redisbeat", "Cluster statistics %t\n", rb.clusterStats)
	logp.Debug("redisbeat", "Keyspace statistics %t\n", rb.keyspaceStats)

	return nil
}

func (rb *Redisbeat) Setup(b *beat.Beat) error {
	rb.events = b.Events
	rb.done = make(chan struct{})

	// Set up redis pool
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial(rb.network, rb.host+":"+strconv.Itoa(rb.port))

		if err != nil {
			return nil, err
		}

		return c, err
	}, rb.maxConn)

	rb.redisPool = redisPool

	if rb.auth {
		c := rb.redisPool.Get()
		defer c.Close()

		authed, err := c.Do("AUTH", rb.pass)
		if err != nil {
			return err
		} else {
			logp.Debug("redisbeat", "AUTH %v", authed)
		}
	}

	return nil
}

func (r *Redisbeat) Run(b *beat.Beat) error {
	var err error

	ticker := time.NewTicker(r.period)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return nil
		case <-ticker.C:
		}

		timerStart := time.Now()

		if r.serverStats {
			err = r.exportStats("server")
			if err != nil {
				logp.Err("Error reading server stats: %v", err)
				break
			}
		}
		if r.clientsStats {
			err = r.exportStats("clients")
			if err != nil {
				logp.Err("Error reading clients stats: %v", err)
				break
			}
		}
		if r.memoryStats {
			err = r.exportStats("memory")
			if err != nil {
				logp.Err("Error reading memory stats: %v", err)
				break
			}
		}
		if r.persistenceStats {
			err = r.exportStats("persistence")
			if err != nil {
				logp.Err("Error reading persistence stats: %v", err)
				break
			}
		}
		if r.statsStats {
			err = r.exportStats("stats")
			if err != nil {
				logp.Err("Error reading stats stats: %v", err)
				break
			}
		}
		if r.replicationStats {
			err = r.exportStats("replication")
			if err != nil {
				logp.Err("Error reading replication stats: %v", err)
				break
			}
		}
		if r.cpuStats {
			err = r.exportStats("cpu")
			if err != nil {
				logp.Err("Error reading cpu stats: %v", err)
				break
			}
		}
		if r.commandStats {
			err = r.exportStats("commandstats")
			if err != nil {
				logp.Err("Error reading commandstats: %v", err)
				break
			}
		}
		if r.clusterStats {
			err = r.exportStats("cluster")
			if err != nil {
				logp.Err("Error reading cluster stats: %v", err)
				break
			}
		}
		if r.keyspaceStats {
			err = r.exportStats("keyspace")
			if err != nil {
				logp.Err("Error reading keypsace stats: %v", err)
				break
			}
		}

		timerEnd := time.Now()
		duration := timerEnd.Sub(timerStart)
		if duration.Nanoseconds() > r.period.Nanoseconds() {
			logp.Warn("Ignoring tick(s) due to processing taking longer than one period")
		}
	}

	return err
}

func (rb *Redisbeat) Cleanup(b *beat.Beat) error {
	// I wonder if the redis pool should released here, after the main loop exists.
	return nil
}

// Stop is triggered on exit, closing the done channel and redis pool
func (r *Redisbeat) Stop() {
	close(r.done)
	r.redisPool.Close()
}

func (r *Redisbeat) exportStats(statType string) error {
	stats, err := r.getInfoReply(statType)
	if err != nil {
		logp.Warn("Failed to fetch server stats: %v", err)
		return err
	}

	event := common.MapStr{
		"@timestamp": common.Time(time.Now()),
		"type":       statType,
		"count":      1,
		"stats":      stats,
	}

	r.events.PublishEvent(event)

	return nil
}

// getInfoReply sends INFO type command and returns the response as a map
func (r *Redisbeat) getInfoReply(infoType string) (map[string]string, error) {
	c := r.redisPool.Get()
	defer c.Close()
	reply, err := redis.Bytes(c.Do("INFO", infoType))

	if err != nil {
		return nil, err
	} else {
		s := string(reply[:])
		return convertReplyToMap(s)
	}
}

// convertReplyToMap converts a bulk string reply from Redis to a map
func convertReplyToMap(s string) (map[string]string, error) {
	var info map[string]string
	info = make(map[string]string)

	// Regex for INFO type property
	infoRegex := `^\s*#\s*\w+\s*$`
	r, err := regexp.Compile(infoRegex)
	if err != nil {
		return nil, errors.New("Regex failed to compile")
	}

	// http://redis.io/topics/protocol#bulk-string-reply
	a := strings.Split(s, "\r\n")

	for _, v := range a {
		if r.MatchString(v) || v == "" {
			logp.Debug("redisbeat", "Skipping reply string - \"%v\"", v)
			continue
		}
		entry := strings.Split(v, ":")
		logp.Debug("redisbeat", "Entry: %#v\n", entry)
		info[entry[0]] = entry[1]
	}
	return info, nil
}
