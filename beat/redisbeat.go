package beat

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/chrsblck/redisbeat/config"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/garyburd/redigo/redis"
)

type Redisbeat struct {
	period time.Duration
	config config.Config
	events publisher.Client

	redisPool *redis.Pool
	done      chan struct{}
}

func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	defaultConfig := config.DefaultConfig
	err := cfg.Unpack(&defaultConfig)
	if err != nil {
		return nil, fmt.Errorf("Error reading configuration file: %v", err)
	}

	rb := &Redisbeat{
		done:   make(chan struct{}),
		config: defaultConfig,
	}

	logp.Debug("redisbeat", "Redisbeat configuration:")
	logp.Debug("redisbeat", "Period %v", rb.config.Period)
	logp.Debug("redisbeat", "Host %v", rb.config.Host)
	logp.Debug("redisbeat", "Port %v", rb.config.Port)
	logp.Debug("redisbeat", "Network %v", rb.config.Network)
	logp.Debug("redisbeat", "Max Connections %v", rb.config.MaxConn)
	logp.Debug("redisbeat", "Auth %t", rb.config.Auth.Required)
	logp.Debug("redisbeat", "Server statistics %t", rb.config.Stats.Server)
	logp.Debug("redisbeat", "Client statistics %t", rb.config.Stats.Clients)
	logp.Debug("redisbeat", "Memory statistics %t", rb.config.Stats.Memory)
	logp.Debug("redisbeat", "Persistence statistics %t", rb.config.Stats.Persistence)
	logp.Debug("redisbeat", "Stats statistics %t", rb.config.Stats.Stats)
	logp.Debug("redisbeat", "Replication statistics %t", rb.config.Stats.Replication)
	logp.Debug("redisbeat", "Cpu statistics %t", rb.config.Stats.Cpu)
	logp.Debug("redisbeat", "Command statistics %t", rb.config.Stats.Commandstats)
	logp.Debug("redisbeat", "Cluster statistics %t", rb.config.Stats.Cluster)
	logp.Debug("redisbeat", "Keyspace statistics %t", rb.config.Stats.Keyspace)

	return rb, nil
}

func (rb *Redisbeat) setup(b *beat.Beat) error {
	rb.events = b.Publisher.Connect()
	rb.done = make(chan struct{})

	// Set up redis pool
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial(rb.config.Network, rb.config.Host+":"+strconv.Itoa(rb.config.Port))

		if err != nil {
			return nil, err
		}

		return c, err
	}, rb.config.MaxConn)

	rb.redisPool = redisPool

	if rb.config.Auth.Required {
		c := rb.redisPool.Get()
		defer c.Close()

		authed, err := c.Do("AUTH", rb.config.Auth.RequiredPass)
		if err != nil {
			return err
		} else {
			logp.Debug("redisbeat", "AUTH %v", authed)
		}
	}

	return nil
}

func (rb *Redisbeat) Run(b *beat.Beat) error {
	var err error

	rb.setup(b)

	ticker := time.NewTicker(rb.config.Period)
	defer ticker.Stop()

	for {
		select {
		case <-rb.done:
			return nil
		case <-ticker.C:
		}

		timerStart := time.Now()

		if rb.config.Stats.Server {
			err = rb.exportStats("server")
			if err != nil {
				logp.Err("Error reading server stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Clients {
			err = rb.exportStats("clients")
			if err != nil {
				logp.Err("Error reading clients stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Memory {
			err = rb.exportStats("memory")
			if err != nil {
				logp.Err("Error reading memory stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Persistence {
			err = rb.exportStats("persistence")
			if err != nil {
				logp.Err("Error reading persistence stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Stats {
			err = rb.exportStats("stats")
			if err != nil {
				logp.Err("Error reading stats stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Replication {
			err = rb.exportStats("replication")
			if err != nil {
				logp.Err("Error reading replication stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Cpu {
			err = rb.exportStats("cpu")
			if err != nil {
				logp.Err("Error reading cpu stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Commandstats {
			err = rb.exportStats("commandstats")
			if err != nil {
				logp.Err("Error reading commandstats: %v", err)
				break
			}
		}
		if rb.config.Stats.Cluster {
			err = rb.exportStats("cluster")
			if err != nil {
				logp.Err("Error reading cluster stats: %v", err)
				break
			}
		}
		if rb.config.Stats.Keyspace {
			err = rb.exportStats("keyspace")
			if err != nil {
				logp.Err("Error reading keypsace stats: %v", err)
				break
			}
		}

		timerEnd := time.Now()
		duration := timerEnd.Sub(timerStart)
		if duration.Nanoseconds() > rb.period.Nanoseconds() {
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
func (rb *Redisbeat) Stop() {
	close(rb.done)
	rb.redisPool.Close()
}

func (rb *Redisbeat) exportStats(statType string) error {
	stats, err := rb.getInfoReply(statType)
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

	rb.events.PublishEvent(event)

	return nil
}

// getInfoReply sends INFO type command and returns the response as a map
func (rb *Redisbeat) getInfoReply(infoType string) (map[string]string, error) {
	c := rb.redisPool.Get()
	defer c.Close()

	if rb.config.Auth.Required {
		authed, err := c.Do("AUTH", rb.config.Auth.RequiredPass)
		if err != nil {
			return nil, err
		} else {
			logp.Debug("redisbeat", "AUTH %v", authed)
		}
	}

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
