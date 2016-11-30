package beat

import (
	"testing"
	"time"

	beat "github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	conf, err := common.LoadFile("../redisbeat.yml")
	if err != nil {
		t.Errorf("Load file failed %v", err)
	}

	b := &beat.Beat{}
	rrb, err := New(b, conf)
	rb, _ := rrb.(*Redisbeat)
	assert.Nil(t, err)

	assert.Equal(t, DEFAULT_PERIOD, rb.period, "Default time period should be %v", DEFAULT_PERIOD)
	assert.Equal(t, DEFAULT_HOST, rb.host, "Default host should be %v", DEFAULT_HOST)
	assert.Equal(t, DEFAULT_PORT, rb.port, "Default port should be %v", DEFAULT_PORT)
	assert.Equal(t, DEFAULT_NETWORK, rb.network, "Default network should be %v", DEFAULT_NETWORK)
	assert.Equal(t, DEFAULT_MAX_CONN, rb.maxConn, "Default max connections should be %v", DEFAULT_MAX_CONN)
	assert.Equal(t, DEFAULT_AUTH_REQUIRED, rb.auth, "Default auth required should be %v", DEFAULT_AUTH_REQUIRED)
	assert.Equal(t, DEFAULT_AUTH_REQUIRED_PASS, rb.pass, "Default auth required pass should be %v", DEFAULT_AUTH_REQUIRED_PASS)
	assert.Equal(t, DEFAULT_STATS_SERVER, rb.serverStats, "Default server stats should be %v", DEFAULT_STATS_SERVER)
	assert.Equal(t, DEFAULT_STATS_CLIENT, rb.clientsStats, "Default client stats should be %v", DEFAULT_STATS_CLIENT)
	assert.Equal(t, DEFAULT_STATS_MEMORY, rb.memoryStats, "Default memory stats should be %v", DEFAULT_STATS_MEMORY)
	assert.Equal(t, DEFAULT_STATS_PERSISTENCE, rb.persistenceStats, "Default persistence stats should be %v", DEFAULT_STATS_PERSISTENCE)
	assert.Equal(t, DEFAULT_STATS_STATS, rb.statsStats, "Default stats stats should be %v", DEFAULT_STATS_STATS)
	assert.Equal(t, DEFAULT_STATS_REPLICATION, rb.replicationStats, "Default replication stats should be %v", DEFAULT_STATS_REPLICATION)
	assert.Equal(t, DEFAULT_STATS_CPU, rb.cpuStats, "Default cpu stats should be %v", DEFAULT_STATS_CPU)
	assert.Equal(t, DEFAULT_STATS_COMMAND, rb.commandStats, "Default command stats should be %v", DEFAULT_STATS_COMMAND)
	assert.Equal(t, DEFAULT_STATS_CLUSTER, rb.clusterStats, "Default cluster stats should be %v", DEFAULT_STATS_CLUSTER)
	assert.Equal(t, DEFAULT_STATS_KEYSPACE, rb.keyspaceStats, "Default keyspace stats should be %v", DEFAULT_STATS_KEYSPACE)
}

func TestModifiedConfig(t *testing.T) {
	conf, err := common.LoadFile("../tests/redisbeat.yml")
	if err != nil {
		t.Fatalf("Load file failed %v", err)
	}

	b := &beat.Beat{}
	rrb, err := New(b, conf)
	rb, _ := rrb.(*Redisbeat)
	assert.Nil(t, err)

	expectedTime := 5 * time.Second
	assert.Equal(t, expectedTime, rb.period, "Configured time period should be %v", expectedTime)
	assert.Equal(t, "redis.testing.fake", rb.host, "Configured host should be %v", "redis.testing.fake")
	assert.Equal(t, 9736, rb.port, "Configured port should be %v", 9736)
	assert.Equal(t, "udp", rb.network, "Configured network should be %v", "udp")
	assert.Equal(t, 5, rb.maxConn, "Configured max connections should be %v", 5)
	assert.Equal(t, true, rb.auth, "Configured auth required should be %v", true)
	assert.Equal(t, "p@ssw0rd", rb.pass, "Configured auth required pass should be %v", "p@ssw0rd")
	assert.Equal(t, true, rb.serverStats, "Configured server stats should be %v", true)
	assert.Equal(t, false, rb.clientsStats, "Configured client stats should be %v", false)
	assert.Equal(t, false, rb.memoryStats, "Configured memory stats should be %v", false)
	assert.Equal(t, false, rb.persistenceStats, "Configured persistence stats should be %v", false)
	assert.Equal(t, false, rb.statsStats, "Configured stats stats should be %v", false)
	assert.Equal(t, false, rb.replicationStats, "Configured replication stats should be %v", false)
	assert.Equal(t, false, rb.cpuStats, "Configured cpu stats should be %v", false)
	assert.Equal(t, false, rb.commandStats, "Configured command stats should be %v", false)
	assert.Equal(t, false, rb.clusterStats, "Configured cluster stats should be %v", false)
	assert.Equal(t, false, rb.keyspaceStats, "Configured keyspace stats should be %v", false)
}

func TestConvertReplyToMap(t *testing.T) {
	testReplyString := "# Server\r\nredis_version:3.0.0\r\nredis_mode:standalone\r\nmultiplexing_api:epoll\r\n"
	replyMap, err := convertReplyToMap(testReplyString)
	assert.Nil(t, err, "Valid string reply should not throw an error")
	assert.Equal(t, "3.0.0", replyMap["redis_version"], "Redis version should be 3.0.0")
	assert.Equal(t, "standalone", replyMap["redis_mode"], "Redis mode should be standalone")
	assert.Equal(t, "epoll", replyMap["multiplexing_api"], "Redis multiplexing api should be epoll")
}
