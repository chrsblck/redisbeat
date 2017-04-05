package beat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertReplyToMap(t *testing.T) {
	testReplyString := "# Server\r\nredis_version:3.0.0\r\nredis_mode:standalone\r\nmultiplexing_api:epoll\r\n"
	replyMap, err := convertReplyToMap(testReplyString)
	assert.Nil(t, err, "Valid string reply should not throw an error")
	assert.Equal(t, "3.0.0", replyMap["redis_version"], "Redis version should be 3.0.0")
	assert.Equal(t, "standalone", replyMap["redis_mode"], "Redis mode should be standalone")
	assert.Equal(t, "epoll", replyMap["multiplexing_api"], "Redis multiplexing api should be epoll")
}
