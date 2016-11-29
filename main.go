package main

import (
	redisbeat "github.com/chrsblck/redisbeat/beat"
	"github.com/elastic/beats/libbeat/beat"

)

var Name = "redisbeat"
var Version = "0.0.1"

func main() {
	beat.Run(Name, Version, redisbeat.New())
}
