package main

import "flag"

var (
	kubemqHost = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	sendQueue  = flag.String("send-queue", "pipeline.stage.1", "set start pipeline queue name")
)

func main() {

}

func init() {
	flag.Parse()

}
