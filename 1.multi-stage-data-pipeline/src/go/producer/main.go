package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const timeDurationUpRange = 5000

type Message struct {
	Sequence     int
	TaskDuration time.Duration
}

var (
	kubemqHost    = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort    = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	sendQueue     = flag.String("send-queue", "pipeline.stage.1", "set start pipeline queue name")
	sendIntervael = flag.Duration("interval", time.Duration(5*time.Second), "set sending interval duration")
	name          = flag.String("name", "producer-1", "set producer name")
)

func getNewMessage(cnt int) []byte {
	randMiliSec := rand.Intn(timeDurationUpRange)
	msg := &Message{
		Sequence:     cnt,
		TaskDuration: time.Millisecond * time.Duration(randMiliSec),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}
	return data
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	log.Printf("producer connected to KubeMQ, sending messages to queue: %s in intervals of %s \n", *sendQueue, *sendIntervael)

	// listening to CTRL-C
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		cnt := 0
		ticker := time.NewTicker(*sendIntervael)
		for {
			select {
			case <-ticker.C:
				cnt++
				// sending queue message
				sendResult, err := producer.NewQueueMessage().
					SetId(fmt.Sprintf("producer.%s.%d", *name, cnt)).
					SetChannel(*sendQueue).
					SetBody(getNewMessage(cnt)).
					Send(ctx)
				if err != nil {
					// if we have an error in transport layer we should log it and exit
					log.Fatalf("sending message %d failed, error: %s\n", cnt, err.Error())
				}
				// we might have an error in KubeMQ level
				if sendResult.IsError {
					log.Printf("message %d was not sent, result error: %s.\n", cnt, sendResult.Error)
				} else {
					log.Printf("message %d sent at %s.\n", cnt, time.Unix(0, sendResult.SentAt))
				}
			case <-ctx.Done():
				ticker.Stop()
				return
			}

		}
	}()

	<-shutdown
	log.Println("shutdown producer completed")
}

func init() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

}
