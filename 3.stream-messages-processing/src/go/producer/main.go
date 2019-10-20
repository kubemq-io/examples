package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type Message struct {
	ID   string
	Data string
}

var (
	kubemqHost        = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort        = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	streamChannel     = flag.String("channel", "stream.channel", "set stream channel name")
	streamWorkerCount = flag.Int("count", 1, "set how many concurrent streams")
	streamInterval    = flag.Duration("interval", time.Second*5, "set message interval duration")
	name              = flag.String("name", "scraper-1", "set scraper name")
)

func getRandomString(length int64) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func getMessage(length int64) []byte {
	message := &Message{
		ID:   uuid.New().String(),
		Data: getRandomString(length),
	}
	data, err := json.Marshal(message)
	if err != nil {
		log.Fatal(err)
	}
	return data

}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// listening to CTRL-C
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	for i := 1; i <= *streamWorkerCount; i++ {
		go func(id int) {
			producerName := fmt.Sprintf("%s-%d", *name, id)
			producer, err := kubemq.NewClient(ctx,
				kubemq.WithAddress(*kubemqHost, *kubemqPort),
				kubemq.WithClientId(producerName),
				kubemq.WithTransportType(kubemq.TransportTypeGRPC))

			if err != nil {
				log.Fatal(err)
			}
			defer producer.Close()

			log.Printf("scraper %s connected to KubeMQ, sending messages to %s channel with inteval of %s.\n", producerName, *streamChannel, *streamInterval)

			cnt := 0
			ticker := time.NewTicker(*streamInterval)
			for {
				select {
				case <-ticker.C:
					cnt++
					// sending stream message
					sendResult, err := producer.NewEventStore().
						SetId(fmt.Sprintf("%s.%d", producerName, cnt)).
						SetChannel(*streamChannel).
						SetBody(getMessage(100)).
						Send(ctx)
					if err != nil {
						// if we have an error in transport layer we should log it and exit
						log.Fatalf("sending message %d failed, error: %s\n", cnt, err.Error())
					}

					// we might have an error in KubeMQ level
					if sendResult.Err != nil {
						log.Printf("%s didn't sned message %d , result error: %s.\n", producerName, cnt, sendResult.Err.Error())
					} else {
						log.Printf("%s sent message %d succefully", producerName, cnt)
					}
				case <-ctx.Done():
					ticker.Stop()
					return
				}

			}

		}(i)

	}
	<-shutdown
	log.Println("shutdown scraper completed")
}

func init() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

}
