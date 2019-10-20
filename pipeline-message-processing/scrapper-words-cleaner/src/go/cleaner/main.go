package main

import (
	"context"
	"flag"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	kubemqHost    = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort    = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	incomingQueue = flag.String("queue-in", "cleaner", "set pipeline cleaner queue name")
	name          = flag.String("name", "cleaner-1", "set consumer name")
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	cleaner, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)

	}
	defer cleaner.Close()
	log.Printf("cleaner (%s) connected to KubeMQ, receiving messages from queue: %s, printing it and ack them. \n", *name, *incomingQueue)
	go func() {

		// starting consumer go routine
		for {
			select {
			case <-ctx.Done():
				return

			default:

			}
			stream := cleaner.NewStreamQueueMessage().SetChannel(*incomingQueue)
			// get message from the queue with 10 seconds visibility and 60 seconds of waiting time.
			queueMessage, err := stream.Next(ctx, 10, 60)
			if err != nil {
				// if we don't have messages to process then we continue and try again
				continue
			}
			log.Printf("message received with size %d, cleaned.", len(queueMessage.Body))
			err = queueMessage.Ack()
			if err != nil {
				log.Fatalf("cannot ack a queue message, error: %s", err.Error())

			}
			continue
		}

	}()

	<-shutdown
	log.Println("shutdown cleaner completed")
}

func init() {
	flag.Parse()
}
