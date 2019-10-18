package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Message struct {
	Sequence     int
	TaskDuration time.Duration
}

var (
	kubemqHost    = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort    = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	incomingQueue = flag.String("incoming-queue", "pipeline.stage.1", "set incoming pipeline queue name")
	outgoingQueue = flag.String("outgoing-queue", "pipeline.stage.2", "set outgoing pipeline queue name")
	name          = flag.String("name", "processor-1", "set processor name")
)

func getMessageToProcess(data []byte) (*Message, error) {
	msg := &Message{}
	err := json.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// creating a processor client
	processor, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)

	}
	defer processor.Close()

	if *outgoingQueue != "" {
		log.Printf("receiver (%s) connected to KubeMQ, receiving messages from queue: %s , processing it and sending to %s. \n", *name, *incomingQueue, *outgoingQueue)
	} else {
		log.Printf("receiver (%s) connected to KubeMQ, receiving messages from queue: %s , processing it and ack the message. \n", *name, *incomingQueue)
	}

	// starting processor go routine
	go func() {

		for {
			// checking for context cancellation
			select {
			case <-ctx.Done():
				return

			default:

			}
			// subscribing to a stream of queue message
			stream := processor.NewStreamQueueMessage().SetChannel(*incomingQueue)
			// get message from the queue with 10 seconds visibility and 60 seconds of waiting time.
			queueMessage, err := stream.Next(ctx, 10, 60)
			if err != nil {
				// if we don't have messages to process then we continue and try again
				continue
			}

			// un-marshaling and checking validity of message
			msg, err := getMessageToProcess(queueMessage.Body)
			if err != nil {
				log.Printf("cannot process message %s, error: %s rejecting message\n", queueMessage.Id, err.Error())
				err := queueMessage.Reject()
				if err != nil {
					log.Fatalf("cannot reject a bad queue message, error: %s", err.Error())
					return
				}
				continue
			}

			log.Printf("processing message: %d at stage %s, time to process: %s", msg.Sequence, *incomingQueue, msg.TaskDuration)
			// simulating message process time
			time.Sleep(msg.TaskDuration)
			if *outgoingQueue == "" {
				err := queueMessage.Ack()
				if err != nil {
					log.Fatalf("cannot ack a queue message, error: %s", err.Error())
					return
				}
				continue
			}
			// resending the message to the next stage
			newQueueMessage := processor.NewQueueMessage().
				SetId(fmt.Sprintf("stage-%s.%d", *name, msg.Sequence)).
				SetChannel(*outgoingQueue).
				SetBody(queueMessage.Body)
			err = stream.ResendWithNewMessage(newQueueMessage)
			if err != nil {
				log.Fatalf("cannot relay a new queue message, error: %s", err.Error())
				return
			}

		}
	}()

	<-shutdown
	log.Println("shutdown processor completed")
}

func init() {
	flag.Parse()

}
