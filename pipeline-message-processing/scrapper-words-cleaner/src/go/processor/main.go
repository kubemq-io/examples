package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Message struct {
	URL     string
	Content []byte
}

var (
	kubemqHost    = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort    = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	incomingQueue = flag.String("queue-in", "stage.1", "set incoming pipeline queue name")
	outgoingQueue = flag.String("queue-out", "stage.2", "set outgoing pipeline queue name")
	word          = flag.String("word", "Go", "set search and remove word")
	name          = flag.String("name", "processor", "set processor name")
	maxSize       = flag.Int("max-size", 1000000, "set processing max size of content")
)

func getMessage(data []byte) (*Message, error) {
	msg := &Message{}
	err := json.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func processMessage(data []byte, word string) ([]byte, int) {
	content := string(data)
	count := strings.Count(content, word)
	if count == 0 {
		return data, 0
	}

	cleanedContent := strings.Replace(content, word, "", -1)
	return []byte(cleanedContent), count
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// creating a processor client
	processor, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(fmt.Sprintf("%s.%s", *name, *incomingQueue)),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)

	}
	defer processor.Close()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	if *outgoingQueue != "" {
		log.Printf("Connected to KubeMQ, receiving messages from queue: %s processing it and sending to %s. \n", *incomingQueue, *outgoingQueue)
	} else {
		log.Printf("Connected to KubeMQ, receiving messages from queue: %s processing it and ack the message. \n", *incomingQueue)
	}

	// starting consumer go routine
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
			msg, err := getMessage(queueMessage.Body)
			if err != nil {
				log.Printf("Cannot process message %s, error: %s rejecting message\n", queueMessage.Id, err.Error())
				err := queueMessage.Reject()
				if err != nil {
					log.Fatalf("Cannot reject a bad queue message, error: %s", err.Error())
					return
				}
				continue
			}

			if len(msg.Content) > *maxSize {
				log.Printf("Content from URL %s received with a size of %d  but exceeded max size of %d bytes, rejecting the message and sending back to the queue", msg.URL, len(msg.Content), *maxSize)
				err := queueMessage.Reject()
				if err != nil {
					log.Fatalf("Cannot reject a bad queue message, error: %s", err.Error())
					return
				}
				continue
			}
			log.Printf("Content from URL %s received, process cleaning of word '%s'...", msg.URL, *word)
			newContent, n := processMessage(msg.Content, *word)

			log.Printf("Processing completed, found %d times.\n", n)

			if *outgoingQueue == "" {
				err := queueMessage.Ack()
				if err != nil {
					log.Fatalf("Cannot ack a queue message, error: %s", err.Error())
					return
				}
				continue
			}

			newMessage := &Message{
				URL:     msg.URL,
				Content: newContent,
			}
			buff, err := json.Marshal(newMessage)
			if err != nil {
				log.Fatal(err)
			}

			// resending the message to the next stage
			newQueueMessage := processor.NewQueueMessage().
				SetId(fmt.Sprintf("%s.%s", queueMessage.Id, uuid.New().String())).
				SetChannel(*outgoingQueue).
				SetBody(buff)
			err = stream.ResendWithNewMessage(newQueueMessage)
			if err != nil {
				log.Fatalf("cannot relay a new queue message, error: %s", err.Error())
				return
			}
		}
	}()

	<-shutdown
	log.Println("shutdown consumer completed")
}

func init() {
	flag.Parse()

}
