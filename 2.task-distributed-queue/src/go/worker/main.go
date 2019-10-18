package main

import (
	"context"
	"encoding/json"
	"flag"
	"strings"

	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Task struct {
	ID   string
	Data string
}

var (
	kubemqHost = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	taskQueue  = flag.String("task-queue", "task.queue", "set task queue name")
	taskChar   = flag.String("count-string", "a", "set the string for counting")
	name       = flag.String("name", "worker-1", "set worker name")
)

func processTask(data []byte) error {
	task := &Task{}
	err := json.Unmarshal(data, task)
	if err != nil {
		return err
	}
	log.Printf("process task id: %s, count of string '%s' is %d ", task.ID, *taskChar, strings.Count(string(task.Data), *taskChar))
	return nil
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// creating a worker client
	worker, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)

	}
	defer worker.Close()

	log.Printf("worker (%s) connected to KubeMQ, receiving tasks from queue: %s. \n", *name, *taskQueue)

	// starting worker go routine
	go func() {

		for {
			// checking for context cancellation
			select {
			case <-ctx.Done():
				return

			default:

			}
			// getting 1 message from the queue
			msgs, err := worker.ReceiveQueueMessages(ctx,
				&kubemq.ReceiveQueueMessagesRequest{
					RequestID:           uuid.New().String(),
					ClientID:            *name,
					Channel:             *taskQueue,
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     60,
				})

			if err != nil {
				log.Fatalf("cannot receive queue messages, error: %s", err.Error())
				return
			}

			// sending message to process as a task
			for _, msg := range msgs.Messages {
				err := processTask(msg.Body)
				if err != nil {
					log.Fatalf("cannot process task, error: %s", err.Error())
				}
			}
		}
	}()
	<-shutdown
	log.Println("shutdown worker completed")
}

func init() {
	flag.Parse()

}
