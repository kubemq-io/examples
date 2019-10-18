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
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type Task struct {
	ID   string
	Data string
}

var (
	kubemqHost     = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort     = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	sendQueue      = flag.String("send-queue", "task.queue", "set task queue channel name")
	sendTasksCount = flag.Int("count", 1, "set how many tasks to send")
	sendTaskLength = flag.Int64("length", 1000, "set size of task data in bytes")
	name           = flag.String("name", "producer-1", "set producer name")
)

func getRandomString(length int64) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func getTask(length int64) []byte {
	task := &Task{
		ID:   uuid.New().String(),
		Data: getRandomString(length),
	}
	data, err := json.Marshal(task)
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

	log.Printf("producer connected to KubeMQ, sending %d tasks with data lenght of %d bytes to queue: %s \n", *sendTasksCount, *sendTaskLength, *sendQueue)

	for i := 0; i < *sendTasksCount; i++ {
		sendResult, err := producer.NewQueueMessage().
			SetId(fmt.Sprintf("producer.%s.task-%d", *name, i)).
			SetChannel(*sendQueue).
			SetBody(getTask(*sendTaskLength)).
			Send(ctx)

		if err != nil {
			// if we have an error in transport layer we should log it and exit
			log.Fatalf("sending task %d failed, error: %s\n", i, err.Error())
		}
		// we might have an error in KubeMQ level
		if sendResult.IsError {
			log.Printf("task %d was not sent, result error: %s.\n", i, sendResult.Error)
		} else {
			log.Printf("task %d sent at %s.\n", i, time.Unix(0, sendResult.SentAt))
		}

	}
	log.Println("task producer completed")
}

func init() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

}
