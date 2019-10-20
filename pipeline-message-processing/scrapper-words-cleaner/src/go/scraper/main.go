package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type Message struct {
	URL     string
	Content []byte
}

var (
	kubemqHost = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	sendQueue  = flag.String("queue-out", "stage.1", "set start pipeline queue name")
	url        = flag.String("url", "https://en.wikipedia.org/wiki/Go_(programming_language)", "set url")
	name       = flag.String("name", "scraper", "set scraper name")
	deadQueue  = flag.String("dead-letter-queue", "", "set dead-letter queue")
)

func getDoc(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var data []byte
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		return bodyBytes, nil
	} else {
		return nil, fmt.Errorf("error: %s", resp.Status)
	}
	return data, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scraper, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)
	}
	defer scraper.Close()

	log.Printf("Scraper connected to KubeMQ, sending messages to queue: %s \n", *sendQueue)

	log.Printf("Getting page from %s ...\n", *url)
	data, err := getDoc(*url)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Scraping completed. Content size is %d bytes, sending to process.\n", len(data))
	msg := &Message{
		URL:     *url,
		Content: data,
	}
	buff, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	queueMessage := scraper.NewQueueMessage().
		SetId(uuid.New().String()).
		SetChannel(*sendQueue).
		SetBody(buff)

	if *deadQueue != "" {
		queueMessage = queueMessage.
			SetPolicyMaxReceiveQueue(*deadQueue).
			SetPolicyMaxReceiveCount(1)
	}

	sendResult, err := queueMessage.Send(ctx)

	if err != nil {
		// if we have an error in transport layer we should log it and exit
		log.Fatalf("sending message failed, error: %s\n", err.Error())
	}
	// we might have an error in KubeMQ level
	if sendResult.IsError {
		log.Printf("message was not sent, result error: %s.\n", sendResult.Error)
	} else {
		log.Printf("message sent to procssing at %s.\n", time.Unix(0, sendResult.SentAt))
	}

}

func init() {
	flag.Parse()
}
