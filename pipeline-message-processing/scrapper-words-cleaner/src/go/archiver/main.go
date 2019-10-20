package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/kubemq-io/kubemq-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	kubemqHost    = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort    = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	incomingQueue = flag.String("queue-in", "archiver", "set pipeline archiver queue name")
	mongoDBUri    = flag.String("mongodb-uri", "mongodb://localhost:27017", "set mongodb uri")
	name          = flag.String("name", "archiver", "set consumer name")
)

type MongoDBClient struct {
	client *mongo.Client
}

type Message struct {
	URL     string
	Content []byte
}

func getMessage(data []byte) (*Message, error) {
	msg := &Message{}
	err := json.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func NewMongoDBClient(ctx context.Context, uri string) (*MongoDBClient, error) {
	mdb := &MongoDBClient{}

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		return nil, err
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	mdb.client = client
	return mdb, nil
}

func (c *MongoDBClient) Insert(ctx context.Context, db, col string, document interface{}) error {
	collection := c.client.Database(db).Collection(col)
	insertResult, err := collection.InsertOne(ctx, document)
	if err != nil {
		return err
	}
	log.Printf("mongodb: inserted a single document: %s\n ", insertResult.InsertedID)
	return nil
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	archiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)

	}
	defer archiver.Close()
	log.Printf("%s connected to KubeMQ, receiving messages from queue: %s and save them to mongodb. \n", *name, *incomingQueue)
	mongoClient, err := NewMongoDBClient(ctx, *mongoDBUri)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s connected to MongoDB and ready to save messages. \n", *name)
	go func() {

		// starting consumer go routine
		for {
			select {
			case <-ctx.Done():
				return

			default:

			}
			stream := archiver.NewStreamQueueMessage().SetChannel(*incomingQueue)
			// get message from the queue with 10 seconds visibility and 60 seconds of waiting time.
			queueMessage, err := stream.Next(ctx, 10, 60)
			if err != nil {
				// if we don't have messages to process then we continue and try again
				continue
			}

			msg, err := getMessage(queueMessage.Body)
			if err != nil {
				log.Printf("cannot process message %s, error: %s rejecting message\n", queueMessage.Id, err.Error())
				err := queueMessage.Reject()
				if err != nil {
					log.Fatalf("cannot reject a bad queue message, error: %s", err.Error())
					return
				}
				continue
			}
			log.Printf("Message receive, size is %d\n", len(msg.Content))
			err = mongoClient.Insert(ctx, "pipeline", "messages", msg)
			if err != nil {
				log.Printf("error on insert message to mongodb, error: %s\n", err.Error())
			}

			err = queueMessage.Ack()
			if err != nil {
				log.Fatalf("cannot ack a queue message, error: %s", err.Error())
			}
		}

	}()

	<-shutdown
	log.Println("shutdown archiver completed")
}

func init() {
	flag.Parse()

}
