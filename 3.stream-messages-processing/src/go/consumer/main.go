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

type Message struct {
	ID   string
	Data string
}

var (
	kubemqHost     = flag.String("kubemq-host", "localhost", "set KubeMQ server gRPC host address")
	kubemqPort     = flag.Int("kubemq-port", 50000, "set KubeMQ server gRPC port")
	mongoDBUri     = flag.String("mongodb-uri", "mongodb://localhost:27017", "set mongodb uri")
	consumeChannel = flag.String("channel", "stream.channel", "set consumer stream channel name")
	consumeGroup   = flag.String("group", "", "set consumer group name")

	name = flag.String("name", "consumer-1", "set consumer name")
)

type MongoDBClient struct {
	client *mongo.Client
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

	// creating a consumer client
	worker, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(*kubemqHost, *kubemqPort),
		kubemq.WithClientId(*name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))

	if err != nil {
		log.Fatal(err)

	}
	defer worker.Close()

	log.Printf("consumer (%s) connected to KubeMQ, receiving messages from channel %s and consumer group %s. \n", *name, *consumeChannel, *consumeGroup)

	mongoClient, err := NewMongoDBClient(ctx, *mongoDBUri)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("consumer (%s) connected to MongoDB and ready to process messages. \n", *name)
	// starting consumer go routine
	go func() {
		// subscribe to event-store channel
		errChan := make(chan error, 1)
		msgChan, err := worker.SubscribeToEventsStore(ctx, *consumeChannel, *consumeGroup, errChan, kubemq.StartFromNewEvents())
		if err != nil {
			log.Fatalf("cannot receive stream messages, error: %s", err.Error())
			return
		}

		for {
			select {
			case streamMsg := <-msgChan:
				msg := &Message{}
				err := json.Unmarshal(streamMsg.Body, msg)
				if err != nil {
					log.Fatal(err)
				}
				err = mongoClient.Insert(ctx, "mydb", "stream-messages", msg)
				if err != nil {
					log.Printf("error on insert message to mongodb, error: %s\n", err.Error())
				}
			case err := <-errChan:
				log.Fatal(err)
			case <-ctx.Done():
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
