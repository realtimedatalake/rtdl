package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

//handler function for incoming REST calls
//based on processingType - either payload is passed on as-is to Kafka or 
//specific message, asking stateful function to reload configuration cache, is put on Kafka 
func producerHandler(kafkaURL string, topic string, processingType string) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {

		var body []byte
		var err1 error

		//normal ingestion request
		if processingType == "ingest" {
			body, err1 = ioutil.ReadAll(req.Body)
			if err1 != nil {
				log.Fatalln(err1)
			}

		} else { //cache refresh request

			body = []byte(`{"source_key":"","message_type":"rtdl_205","payload":{}}`)

		}

		// to produce messages
		partition := 0

		conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
		}

		conn.SetWriteDeadline(time.Now().Add(10 * time.Second)) //10 seconds timeout
		_, err = conn.WriteMessages(
			kafka.Message{
				Key:   []byte("message"),
				Value: body,
			},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}

		fmt.Println("message written")
	})
}

func main() {

	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("KAFKA_URL")
	topic := os.Getenv("KAFKA_TOPIC")

	//defer kafkaWriter.Close()

	// Add handle func for producer.
	http.HandleFunc("/ingest", producerHandler(kafkaURL, topic, "ingest"))

	http.HandleFunc("/refreshCache", producerHandler(kafkaURL, topic, "refresh-cache"))

	// Run the web server.
	log.Fatal(http.ListenAndServe(":8080", nil))
}
