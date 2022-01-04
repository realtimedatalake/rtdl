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

func producerHandler(kafkaURL string, topic string) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}
		
	
		// to produce messages
		partition := 0

		conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
		}
		
		
		conn.SetWriteDeadline(time.Now().Add(10*time.Second))
		_, err = conn.WriteMessages(
			kafka.Message{
				Key: []byte("message"),
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
	http.HandleFunc("/ingest", producerHandler(kafkaURL, topic))

	// Run the web server.
	fmt.Println("start producer-api ... !!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

