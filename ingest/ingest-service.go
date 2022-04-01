package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type OutgoingMessage struct {
	StreamId    string                 `json:"stream_id,omitempty"`
	StreamAltId string                 `json:"stream_alt_id,omitempty"`
	MessageType string                 `json:"message_type,omitempty"`
	ProjectId   string                 `json:"projectId,omitempty"`
	Payload     map[string]interface{} `json:"payload"`
}

// GetEnv get key environment variable if exist otherwise return defalutValue
func GetEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

//handler function for incoming REST calls
//based on processingType - either payload is passed on as-is to Kafka or
//specific message, asking stateful function to reload configuration cache, is put on Kafka
func producerHandler(kafkaURL string, topic string, processingType string) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {

		var body []byte
		var err error

		//normal ingestion request
		if processingType == "ingest" {
			body, err = ioutil.ReadAll(req.Body)
			if err != nil {
				log.Println(err)
				return
			}

			log.Println("Received : ", string(body))
			outgoingMessage := new(OutgoingMessage)

			//first need to study message to check if it has stream_id or writeKey. one is necessary
			var message map[string]interface{}

			err2 := json.Unmarshal(body, &message)

			if err2 != nil {
				log.Println(err2)
				return
			}

			if message["projectId"] == nil {
				if message["writeKey"] != nil { //no writeKey

					outgoingMessage.StreamAltId = message["writeKey"].(string) //put writKey to stream_alt_id
				}

			} else {

				outgoingMessage.StreamAltId = message["projectId"].(string) //put projectId to stream_alt_id

			}

			if message["stream_id"] != nil { //no stream_id

				outgoingMessage.StreamId = message["stream_id"].(string)
			}

			if message["type"] != nil { //use type from message

				outgoingMessage.MessageType = message["type"].(string)

			}

			//finally put the original message inside payload
			outgoingMessage.Payload = message

			//and create json
			body, err = json.Marshal(outgoingMessage)

			if err != nil {

				log.Println(err)

			}

		} else { //cache refresh request

			body = []byte(`{"stream_id":"","message_type":"rtdl_205","payload":{}}`)

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
	log.Fatal(http.ListenAndServe(":"+GetEnv("LISTENER_PORT", "8080"), nil))
}
