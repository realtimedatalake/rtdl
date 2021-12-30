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
	//"github.com/segmentio/kafka-go/snappy"
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
			kafka.Message{Value: body},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	})
}


func main() {
	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("KAFKA_URL")
	topic := os.Getenv("KAFKA_TOPIC")


	//defer kafkaWriter.Close()

	// Add handle func for producer.
	http.HandleFunc("/", producerHandler(kafkaURL, topic))

	// Run the web server.
	fmt.Println("start producer-api ... !!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// import (
// 	"fmt"
// 	"net"
// 	"net/http"
// 	"net/http/httputil"
// 	"strconv"

// 	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"

// 	//"encoding/json"
// )

// func processJSON(c *gin.Context) {
// 	var event EVENT

// 	reqByte, err := httputil.DumpRequest(c.Request, true)
// 	if err != nil {
// 		c.IndentedJSON(http.StatusBadRequest, "Error reading request")
// 	} else {
// 		err := c.BindJSON(&event)
// 		if err != nil {
// 			c.IndentedJSON(http.StatusBadRequest, "Error binding")
// 		} else {
// 			sendToPort(event.ID, reqByte)
// 			c.IndentedJSON(http.StatusOK, "POST received and forwarded to port "+strconv.Itoa(event.ID))
// 		}

// 		/* reqString := string(reqByte)
// 		if err := c.BindJSON(&event); err != nil {
// 			fmt.Println("Error binding:")
// 			fmt.Println(reqString)
// 			c.IndentedJSON(http.StatusBadRequest, "Error binding:\n"+reqString)
// 		} else {
// 			fmt.Println("POST received:" + event.ID)
// 			fmt.Println(reqString)
// 			c.IndentedJSON(http.StatusOK, "POST received: "+event.ID+"\n"+reqString)
// 		} */
// 	}
// }

// func sendToPort(port int, payload []byte) {
// 	// Use ports 49152-65535
// 	con, err := net.Dial("udp", "localhost:"+strconv.Itoa(port))
// 	if err != nil {
// 		fmt.Println("Error opening connection to port " + strconv.Itoa(port))
// 	} else {
// 		defer con.Close()

// 		_, err = con.Write(payload)
// 		if err != nil {
// 			fmt.Println("Sent to port " + strconv.Itoa(port) + " FAILURE, Payload: " + string(payload))
// 		} else {
// 			fmt.Println("Sent to port " + strconv.Itoa(port) + ", Payload SUCCESS: " + string(payload))
// 		}
// 	}
// }
