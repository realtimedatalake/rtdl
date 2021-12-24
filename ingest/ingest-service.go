package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"

	//"encoding/json"
	"github.com/gin-gonic/gin"
)

type EVENT struct {
	ID int `json:"id" binding:"required"`
}

func main() {
	router := gin.Default()
	router.POST("/ingest", processJSON)

	router.Run(":8080")
}

func processJSON(c *gin.Context) {
	var event EVENT

	reqByte, err := httputil.DumpRequest(c.Request, true)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, "Error reading request")
	} else {
		err := c.BindJSON(&event)
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, "Error binding")
		} else {
			sendToPort(event.ID, reqByte)
			c.IndentedJSON(http.StatusOK, "POST received and forwarded to port "+strconv.Itoa(event.ID))
		}

		/* reqString := string(reqByte)
		if err := c.BindJSON(&event); err != nil {
			fmt.Println("Error binding:")
			fmt.Println(reqString)
			c.IndentedJSON(http.StatusBadRequest, "Error binding:\n"+reqString)
		} else {
			fmt.Println("POST received:" + event.ID)
			fmt.Println(reqString)
			c.IndentedJSON(http.StatusOK, "POST received: "+event.ID+"\n"+reqString)
		} */
	}
}

func sendToPort(port int, payload []byte) {
	// Use ports 49152-65535
	con, err := net.Dial("udp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error opening connection to port " + strconv.Itoa(port))
	} else {
		defer con.Close()

		_, err = con.Write(payload)
		if err != nil {
			fmt.Println("Sent to port " + strconv.Itoa(port) + " FAILURE, Payload: " + string(payload))
		} else {
			fmt.Println("Sent to port " + strconv.Itoa(port) + ", Payload SUCCESS: " + string(payload))
		}
	}
}
