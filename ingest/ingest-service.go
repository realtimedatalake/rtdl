package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"net/http/httputil"
)

type EVENT struct {
	ID string `json:"id" binding:"required"`
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
		log.Fatalln(err)
	} else {
		reqString := string(reqByte)
		if err := c.BindJSON(&event); err != nil {
			fmt.Println("Error binding:")
			fmt.Println(reqString)
			c.IndentedJSON(http.StatusBadRequest, "Error binding:\n"+reqString)
		} else {
			fmt.Println("POST received:" + event.ID)
			fmt.Println(reqString)
			c.IndentedJSON(http.StatusOK, "POST received: "+event.ID+"\n"+reqString)
		}
	}
}
