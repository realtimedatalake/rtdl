package main

import (
    "net/http"
    "github.com/gin-gonic/gin"
)

type EVENT struct{
    ID string `json:"id" binding:"required"`
    BODY string `json:"body" binding:"required"`
}

func main() {
    router := gin.Default()
	route.POST("/ingest", processJSON)

    router.Run(":8080")
}

func processJSON(c *gin.Context) {
    var event EVENT
    c.BindJSON(&event)
	c.IndentedJSON(http.StatusOK, "POST received: " + event.ID)
}