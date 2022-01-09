// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"net/http"
	"encoding/json"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

type IncomingMessage struct {

	SourceKey string `json:"source_key"`
	MessageType string `json:"message_type"`
	Payload map[string]interface{} `json:"payload"`
}

type Config struct {
	StreamId string `db:"stream_id"`
	AlternateStreamId string `db:"stream_ald_id"`
	Active bool `db:"active"`
	FileStoreTypeId int64 `db:"file_store_type_id"`
	Region string `db:"region"`
	BucketName string `db:"bucket_name"`
	FolderName string `db:"folder_name"`
	IamArn string `db:"iam_arn"`
	Credentials string `db:"credentials"`
}

var config Config


var (
	
	IngestTypeName     = statefun.TypeNameFrom("com.rtdl.sf/ingest")
	KafkaEgressTypeName = statefun.TypeNameFrom("com.rtdl.sf/egress")
	IncomingMessageType    = statefun.MakeJsonType(statefun.TypeNameFrom("com.rtdl.sf/IncomingMessage"))
)

// getEnv get key environment variable if exist otherwise return defalutValue
func getEnv(key, defaultValue string) string {
    value := os.Getenv(key)
    if len(value) == 0 {
        return defaultValue
    }
    return value
}

func getParquetDataType(dataType string) string {

	switch dataType {
		case "string":
			return `BYTE_ARRAY`
		case "int32":
			return `INT32`
		case "int64":
			return `INT64`
		case "int96":
			return `INT96`
		case "float32":
			return `FLOAT`
		case "float64":
			return `DOUBLE`

	}
	return ""

}

func generateSchema(payload map[string]interface{}, messageType string, jsonSchema string) string {

	if jsonSchema == "" {
		jsonSchema = `{"Tag": "name=` + messageType +`, repititiontype=REQUIRED",`
		jsonSchema += `"Fields": [`
	}
	
	

	for key,value := range(payload) {
	
		jsonSchema += `{"Tag": "name=` + key + `, type=`
		
		dataType := reflect.TypeOf(value).String()
		fmt.Println(fmt.Sprintf("%s is %s",key, dataType))
		
		
		
		if strings.HasPrefix(dataType, "map[string]interface") {
			
			jsonSchema += `MAP, repititiontype=REQUIRED", "Fields" : [`
			jsonSchema = generateSchema(value.(map[string]interface{}), messageType, jsonSchema)
			jsonSchema += `]},`
			
		} else if strings.HasPrefix(dataType, "[]interface") {
		
			jsonSchema += `LIST, repititiontype=REQUIRED", "Fields" : [`
			arrayItemDataType := reflect.TypeOf(value.([]interface{})[0]).String()
			if strings.HasPrefix(arrayItemDataType, "map[string]interface") {
				 
				jsonSchema = generateSchema(value.([]interface{})[0].(map[string]interface{}),messageType, jsonSchema)
				
			} else {
			
				jsonSchema += `{"Tag": "name=element, type=` + getParquetDataType(reflect.TypeOf(value.([]interface{})[0]).String())
				jsonSchema += `, repititiontype=REQUIRED"},`
			}
			jsonSchema += `]},`
		} else {
			
			jsonSchema += getParquetDataType(dataType)	
			jsonSchema += `, repititiontype=REQUIRED"},`
			
		}
		
		
		
		
	}
	
	return jsonSchema

}


func Ingest(ctx statefun.Context, message statefun.Message) error {
	var request IncomingMessage
	if err := message.As(IncomingMessageType, &request); err != nil {
		return fmt.Errorf("failed to deserialize incoming message: %w", err)
	}
	
	fmt.Println(generateSchema(request.Payload,request.MessageType, "")+"]}")

	payload, _ := json.Marshal(request.Payload)	
	

	ctx.SendEgress(statefun.KafkaEgressBuilder{
		Target: KafkaEgressTypeName,
		Topic:  "egress",
		Key:    "message",
		Value:  []byte(payload),
	})
	
	fmt.Println("egress message written")

	return nil
}

func getConfig() error {


	pghost := getEnv("POSTGRES_HOST","localhost")
	pgport, err := strconv.Atoi(getEnv("POSTGRES_PORT","5432"))
	if err != nil {
		pgport = 5432
	}
	pguser := getEnv("POSTGRES_USER","postgres")
	pgpassword := getEnv("POSTGRES_PASSWORD","postgres")
	pgdbname := getEnv("POSTGRES_DBNAME","postgres")
	
	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable",pguser, pgpassword, pghost, pgport, pgdbname)
	
	db, err := sqlx.Connect("postgres", dsn)
	
	if err != nil {
		fmt.Println("Failed to open a DB connection: ", err)
		return err
	}
	

	var configs []Config
	configSql := "SELECT * FROM streams"

	db.Select(&configs, configSql)
	if err != nil {
		fmt.Println("Failed to execute query: ", err)
		return err
	}
	
	defer db.Close()
	return nil
}

func main() {


	getConfig()
	builder := statefun.StatefulFunctionsBuilder()

					
	err := getConfig()
	
	if err!= nil {
		fmt.Println(err)
	}

	_ = builder.WithSpec(statefun.StatefulFunctionSpec{
		FunctionType: IngestTypeName,
		Function:     statefun.StatefulFunctionPointer(Ingest),
	})

	http.Handle("/statefun", builder.AsHandler())
	_ = http.ListenAndServe(":8082", nil)
}
