//stateful function that reads messages from Kafka topic and writes to data lake
//event messages are put on to Kafka by the ingest REST endpoint

package main

import (
	"os"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"net/http"
	"encoding/json"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	
)


//Incoming message would have 
// - a source key to identify the stream
// - a message type that can be used to indicate the message purpose
// - generic payload

type IncomingMessage struct {

	SourceKey string `json:"source_key"`
	MessageType string `json:"message_type"`
	Payload map[string]interface{} `json:"payload"`
}


//struct representation of stream configuration
// AlternateStreamId is applicable where the stream is being fed from an external system and the alternate id 
// represents the unique identifier for that system
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

var configs []Config

//name variables for stateful function
var (
	
	IngestTypeName     = statefun.TypeNameFrom("com.rtdl.sf/ingest")
	KafkaEgressTypeName = statefun.TypeNameFrom("com.rtdl.sf/egress")
	IncomingMessageType    = statefun.MakeJsonType(statefun.TypeNameFrom("com.rtdl.sf/IncomingMessage"))
)


//loads all stream configurations 
func loadConfig() error {

	//directly load data from PostgreSQL
	//connection parameters read from docker-compose.yml, defaults mentioned here
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
		log.Println("Failed to open a DB connection: ", err)
		return err
	}
	

	configSql := "SELECT * FROM streams"

	db.Select(&configs, configSql) //populate stream configurations into array of stream config structs
	if err != nil {
		log.Println("Failed to execute query: ", err)
		return err
	}
	
	defer db.Close()
	log.Println("No. of config records retrieved : " + strconv.Itoa(len(configs)))
	return nil
}



// getEnv get key environment variable if exist otherwise return defalutValue
func getEnv(key, defaultValue string) string {
    value := os.Getenv(key)
    if len(value) == 0 {
        return defaultValue
    }
    return value
}

//map between Go and Parquet data types
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

//use reflection to study incoming generic payload and construct schema necessary for Parquet
//payload is passed recursively through the function to break down till the elemental level

func generateSchema(payload map[string]interface{}, messageType string, jsonSchema string) string {

	if jsonSchema == "" {
		jsonSchema = `{"Tag": "name=` + messageType +`, repetitiontype=REQUIRED",`
		jsonSchema += `"Fields": [`
	}
	
	

	for key,value := range(payload) {
	
		jsonSchema += `{"Tag": "name=` + key 
		
		dataType := reflect.TypeOf(value).String()
		//log.Println(fmt.Sprintf("%s is %s",key, dataType))
		
		
		//special processing for nested object structures
		if strings.HasPrefix(dataType, "map[string]interface") {
			
			jsonSchema += `, repetitiontype=REQUIRED", "Fields" : [`
			jsonSchema = generateSchema(value.(map[string]interface{}), messageType, jsonSchema) //need recursion
			jsonSchema = strings.TrimRight(jsonSchema,",") //remove trailing comma
			jsonSchema += `]},`
			
		} else if strings.HasPrefix(dataType, "[]interface") { //special processing for arrays as well
		
			jsonSchema += `, type=LIST, repetitiontype=REQUIRED", "Fields" : [`
			arrayItemDataType := reflect.TypeOf(value.([]interface{})[0]).String()
			if strings.HasPrefix(arrayItemDataType, "map[string]interface") { //if array consists of objects then same have to be recursed
				 
				jsonSchema = generateSchema(value.([]interface{})[0].(map[string]interface{}),messageType, jsonSchema)
				
			} else { //arrays composed of native data types can be handled directly
			
				jsonSchema += `{"Tag": "name=element, type=` + getParquetDataType(reflect.TypeOf(value.([]interface{})[0]).String())
				jsonSchema += `, repetitiontype=REQUIRED"},`
			}
			jsonSchema = strings.TrimRight(jsonSchema,",")
			jsonSchema += `]},`
		} else { //native data type
			
			jsonSchema += `, type=` + getParquetDataType(dataType)	
			jsonSchema += `, repetitiontype=REQUIRED"},`
			
		}
		
		
	}
	
	return jsonSchema

}


//main stateful function
func Ingest(ctx statefun.Context, message statefun.Message) error {
	var request IncomingMessage
	if err := message.As(IncomingMessageType, &request); err != nil {
		return fmt.Errorf("failed to deserialize incoming message: %w", err)
	}
	
	if request.MessageType == "rtdl_205" { //this is internal message for refershing configuration cache
	
		err := loadConfig()
	
		if err!= nil {
			log.Println(err)
			return err
		}

		return nil
	}
	//log.Println(generateSchema(request.Payload,request.MessageType, "")+"]}")
	
	schema := strings.TrimRight(generateSchema(request.Payload,request.MessageType, ""),",")+"]}"
	
	
	
	log.Println(schema)

	payload, _ := json.Marshal(request.Payload)	//convert generic payload structure to JSON string
	
	//write
	fw, err := local.NewLocalFileWriter("json.parquet")
	
	
	if err != nil {
		log.Println("Can't create file", err)
		return err
	}
	
	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		log.Println("Can't create json writer", err)
		return err
	}
	
	if err = pw.Write(payload); err != nil {
			log.Println("Write error", err)
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("Write Finished")
	fw.Close()
	
	//initial implementation to test out data flow
	//not required once actual Parquet writing logic has been implemented
	ctx.SendEgress(statefun.KafkaEgressBuilder{
		Target: KafkaEgressTypeName,
		Topic:  "egress",
		Key:    "message",
		Value:  []byte(payload),
	})
	
	log.Println("egress message written")

	return nil
}



func main() {

	//load configuration at the outset
	//should panic if unable to do source
	err := loadConfig()
	
		
	if err!= nil {
		panic(err)
	}

	
	builder := statefun.StatefulFunctionsBuilder()					
	
	//only the one function in the chain now
	_ = builder.WithSpec(statefun.StatefulFunctionSpec{
		FunctionType: IngestTypeName,
		Function:     statefun.StatefulFunctionPointer(Ingest),
	})

	http.Handle("/statefun", builder.AsHandler())
	_ = http.ListenAndServe(":8082", nil)
}
