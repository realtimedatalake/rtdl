package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"github.com/creamdog/gonfig"

	kafka "github.com/segmentio/kafka-go"
)

type OutgoingMessage struct {
	StreamId    string                 `json:"stream_id,omitempty"`
	StreamAltId string                 `json:"stream_alt_id,omitempty"`
	MessageType string                 `json:"message_type,omitempty"`
	ProjectId   string                 `json:"projectId,omitempty"`
	Payload     map[string]interface{} `json:"payload"`
}

//struct representation of stream configuration
// StreamAltId is applicable where the stream is being fed from an external system and the alternate id
// represents the unique identifier for that system
type Config struct {
	StreamId                sql.NullString `db:"stream_id" default:""`
	StreamAltId             sql.NullString `db:"stream_alt_id" default:""`
	Active                  sql.NullBool   `db:"active"`
	MessageType             sql.NullString `db:"message_type" default:""`
	FileStoreTypeId         sql.NullInt64  `db:"file_store_type_id"`
	Region                  sql.NullString `db:"region" default:""`
	BucketName              sql.NullString `db:"bucket_name" default:""`
	FolderName              sql.NullString `db:"folder_name" default:""`
	PartitionTimeId         sql.NullInt64  `db:"partition_time_id"`
	CompressionTypeId       sql.NullInt64  `db:"compression_type_id"`
	AWSAcessKeyID           sql.NullString `db:"aws_access_key_id" default:""`
	AWSSecretAcessKey       sql.NullString `db:"aws_secret_access_key" default:""`
	GCPJsonCredentials      sql.NullString `db:"gcp_json_credentials" default:""`
	AzureStorageAccountname sql.NullString `db:"azure_storage_account_name" default:""`
	AzureStorageAccessKey   sql.NullString `db:"azure_storage_access_key" default:""`
	NamenodeHost            sql.NullString `db:"namenode_host" default:"host.docker.internal"`
	NamenodePort            sql.NullInt64  `db:"namenode_port" default:8020`
	GlueEnabled             sql.NullBool   `db:"glue_enabled"`
	GlueRole                sql.NullString `db:"glue_role" default:""`
	GlueScheduleCron        sql.NullString `db:"glue_schedule_cron" default:""`
	SnowflakeEnabled        sql.NullBool   `db:"snowflake_enabled"`
	SnowflakeAccount        sql.NullString `db:"snowflake_account" default:""`
	SnowflakeUsername       sql.NullString `db:"snowflake_username" default:""`
	SnowflakePassword       sql.NullString `db:"snowflake_password" default:""`
	SnowflakeDatabase       sql.NullString `db:"snowflake_database" default:""`
	Functions               sql.NullString `db:"functions" default:""`
	CreatedAt               time.Time      `db:"created_at"`
	UpdatedAt               time.Time      `db:"updated_at"`
}

var configs []Config

var streamConfigs []map[string]interface{}

var allFunctions []string

//utility method to remove duplicate strings from array
//https://stackoverflow.com/questions/66643946/how-to-remove-duplicates-strings-or-int-from-slice-in-go
func removeDuplicateStr(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

// GetEnv get key environment variable if exist otherwise return defalutValue
func GetEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

//loads all stream configurations
func LoadConfig() error {
	streamConfigs = make([]map[string]interface{}, 0)
	configFiles, err := ioutil.ReadDir("configs")
	if err != nil {
		return err
	}

	//read all the config files and load them into the array of map[string]interface{}
	for _, configFile := range configFiles {

		configString, err2 := ioutil.ReadFile("configs/" + configFile.Name())
		if err2 != nil {
			return err2
		}
		var configObject map[string]interface{}
		json.Unmarshal(configString, &configObject)
		streamConfigs = append(streamConfigs, configObject)

	}
	log.Println("No. of configs loaded " + strconv.Itoa(len(streamConfigs)))
	return nil

}

//utility method for Kafka message writing
func WriteKafkaMessage(kafkaURL string, topic string,body []byte) {
	// to produce messages
	partition := 0

	if topic != "" {
		fmt.Println("Topic: ", topic)

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
	}
	
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

			log.Println(len(streamConfigs))
			//now figure out the topic
			var matchingConfig map[string]interface{}

			//first retrieve relevant destination information from config array

			for _, configRecord := range streamConfigs {


				if message["stream_alt_id"] != nil && message["stream_alt_id"] != "" { //use stream_alt_id

					if configRecord["stream_alt_id"] == message["stream_alt_id"] {
						matchingConfig = configRecord
						break

					}

				}

				if message["stream_id"] != nil && message["stream_id"] != "" {
					if configRecord["stream_id"] == message["stream_id"] {
						matchingConfig = configRecord
						break

					}

				}

			}

			if matchingConfig != nil {
				if matchingConfig["functions"] != nil && fmt.Sprint(matchingConfig["functions"]) != "" {
					//parse sequence into string array
					functions := strings.Split(fmt.Sprint(matchingConfig["functions"]), ",")
					//next need to sanitize the sequence to avoid repeats
					functions = removeDuplicateStr(functions)
					topic = functions[0] + "-ingress"
				} else {
					topic = "ingester-ingress" //default flow
				}

				WriteKafkaMessage(kafkaURL, topic, body)
	
			}


		} else { //cache refresh request

			err := LoadConfig()

			if err != nil {
				log.Fatal("Unable to load configuration ", err)
			}

			body = []byte(`{"stream_id":"","message_type":"rtdl_205","payload":{}}`)

			//cache refresh request to all functions
			for _, function := range allFunctions {
				WriteKafkaMessage(kafkaURL, function + "-ingress", body)
			}

		}
	

	})
}

func main() {

	err := LoadConfig()

	if err != nil {
		log.Fatal("Unable to load configuration ", err)
	}

	//load list of all functions, this would be required for sending control messages
	allFunctionsListFile, err := os.Open("constants/all_functions.json")
	if err != nil {
		log.Println(err)
	} else {
		allFunctionsList, err := gonfig.FromJson(allFunctionsListFile)
		if err != nil {
			log.Println(err)
		} else {
			allFunctionsListString, err := allFunctionsList.GetString("functions","")
			if err != nil {
				log.Println(err)
			} else {
				if allFunctionsListString != "" {
					allFunctions = strings.Split(fmt.Sprint(allFunctionsListString),",")
				}
			}
		}

	}
	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("KAFKA_URL")
	//topic := os.Getenv("KAFKA_TOPIC")

	topic := ""

	//defer kafkaWriter.Close()

	// Add handle func for producer.
	http.HandleFunc("/ingest", producerHandler(kafkaURL, topic, "ingest"))

	http.HandleFunc("/refreshCache", producerHandler(kafkaURL, topic, "refresh-cache"))

	// Run the web server.
	log.Fatal(http.ListenAndServe(":"+GetEnv("LISTENER_PORT", "8080"), nil))
}
