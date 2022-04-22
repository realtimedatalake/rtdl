//stateful function that reads messages from Kafka topic and writes to data lake
//event messages are put on to Kafka by the ingest REST endpoint

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/colinmarc/hdfs"
	//"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/snowflakedb/gosnowflake"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"github.com/creamdog/gonfig"
)

// default database connection settings
const (
	db_host_def     = "rtdl-db"
	db_port_def     = 5433
	db_user_def     = "rtdl"
	db_password_def = "rtdl"
	db_dbname_def   = "rtdl_db"
)

// create the `psqlCon` string used to connect to the database
var psqlCon string

// Dremio host and port
var dremioHost string
var dremioPort string

// header for Dremio communication
var dremioToken string

//Incoming message would have
// - a source key to identify the stream
// - a message type that can be used to indicate the message purpose
// - generic payload

type IncomingMessage struct {
	StreamId    string                 `json:"stream_id,omitempty"`
	StreamAltId string                 `json:"stream_alt_id,omitempty"`
	MessageType string                 `json:"message_type,omitempty"`
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
	CreatedAt               time.Time      `db:"created_at"`
	UpdatedAt               time.Time      `db:"updated_at"`
}

var configs []Config

var streamConfigs []map[string]interface{}

var configKeyValue map[string]interface{} //every config entry is a JSON object now

//struct represenation of file store types
type FileStoreType struct {
	FileStoreTypeId   int64  `db:"file_store_type_id"`
	FileStoreTypeName string `db:"file_store_type_name"`
}

var fileStoreTypes []FileStoreType

//struct representation of partition times
type PartitionTime struct {
	PartitionTimeId   int64  `db:"partition_time_id"`
	PartitionTimeName string `db:"partition_time_name"`
}

var partitionTimes []PartitionTime

type CompressionType struct {
	CompressionTypeId   int64  `db:"compression_type_id"`
	CompressionTypeName string `db:"compression_type_name"`
}

var compressionTypes []CompressionType

//name variables for stateful function
var (
	IngestTypeName      = statefun.TypeNameFrom("com.rtdl.sf/ingest")
	KafkaEgressTypeName = statefun.TypeNameFrom("com.rtdl.sf/egress")
	IncomingMessageType = statefun.MakeJsonType(statefun.TypeNameFrom("com.rtdl.sf/IncomingMessage"))
)

//GCP config structure
type GCPCredentials struct {
	accountType             string `json:"type"`
	projectId               string `json:"project_id"`
	privateKeyId            string `json:"private_key_id"`
	privateKey              string `json:"private_key"`
	clientEmail             string `json:"client_email"`
	clientId                string `json:"client_id"`
	authUri                 string `json:"auth_uri"`
	tokenUri                string `json:"token_uri"`
	authProviderX509CertUrl string `json:"auth_provider_x509_cert_url"`
	clientX509CertUrl       string `json:"client_x509_cert_url"`
}

var storageTypesConstants gonfig.Gonfig
var partitionTimesConstants gonfig.Gonfig
var compressionTypesConstants gonfig.Gonfig


//load constants from shared constants JSONs
func LoadConstants() error {
	storageTypesConstantsFile, err := os.Open("constants/file_store_types.json")
	if err != nil {
	  return err
	}
	defer storageTypesConstantsFile.Close();
	storageTypesConstants, err = gonfig.FromJson(storageTypesConstantsFile)
	if err != nil {
	  return err
	}

	partitionTimesConstantsFile, err := os.Open("constants/partition_times.json")
	if err != nil {
	  return err
	}
	defer partitionTimesConstantsFile.Close();
	partitionTimesConstants, err = gonfig.FromJson(partitionTimesConstantsFile)
	if err != nil {
	  return err
	}

	compressionTypesConstantsFile, err := os.Open("constants/compression_types.json")
	if err != nil {
	  return err
	}
	defer compressionTypesConstantsFile.Close();
	compressionTypesConstants, err = gonfig.FromJson(compressionTypesConstantsFile)
	if err != nil {
	  return err
	}

	return nil

}

//utility methods to wrap around gonfig's constant reading logic
func GetStorageTypeId(fileStoreTypeLiteral string) float64 {
	fileStoreTypeId, err := storageTypesConstants.GetFloat(fileStoreTypeLiteral,nil)
	if err != nil {
		return -1
	} else {
		return fileStoreTypeId
	}
}

func GetPartitionTimeId(partitionTimeLiteral string) float64 {
	partitionTimeId, err := partitionTimesConstants.GetFloat(partitionTimeLiteral,nil)
	if err != nil {
		return -1
	} else {
		return partitionTimeId
	}
}

func GetCompressionTypeId(compressionTypeLiteral string) float64 {
	compressionTypeId, err := compressionTypesConstants.GetFloat(compressionTypeLiteral,nil)
	if err != nil {
		return -1
	} else {
		return compressionTypeId
	}
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
	streamConfigs = make([]map[string]interface{},0)
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
		json.Unmarshal(configString,&configObject)
		streamConfigs = append(streamConfigs, configObject)

	}
	log.Println("No. of configs loaded " + strconv.Itoa(len(streamConfigs)))
	return nil

}

//loads all stream configurations - old implementation

//generic function for Dremio request response
func DremioReqRes(endPoint string, data []byte) (map[string]interface{}, error) {

	var version string
	var method string
	var request *http.Request
	var err error
	var url string

	if strings.Contains(dremioHost, "cloud") { //Dremio cloud

		dremioCloudProjectId := os.Getenv("DREMIO_CLOUD_PROJECT_ID")

		if dremioCloudProjectId == "" {
			return nil, errors.New("DREMIO_CLOUD_PROJECT_ID cannot be blank for Dremio Cloud")
		}

		url = "https://" + dremioHost + "/v0/projects/" + dremioCloudProjectId + "/" + endPoint

	} else {
		if endPoint == "login" || strings.Contains(endPoint, "folder_format") { //end point v2

			version = "apiv2"

		} else {

			version = "api/v3"

		}

		url = "http://" + dremioHost + ":" + dremioPort + "/" + version + "/" + endPoint

	}

	//log.Println(url)

	if strings.Contains(endPoint, "folder_format") {

		method = "PUT"
		request, err = http.NewRequest(method, url, strings.NewReader(`{"type":"Parquet"}`))

	} else {
		if data == nil { //Get request
			method = "GET"
			request, err = http.NewRequest(method, url, nil)

		} else {
			method = "POST"
			request, err = http.NewRequest(method, url, bytes.NewBuffer(data))
		}

	}

	if err != nil {
		log.Println("Error communicating with Dremio server ", err)
		return nil, err

	}

	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	if endPoint != "login" { //need to set auth header for non-login calls

		request.Header.Set("Authorization", dremioToken)
	}

	client := &http.Client{}
	response, error := client.Do(request)
	if error != nil {
		return nil, error
	}
	defer response.Body.Close()

	body, _ := ioutil.ReadAll(response.Body)

	var dremioResponse map[string]interface{}

	err = json.Unmarshal(body, &dremioResponse)

	if err != nil {
		return nil, err
	}

	//log.Println(dremioResponse)
	return dremioResponse, nil

}

//connect to Dremio server and retrieve token for subsequent calls
func SetDremioToken() error {

	if strings.Contains(dremioHost, "cloud") { //Dremio Cloud

		dremioCloudToken := os.Getenv("DREMIO_PASSWORD")

		if dremioCloudToken == "" { //Password cannot be blank for Dremio Cloud
			return errors.New("DREMIO_PASSWORD cannot be blank for Dremio Cloud")
		}

		dremioToken = "Bearer " + dremioCloudToken
		return nil

	}

	username := GetEnv("DREMIO_USERNAME", "rtdl")
	password := GetEnv("DREMIO_PASSWORD", "rtdl1234")

	loginData := []byte(`{"userName":"` + username + `", "password":"` + password + `"}`)

	dremioResponse, err := DremioReqRes("login", loginData)

	if err != nil {

		log.Println("Error retrieving Dremio token ", err)
		return err
	}

	dremioToken = fmt.Sprint(dremioResponse["token"])

	return nil

}

//initialize Dremio connection
func SetDremioConnection() error {

	// set Dremio host and port for use in other calls
	dremioHost = GetEnv("DREMIO_HOST", "host.docker.internal")
	dremioPort = GetEnv("DREMIO_PORT", "9047")

	err := SetDremioToken()

	if err != nil {
		return err
	}

	return nil

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
	case "boolean", "bool":
		return `BOOLEAN`

	}
	return ""

}

//use reflection to study incoming generic payload and construct schema necessary for Parquet
//payload is passed recursively through the function to break down till the elemental level

func GenerateSchema(payload map[string]interface{}, messageType string, jsonSchema string) string {

	if jsonSchema == "" {
		jsonSchema = `{"Tag": "name=` + messageType + `, repetitiontype=REQUIRED",`
		jsonSchema += `"Fields": [`
	}

	for key, value := range payload {

		if value == nil {

			continue //skip nulls
		}

		dataType := reflect.TypeOf(value).String()

		//special processing for nested object structures
		if strings.HasPrefix(dataType, "map[string]interface") {

			if len(value.(map[string]interface{})) == 0 {

				continue //skip empty structs

			}

			jsonSchema += `{"Tag": "name=` + key

			jsonSchema += `, repetitiontype=REQUIRED", "Fields" : [`
			jsonSchema = GenerateSchema(value.(map[string]interface{}), messageType, jsonSchema) //need recursion
			jsonSchema = strings.TrimRight(jsonSchema, ",")                                      //remove trailing comma
			jsonSchema += `]},`

		} else if strings.HasPrefix(dataType, "[]interface") { //special processing for arrays as well

			if len(value.([]interface{})) > 0 { //to be generated only for non-empty arrays

				jsonSchema += `{"Tag": "name=` + key

				jsonSchema += `, type=LIST, repetitiontype=REQUIRED", "Fields" : [`
				arrayItemDataType := reflect.TypeOf(value.([]interface{})[0]).String()
				if strings.HasPrefix(arrayItemDataType, "map[string]interface") { //if array consists of objects then same have to be recursed
					jsonSchema += `{"Tag": "name=element, repetitiontype=REQUIRED", "Fields" : [`
					jsonSchema = GenerateSchema(value.([]interface{})[0].(map[string]interface{}), messageType, jsonSchema)
					jsonSchema = strings.TrimRight(jsonSchema, ",")
					jsonSchema += `]},`
				} else { //arrays composed of native data types can be handled directly
					jsonSchema += `{"Tag": "name=element, type=` + getParquetDataType(reflect.TypeOf(value.([]interface{})[0]).String())
					jsonSchema += `, repetitiontype=REQUIRED"},`
				}
				jsonSchema = strings.TrimRight(jsonSchema, ",")
				jsonSchema += `]},`

			}

		} else { //native data type

			jsonSchema += `{"Tag": "name=` + key

			jsonSchema += `, type=` + getParquetDataType(dataType)
			jsonSchema += `, repetitiontype=REQUIRED"},`

		}

	}

	return jsonSchema

}

func generateSubFolderName(messageType string, configRecord map[string]interface{}) string {

	var subFolderName string

	switch configRecord["partition_time_id"] {

	case GetPartitionTimeId("partition_time_hourly"):

		subFolderName = messageType + "/" + time.Now().Format("2006-01-02-15")

	case GetPartitionTimeId("partition_time_daily"):
		subFolderName = messageType + "/" + time.Now().Format("2006-01-02")

	case GetPartitionTimeId("partition_time_weekly"):
		year, week := time.Now().ISOWeek()
		subFolderName = messageType + "/" + strconv.Itoa(year) + "-" + strconv.Itoa(week)

	case GetPartitionTimeId("partition_time_monthly"):
		subFolderName = messageType + "/" + time.Now().Format("2006-01")

	case GetPartitionTimeId("partition_time_quarterly"):
		quarter := int((time.Now().Month() + 2) / 3)
		subFolderName = messageType + "/" + time.Now().Format("2006") + "-" + string(quarter)
	}

	return subFolderName
}

//generate the leaf level file name
func generateLeafLevelFileName() string {

	//construct the timestamp string
	t := time.Now()
	year := t.Year()
	month := t.Month()
	day := t.Day()
	hour := t.Hour()
	min := t.Minute()
	sec := t.Second()
	nanosec := t.Nanosecond()

	return strconv.Itoa(year) + strconv.Itoa(int(month)) + strconv.Itoa(day) + "_" + strconv.Itoa(hour) + strconv.Itoa(min) + strconv.Itoa(sec) + strconv.Itoa(nanosec) + ".parquet"

}

//writer-agnostic function to actually write to file
func WriteToFile(schema string, fw source.ParquetFile, payload []byte, configRecord map[string]interface{}) error {

	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		log.Println("Can't create json writer", err)
		return err
	}

	//set compression

	compressionType := configRecord["compression_type_id"].(float64)

	if compressionType > 0 && compressionType < 4 { //supported compression type

		switch compressionType {
		case GetCompressionTypeId("compression_type_snappy"):
			pw.CompressionType = parquet.CompressionCodec_SNAPPY
		case GetCompressionTypeId("compression_type_gzip"):
			pw.CompressionType = parquet.CompressionCodec_GZIP
		case GetCompressionTypeId("compression_type_lzo"):
			pw.CompressionType = parquet.CompressionCodec_LZO

		}
	}

	if err = pw.Write(payload); err != nil {
		log.Println("Write error", err)
		return err
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return err
	}
	log.Println("Write Finished")
	fw.Close()
	return nil

}

//Function to update Snowflake
func UpdateSnowflake(messageType string, sourceType string, configRecord map[string]interface{}) error {

	var path string

	user := os.Getenv("SNOWFLAKE_USER")
	password := os.Getenv("SNOWFLAKE_PASSWORD")
	acct := os.Getenv("SNOWFLAKE_ACCT")
	db := os.Getenv("SNOWFLAKE_DB")
	if user == "" || password == "" || acct == "" || db == "" {
		return errors.New("Valid values required for all of Snowflake Account, User, Password and Database")
	}

	connectionString := user + ":" + password + "@" + acct + "/" + db
	conn, err := sql.Open("snowflake", connectionString)
	if err != nil {
		log.Println("Unable to open Snowflake connection", err)
		return err
	}
	defer conn.Close()

	switch sourceType {
	case "S3":
		path = "s3://"

	case "GCS":
		path = "gcs://"

	case "Azure":
		path = "azure://" + configRecord["azure_storage_account_name"].(string) + ".blob.core.windows.net/"
	}

	path += configRecord["bucket_name"].(string)
	if configRecord["folder_name"].(string) != "" {
		path += "/" + configRecord["folder_name"].(string)

	}

	path += "/" + messageType //do not remove hyphens from this since this points to Azure location

	//cannot use stream_id as is for schema name like Dremio or Glue
	//because of Snowflake naming convention-related restrictions
	schemaName := "s_" + strings.Replace(configRecord["stream_id"].(string), "-", "_", -1)

	//stagename also needs to be cleansed similarly
	stageName := strings.Replace(messageType, "-", "_", -1)

	schemaCreationQuery := "create schema if not exists " + schemaName + ";"

	_, snowflakeErr := conn.ExecContext(context.Background(), schemaCreationQuery)

	if snowflakeErr != nil {
		log.Println("Error creating Snowflake schema", snowflakeErr)
		return snowflakeErr
	}

	stageCreationQuery := "use schema " + schemaName + ";"
	stageCreationQuery += "create stage if not exists " + stageName //hyphen not allowed
	stageCreationQuery += " URL = '" + path + "' "
	//stageCreationQuery += " DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = FALSE) "
	switch sourceType {
	case "S3":
		stageCreationQuery += " CREDENTIALS = (AWS_KEY_ID = '" + configRecord["aws_access_key_id"].(string) + "' "
		stageCreationQuery += " AWS_SECRET_KEY = '" + configRecord["aws_secret_access_key"].(string) + "');"

	case "Azure":
		stageCreationQuery += " CREDENTIALS = (AZURE_SAS_TOKEN = '" + configRecord["azure_storage_access_key"].(string) + "'); "
	}

	multiStatementContext, _ := gosnowflake.WithMultiStatement(context.Background(), 2)
	_, snowflakeErr = conn.ExecContext(multiStatementContext, stageCreationQuery)

	if snowflakeErr != nil {
		log.Println("Error creating Snowflake stage", snowflakeErr)
		return snowflakeErr
	}

	tableCreationQuery := "use schema " + schemaName + ";"
	tableCreationQuery += "create external table if not exists " + stageName //table=stage
	tableCreationQuery += " location = @" + stageName
	tableCreationQuery += " file_format = (type = PARQUET);"

	_, snowflakeErr = conn.ExecContext(multiStatementContext, tableCreationQuery)

	if snowflakeErr != nil {
		log.Println("Error creating Snowflake external table", snowflakeErr)
		return snowflakeErr
	}

	log.Println("Snowflake external table created if not already existing")

	return nil
}

//Function for updating Glue
func UpdateGlue(messageType string, configRecord map[string]interface{}, awsSession client.ConfigProvider) error {
	//create Glue Catalog entry irrespective of whether Dremio succeeded or not
	glueClient := glue.New(awsSession, aws.NewConfig().WithRegion(configRecord["region"].(string)))
	//check if database exists
	streamId := configRecord["stream_id"].(string)
	_, err := glueClient.GetDatabase(&glue.GetDatabaseInput{Name: &streamId})

	if err != nil { //assume EntityNotFoundException for now, need to refine error handling later
		//database name will be same as stream_id
		_, err = glueClient.CreateDatabase(&glue.CreateDatabaseInput{DatabaseInput: &glue.DatabaseInput{Name: &streamId}})
		if err != nil {
			log.Println("Error creating Glue database", err)
			return err
		}

		log.Println("Glue database created")

	} else {
		log.Println("Glue database found")
	}

	crawlerName := configRecord["stream_id"].(string) + "_" + messageType
	_, err = glueClient.GetCrawler(&glue.GetCrawlerInput{Name: &crawlerName})

	if err != nil { //assume EntityNotFoundException for now, need to refine error handling later

		//construct crawler path
		crawlerPath := "s3://" + configRecord["bucket_name"].(string)
		if configRecord["folder_name"].(string) != "" {
			crawlerPath += "/" + configRecord["folder_name"].(string)
		}

		crawlerPath += "/" + messageType

		s3Target := &glue.S3Target{Path: &crawlerPath}
		s3TargetList := []*glue.S3Target{s3Target}

		glueRole := os.Getenv("GLUE_ROLE")

		if glueRole == "" {
			log.Println("Role ARN for accessing Glue Services must be provided")
			return errors.New("AWS Role ARN for accessing Glue Services must be provided")
		}

		glueScheduleCron := "cron(" + GetEnv("GLUE_SCHEDULE_CRON", "0 0 * * ? *") + ")" //default every day at 12 AM

		databaseName := configRecord["stream_id"].(string)

		createCrawlerInput := &glue.CreateCrawlerInput{Name: &crawlerName,
			DatabaseName: &databaseName,
			Targets:      &glue.CrawlerTargets{S3Targets: s3TargetList},
			Role:         &glueRole,
			Schedule:     &glueScheduleCron}

		_, err = glueClient.CreateCrawler(createCrawlerInput)
		if err != nil {
			log.Println("Error creating Glue crawler", err)
			return err
		}

		log.Println("Glue crawler created")

	} else {
		log.Println("Glue crawler exists")
	}
	return nil
}

//Function for making Dremio entry
func UpdateDremio(messageType string, sourceType string, location string, configRecord map[string]interface{}) error {

	var sourceDef []byte
	var sourceExists bool
	var sourceId string
	var datasetExists bool

	//desiredPath := messageType + "_" + sourceType //our source names will be <message type>_<source type>
	sourceName := configRecord["stream_id"].(string)
	dremioResponse, err1 := DremioReqRes("source", nil)

	if err1 != nil {

		log.Println("Error retrieving Dremio catalog information ", err1)
		return err1

	}

	log.Println("Dremio source information retrieved")

	//iterate through catalog and find if source already exists

	sources, ok1 := dremioResponse["data"].([]interface{})
	if !ok1 {

		return errors.New("error handling Dremio server response during source retrieval")
	}

	for _, source := range sources {

		entry, ok2 := source.(map[string]interface{})
		if !ok2 {

			return errors.New("error handling Dremio server response during source retrieval")
		}

		if entry["name"] == sourceName {

			sourceExists = true

			//ok, source exits - check if dataset exits
			sourceId, _ = entry["id"].(string)
			dremioResponse, _ = DremioReqRes("catalog/"+sourceId, nil)

			children, ok3 := dremioResponse["children"].([]interface{})
			if !ok3 {

				return errors.New("error handling Dremio server response during dataset retrieval ok3")
			}

			for _, childNode := range children {

				child, _ := childNode.(map[string]interface{})

				path, ok4 := child["path"].([]interface{})
				if !ok4 {

					return errors.New("error handling Dremio server response during dataset retrieval ok4")
				}

				datasetName, _ := path[1].(string)
				datasetType, _ := child["type"].(string)
				if datasetName == messageType && datasetType == "DATASET" {

					datasetExists = true
					break
				}
			}
			break
		}
	}

	if !sourceExists {

		log.Println("Source does not exist for message type, creating ...")
		dremioMountPath := GetEnv("DREMIO_MOUNT_PATH", "/mnt/datastore")

		sourceStringMultiLine := `{"name": "` + sourceName + `"`
		switch sourceType {

		case "Local":
			sourceStringMultiLine += `, "type": "NAS", "config": {"path": "file:///` + dremioMountPath + `/` + configRecord["folder_name"].(string)

		case "S3":

			sourceStringMultiLine += `, "type": "S3", "config": {"accessKey": "` + configRecord["aws_access_key_id"].(string) + `"`
			sourceStringMultiLine += `, "accessSecret": "` + configRecord["aws_secret_access_key"].(string) + `"`
			//sourceStringMultiLine += `, "externalBucketList": ["` + location + `"]`
			if strings.Contains(dremioHost, "cloud") {
				sourceStringMultiLine += `, "rootPath": "/`
			} else {
				sourceStringMultiLine += `, "rootPath": "/` + location + `/`
				if configRecord["folder_name"] != "" {
					sourceStringMultiLine += configRecord["folder_name"].(string) + `/`

				}

			}

		case "GCS":
			var gcpCreds map[string]interface{}
			//need to extract all variable values from GCP crendentials object

			err := json.Unmarshal([]byte(configRecord["gcp_json_credentials"].(string)), &gcpCreds)
			if err != nil {
				log.Println("Error reading GCP credentials from configuration record", err)
				return err
			}

			projectId := gcpCreds["project_id"].(string)
			clientEmail := gcpCreds["client_email"].(string)
			clientId := gcpCreds["client_id"].(string)
			privateKeyId := gcpCreds["private_key_id"].(string)
			privateKey := strings.Replace(gcpCreds["private_key"].(string), "\n", "\\n", -1)
			sourceStringMultiLine += `, "type":"GCS", "config": {"projectId": "` + projectId + `"`
			sourceStringMultiLine += `, "authMode": "SERVICE_ACCOUNT_KEYS", "clientEmail": "` + clientEmail + `"`
			sourceStringMultiLine += `, "clientId": "` + clientId + `", "privateKeyId": "` + privateKeyId + `"`
			sourceStringMultiLine += `, "privateKey": "` + privateKey + `"`
			sourceStringMultiLine += `, "rootPath": "/` + location + `/`
			if configRecord["folder_name"] != "" {
				sourceStringMultiLine += configRecord["folder_name"].(string) + `/`

			}

		case "Azure":

			//sourceStringMultiLine += `, "metadataPolicy": {"datasetUpdateMode": "INLINE"} `
			sourceStringMultiLine += `, "metadataPolicy": {"datasetUpdateMode": "INLINE", "datasetRefreshAfterMs": 60000 ` //to be made customisable
			sourceStringMultiLine += `, "namesRefreshMs": 60000, "authTTLMs": 60000, "datasetExpireAfterMs": 60000} `      //metadata end, comment/delete two lines together
			sourceStringMultiLine += `, "type": "AZURE_STORAGE", "config": {"accountName": "` + configRecord["azure_storage_account_name"].(string) + `"`
			sourceStringMultiLine += `, "enableSSL": true, "isCachingEnabled": false, "accountKind":"STORAGE_V2","credentialsType":"ACCESS_KEY"` //candidate for future customisation
			sourceStringMultiLine += `, "accessKey": "` + configRecord["azure_storage_access_key"].(string) + `"`
			sourceStringMultiLine += `, "rootPath": "/` + location + `/`
			if configRecord["folder_name"].(string) != "" {
				sourceStringMultiLine += configRecord["folder_name"].(string) + `/`

			}

		case "HDFS":

			sourceStringMultiLine += `, "type": "HDFS", "config": {"hostname": "` + configRecord["namenode_host"].(string) + `"`
			sourceStringMultiLine += `, "port": ` + strconv.Itoa(int(configRecord["namenode_port"].(float64)))
			sourceStringMultiLine += `, "rootPath": "/` + location + `/`
			if configRecord["folder_name"].(string) != "" {
				sourceStringMultiLine += configRecord["folder_name"].(string) + `/`

			}

		}

		sourceStringMultiLine += `"}}`

		sourceDef = []byte(sourceStringMultiLine)

		//log.Println(sourceStringMultiLine)

		dremioResponse, err1 = DremioReqRes("source", sourceDef)

		if err1 != nil {

			log.Println("Error creating Dremio source ", err1)
			return err1
		}

	}

	if !datasetExists {

		var encodedId string
		var datasetDefMultiLine string

		//next we have to create the dataset

		if sourceType == "HDFS" {

			return nil //HDFS dataset creation to be done separately

		} else {

			if strings.Contains(dremioHost, "cloud") { //for Dremio Cloud, need to add bucket and folder to path
				encodedId = "dremio%3A%2F" + sourceName + "%2F" + configRecord["bucket_name"].(string)
				if configRecord["folder_name"].(string) != "" {
					encodedId += "%2F" + configRecord["folder_name"].(string)
				}
				encodedId += "%2F" + messageType
				datasetDefMultiLine = `{"id": "` + encodedId + `", "entityType": "dataset", "path": ["` + sourceName + `", "` + configRecord["bucket_name"].(string) + `" `
				if configRecord["folder_name"].(string) != "" {
					datasetDefMultiLine += `, "` + configRecord["folder_name"].(string) + `" `
				}
				datasetDefMultiLine += `, "` + messageType + `"]`
			} else {
				encodedId = "dremio%3A%2F" + sourceName + "%2F" + messageType
				datasetDefMultiLine = `{"id": "` + encodedId + `", "entityType": "dataset", "path": ["` + sourceName + `", "` + messageType + `"]`
			}

			datasetDefMultiLine += `, "format": {"type": "Parquet"}`
			datasetDefMultiLine += `, "type": "PHYSICAL_DATASET"`
			datasetDefMultiLine += `}`
			datasetDef := []byte(datasetDefMultiLine)

			dremioResponse, err1 = DremioReqRes("catalog/"+encodedId, datasetDef)

		}

		if err1 != nil {

			log.Println("Error creating Dremio dataset ", err1)
			return err1
		}

	}

	return nil

}

//Write local Parquet
func WriteLocalParquet(messageType string, schema string, payload []byte, configRecord map[string]interface{}) error {

	//write
	path := "datastore" //root will always be datastore

	folderName := configRecord["folder_name"].(string)
	if folderName != "" { //default
		path += "/" + folderName
	}

	path += "/" + generateSubFolderName(messageType, configRecord)

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Println("Can't create output directory", err)
		return err
	}

	location := os.Getenv("LOCAL_FS_MOUNT_PATH") + "/" + path
	fileName := path + "/" + generateLeafLevelFileName()

	log.Println("Local path:", fileName)

	fw, err := local.NewLocalFileWriter(fileName)

	if err != nil {
		log.Println("Can't create file", err)
		return err
	}

	err = WriteToFile(schema, fw, payload, configRecord)

	if err == nil { //file write successful, update Dremio

		return UpdateDremio(messageType, "Local", location, configRecord)

	}

	return err

}

func CreateHDFSDataset(messageType string, configRecord map[string]interface{}) error {

	var url string

	if strings.Contains(dremioHost, "cloud") { //

		dremioCloudProjectId := os.Getenv("DREMIO_CLOUD_PROJECT_ID")

		if dremioCloudProjectId == "" {
			return errors.New("DREMIO_CLOUD_PROJECT_ID cannot be blank for Dremio Cloud")
		}

		url = "https://" + dremioHost + "/v0/projects/" + dremioCloudProjectId + "/source/" + configRecord["stream_id"].(string) + "/folder_format/" + messageType

	} else {
		url = "http://" + dremioHost + ":" + dremioPort + "/apiv2/source/" + configRecord["stream_id"].(string) + "/folder_format/" + messageType
	}

	method := "PUT"

	payload := strings.NewReader(`{"type":"Parquet"}`)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		log.Println(err)
		return err
	}
	req.Header.Add("Authorization", dremioToken)
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}
	defer res.Body.Close()

	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil

}

func WriteHDFSParquet(messageType string, schema string, payload []byte, configRecord map[string]interface{}) error {

	if configRecord["bucket_name"] == "" {
		return errors.New("HDFS root folder (bucket) name cannot be null or empty")
	}
	subFolderName := generateSubFolderName(messageType, configRecord)
	leafLevelFileName := generateLeafLevelFileName()

	path := "/" + configRecord["bucket_name"].(string)

	if configRecord["folder_name"].(string) != "" {
		path = path + "/" + configRecord["folder_name"].(string)
	}

	path = path + "/" + subFolderName

	fw, err := local.NewLocalFileWriter(leafLevelFileName)
	err = WriteToFile(schema, fw, payload, configRecord) //write temporary local file
	if err != nil {
		log.Println("Unable to write temporary local file", err)
		return err
	}

	tempFile, err1 := os.Open(leafLevelFileName) //open temporary local file
	if err1 != nil {
		log.Println("Unable to open temporary local file", err1)
		return err1
	}

	defer tempFile.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := tempFile.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	tempFile.Read(buffer)

	//temporary code for HDFS write test
	client, clientError := hdfs.New(configRecord["namenode_host"].(string) + ":" + strconv.Itoa(int(configRecord["namenode_port"].(float64))))
	if clientError != nil {
		log.Println(clientError)
	} else {
		_, hdfsReadError := client.ReadDir(path)

		if hdfsReadError != nil { //directory does not exist
			createErr := client.MkdirAll("/"+path, os.FileMode(0777))
			if createErr != nil {
				log.Println(createErr)
			} else {

				log.Println("directory created in hdfs")
			}

		} else {
			log.Println("HDFS Directory " + path + " exists")
		}
	}

	fw2, err2 := client.Create(path + "/" + leafLevelFileName)
	if err2 != nil {
		log.Println("Error creating file in HDFS", err2)
		return err2
	}

	fw2.Close()

	fw2, err2 = client.Append(path + "/" + leafLevelFileName)
	if err2 != nil {
		log.Println("Error opening file for writing in HDFS", err2)
		return err2
	}

	defer fw2.Close()

	_, err = fw2.Write(buffer)

	if err != nil {
		log.Println("Error writing file to HDFS", err)
	}

	err = fw2.Flush()

	if err != nil {
		log.Println("Error flushing file to HDFS", err)
	}

	os.Remove(leafLevelFileName) //remove the temp file
	if err == nil {
		log.Println("Finished writing file to HDFS")
		err = UpdateDremio(messageType, "HDFS", configRecord["bucket_name"].(string), configRecord)
	}

	return err

}

func WriteAWSParquet(messageType string, schema string, payload []byte, configRecord map[string]interface{}) error {

	var key string

	subFolderName := generateSubFolderName(messageType, configRecord)
	leafLevelFileName := generateLeafLevelFileName()

	if configRecord["region"] == "" {
		return errors.New("AWS Region cannot be null or empty")
	}

	region := strings.TrimSpace(configRecord["region"].(string))
	awsAccessKeyId := strings.TrimSpace(configRecord["aws_access_key_id"].(string))
	awsSecretAccessKey := strings.TrimSpace(configRecord["aws_secret_access_key"].(string))

	//log.Println("AWS Parquet writing implementation pending")
	bucketName := configRecord["bucket_name"].(string)
	if bucketName == "" {
		return errors.New("S3 bucket name cannot be null or empty")
	}

	if configRecord["folder_name"] != "" {

		key = configRecord["folder_name"].(string) + "/" + subFolderName + "/" + leafLevelFileName

	} else {

		key = subFolderName + "/" + leafLevelFileName

	}

	fw, err := local.NewLocalFileWriter(leafLevelFileName)
	err = WriteToFile(schema, fw, payload, configRecord) //write temporary local file
	if err != nil {
		log.Println("Unable to write temporary local file", err)
		return err
	}

	awsSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, ""),
	})
	if err != nil {
		log.Println("Failed to create AWS Session ", err)
		return err
	}

	tempFile, err1 := os.Open(leafLevelFileName) //open temporary local file
	if err1 != nil {
		log.Println("Unable to open temporary local file", err1)
		return err1
	}

	defer tempFile.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := tempFile.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	tempFile.Read(buffer)

	// Config settings: this is where we choose the bucket, filename, content-type etc.
	// of the file we're uploading.
	_, err = s3.New(awsSession).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		//ACL:                  aws.String("private"),
		Body: bytes.NewReader(buffer),
		//ContentLength:        aws.Int64(size),
		//ContentType:          aws.String(http.DetectContentType(buffer)),
		//ContentDisposition:   aws.String("attachment"),
		//ServerSideEncryption: aws.String("AES256"),
	})

	os.Remove(leafLevelFileName) //remove the temp file
	log.Println("Finished uploading file to S3")

	if err == nil {

		err = UpdateDremio(messageType, "S3", bucketName, configRecord)

		if err != nil {
			log.Println("Error updating Dremio", err)
		}

		glueEnabled, _ := strconv.ParseBool(GetEnv("GLUE_ENABLED", "false"))
		if glueEnabled {
			err = UpdateGlue(messageType, configRecord, awsSession)
		}

		snowflakeEnabled, _ := strconv.ParseBool(GetEnv("SNOWFLAKE_ENABLED", "false"))
		if snowflakeEnabled {
			err = UpdateSnowflake(messageType, "S3", configRecord)
		}

	}

	return err

}

func WriteGCPParquet(messageType string, schema string, payload []byte, configRecord map[string]interface{}) error {

	var path string
	//var location string

	subFolderName := generateSubFolderName(messageType, configRecord)
	leafLevelFileName := generateLeafLevelFileName()

	//replace all \n	with \\n to preserve them
	jsonCreds := strings.Replace(configRecord["gcp_json_credentials"].(string), "\n", "\\n", -1)

	//create client
	ctx := context.Background()
	creds, err := google.CredentialsFromJSON(ctx, []byte(jsonCreds), secretmanager.DefaultAuthScopes()...)
	if err != nil {
		log.Println("Error creating GCP credentials", err)
		return err
	}

	client, err := storage.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		log.Println("Error creating GCP client", err)
		return err
	}
	defer client.Close()

	bucketName := configRecord["bucket_name"].(string)
	if bucketName == "" {
		return errors.New("GCS bucket name cannot be null or empty")
	}

	if configRecord["folder_name"].(string) != "" {

		path = configRecord["folder_name"].(string) + "/" + subFolderName + "/" + leafLevelFileName
		//location = configRecord["folder_name"] + "/" + subFolderName
	} else {

		path = subFolderName + "/" + leafLevelFileName
		//location = subFolderName
	}

	fw, err := local.NewLocalFileWriter(leafLevelFileName)
	err = WriteToFile(schema, fw, payload, configRecord) //write temporary local file
	if err != nil {
		log.Println("Unable to write temporary local file", err)
		return err
	}

	tempFile, err1 := os.Open(leafLevelFileName) //open temporary local file
	if err1 != nil {
		log.Println("Unable to open temporary local file", err1)
		return err1
	}

	defer tempFile.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	writer := client.Bucket(bucketName).Object(path).NewWriter(ctx)
	if _, err = io.Copy(writer, tempFile); err != nil {
		log.Println("Error uploading file", err)
		return err
	}
	if err := writer.Close(); err != nil {
		log.Println("Error closing writer", err)
		return err
	}

	os.Remove(leafLevelFileName) //remove the temp file

	log.Println("Finished uploading file to GCS")
	if err == nil {

		//update Dremio if file update was successful
		err = UpdateDremio(messageType, "GCS", bucketName, configRecord)

		if err != nil {
			log.Println("Error updating Dremio", err)
		}

		//update Snowflake if support is enabled
		//this is on hold for now as it requires manual intervention and cannot be automated completely
		/*
			snowflakeEnabled, _ := strconv.ParseBool(GetEnv("SNOWFLAKE_ENABLED", "false"))
			if snowflakeEnabled {
				err = UpdateSnowflake(messageType, "GCS", configRecord)
			}
		*/

	}

	return err

}

func WriteAzureParquet(messageType string, schema string, payload []byte, configRecord map[string]interface{}) error {

	log.Println("inside WriteAzureParquet")
	var path string
	//var location string

	subFolderName := generateSubFolderName(messageType, configRecord)
	leafLevelFileName := generateLeafLevelFileName()

	// Create a request pipeline that is used to process HTTP(S) requests and responses. It requires
	// your account credentials. In more advanced scenarios, you can configure telemetry, retry policies,
	// logging, and other options. Also, you can configure multiple request pipelines for different scenarios.
	azureCredential, err := azblob.NewSharedKeyCredential(configRecord["azure_storage_account_name"].(string), configRecord["azure_storage_access_key"].(string))
	if err != nil {
		log.Println("Error constructing Azure credential", err)
		return err
	}

	azurePipeline := azblob.NewPipeline(azureCredential, azblob.PipelineOptions{})

	//Storage account blob service URL endpoint
	azureUrl, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", configRecord["azure_storage_account_name"].(string)))

	bucketName := configRecord["bucket_name"].(string) //maps to Container Name for Azure Storage
	if bucketName == "" {
		return errors.New("Bucket name (maps to Azure Storage Account Name) cannot be null or empty")
	}

	if configRecord["folder_name"] != "" {

		path = configRecord["folder_name"].(string) + "/" + subFolderName + "/" + leafLevelFileName
		//location = configRecord["folder_name"] + "/" + subFolderName
	} else {

		path = subFolderName + "/" + leafLevelFileName
		//location = subFolderName
	}

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	azureServiceURL := azblob.NewServiceURL(*azureUrl, azurePipeline)

	fw, err := local.NewLocalFileWriter(leafLevelFileName)
	err = WriteToFile(schema, fw, payload, configRecord) //write temporary local file
	if err != nil {
		log.Println("Unable to write temporary local file", err)
		return err
	}

	ctx := context.Background()
	tempFile, err1 := os.Open(leafLevelFileName) //open temporary local file
	if err1 != nil {
		log.Println("Unable to open temporary local file", err1)
		return err1
	}

	defer tempFile.Close()

	// Create a URL that references a to-be-created container in your Azure Storage account.
	// This returns a ContainerURL object that wraps the container's URL and a request pipeline (inherited from serviceURL)
	azureContainerURL := azureServiceURL.NewContainerURL(strings.ToLower(bucketName)) // Container names require lowercase

	//check if container exists
	azureContainerProperties, _ := azureContainerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})

	if azureContainerProperties == nil { //container does not exist, need to create
		// Create the container on the service (with no metadata and no public access)
		_, err := azureContainerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		if err != nil {
			log.Println("Error creating Azure Storage container", err)
			return err
		}
	}

	// Create a URL that references a to-be-created blob in your Azure Storage account's container.
	// This returns a BlockBlobURL object that wraps the blob's URL and a request pipeline (inherited from containerURL)
	azureBlobURL := azureContainerURL.NewBlockBlobURL(path)

	_, err = azureBlobURL.Upload(ctx, tempFile, azblob.BlobHTTPHeaders{ContentType: "application/octet-stream"}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		log.Println("Error writing Azure blob", err)
		return err
	}

	os.Remove(leafLevelFileName) //remove the temp file

	log.Println("Finished uploading file to Azure")
	if err == nil {

		err = UpdateDremio(messageType, "Azure", bucketName, configRecord)

		if err != nil {
			log.Println(("Error updating Dremio"))
		}
		//update Snowflake if support is enabled
		//this is on hold for now as it requires manual intervention and cannot be automated completely
		/*
			snowflakeEnabled, _ := strconv.ParseBool(GetEnv("SNOWFLAKE_ENABLED", "false"))
			if snowflakeEnabled {
				err = UpdateSnowflake(messageType, "Azure", configRecord)
			}
		*/

	}
	return err

}

//Parquet writing logic
func WriteParquet(request IncomingMessage) error {

	//log.Println(GenerateSchema(request.Payload,request.MessageType, "")+"]}")

	//message type precedence order will be 1."type" within request.Payload 2."message_type" within incoming message 3. Config Record MessageType
	//a default value will also be kept

	var messageType string = "rtdl_default"

	payload, _ := json.Marshal(request.Payload) //convert generic payload structure to JSON string

	var matchingConfig map[string]interface{}

	//first retrieve relevant destination information from config array

	for _, configRecord := range streamConfigs {

		if request.StreamAltId != "" { //use stream_alt_id

			if configRecord["stream_alt_id"] == request.StreamAltId {
				matchingConfig = configRecord
				break

			}

		}

		if request.StreamId != "" {
			if configRecord["stream_id"] == request.StreamId {
				matchingConfig = configRecord
				break

			}

		}

	}

	//least precendence - config record message_type
	if matchingConfig["message_type"] != "" {

		messageType = matchingConfig["message_type"].(string)
	}

	//higher precendence message_type within message
	if request.MessageType != "" {

		messageType = request.MessageType
	}

	//highest precedence - type inside main payload
	if payloadType, found := request.Payload["type"]; found {
		if typeString, ok := payloadType.(string); ok {

			messageType = typeString
		}
	}

	schema := strings.TrimRight(GenerateSchema(request.Payload, messageType, ""), ",") + "]}"

	switch matchingConfig["file_store_type_id"].(float64) {
	case GetStorageTypeId("file_store_local"):
		return WriteLocalParquet(messageType, schema, payload, matchingConfig)
	case GetStorageTypeId("file_store_aws"):
		return WriteAWSParquet(messageType, schema, payload, matchingConfig)
	case GetStorageTypeId("file_store_gcp"):
		return WriteGCPParquet(messageType, schema, payload, matchingConfig)
	case GetStorageTypeId("file_store_azure"):
		return WriteAzureParquet(messageType, schema, payload, matchingConfig)
	case GetStorageTypeId("file_store_hdfs"):
		err := WriteHDFSParquet(messageType, schema, payload, matchingConfig)
		if err != nil {
			log.Println("Error writing HDFS file")
			return err
		} else { //need to call HDFS dataset creation now

			return CreateHDFSDataset(messageType, matchingConfig)
		}

	}
	return nil
}

//main stateful function
func Ingest(ctx statefun.Context, message statefun.Message) error {
	var request IncomingMessage
	if err := message.As(IncomingMessageType, &request); err != nil {
		return fmt.Errorf("failed to deserialize incoming message: %w", err)
	}

	if request.MessageType == "rtdl_205" { //this is internal message for refershing configuration cache

		err := LoadConfig()

		if err != nil {
			log.Println(err)
			return err
		}

		return nil
	}

	err := WriteParquet(request)
	if err != nil {

		log.Println("error writing Parquet", err)

	}

	payload, _ := json.Marshal(request.Payload) //convert generic payload structure to JSON string

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

	//load constants from shared files
	err := LoadConstants()
	if err != nil {
		log.Fatal("Unable to load constants ", err)
	}


	//load configuration at the outset
	//should panic if unable to do source
	err = LoadConfig()

	if err != nil {
		log.Fatal("Unable to load configuration ", err)
	}

	err = SetDremioConnection()

	if err != nil {

		log.Fatal("Unable to connect with Dremio ", err)
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
