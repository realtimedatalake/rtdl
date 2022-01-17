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
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"io"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/oauth2/google"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"google.golang.org/api/option"
	"cloud.google.com/go/storage"
	
)

//Incoming message would have
// - a source key to identify the stream
// - a message type that can be used to indicate the message purpose
// - generic payload

type IncomingMessage struct {
	SourceKey   string                 `json:"source_key"`
	MessageType string                 `json:"message_type"`
	Payload     map[string]interface{} `json:"payload"`
}

//struct representation of stream configuration
// StreamAltID is applicable where the stream is being fed from an external system and the alternate id
// represents the unique identifier for that system
type Config struct {
	StreamId           sql.NullString `db:"stream_id" default:""`
	StreamAltID        sql.NullString `db:"stream_alt_id" default:""`
	Active             sql.NullBool   `db:"active"`
	MessageType        sql.NullString `db:"message_type" default:""`
	FileStoreTypeId    sql.NullInt64  `db:"file_store_type_id"`
	Region             sql.NullString `db:"region" default:""`
	BucketName         sql.NullString `db:"bucket_name" default:""`
	FolderName         sql.NullString `db:"folder_name" default:""`
	PartitionTimeId    sql.NullInt64  `db:"partition_time_id"`
	CompressionTypeId  sql.NullInt64  `db:"compression_type_id"`
	AWSAcessKeyID      sql.NullString `db:"aws_access_key_id" default:""`
	AWSSecretAcessKey  sql.NullString `db:"aws_secret_access_key" default:""`
	GCPJsonCredentials sql.NullString `db:"gcp_json_credentials" default:""`
	CreatedAt          time.Time      `db:"created_at"`
	UpdatedAt          time.Time      `db:"updated_at"`
}

var configs []Config

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

// getEnv get key environment variable if exist otherwise return defalutValue
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

//loads all stream configurations
func loadConfig() error {

	//directly load data from PostgreSQL
	//connection parameters read from docker-compose.yml, defaults mentioned here
	pghost := getEnv("POSTGRES_HOST", "localhost")
	pgport, err := strconv.Atoi(getEnv("POSTGRES_PORT", "5432"))
	if err != nil {
		pgport = 5432
	}
	pguser := getEnv("POSTGRES_USER", "postgres")
	pgpassword := getEnv("POSTGRES_PASSWORD", "postgres")
	pgdbname := getEnv("POSTGRES_DBNAME", "postgres")

	dsn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable", pguser, pgpassword, pghost, pgport, pgdbname)

	db, err := sqlx.Connect("postgres", dsn)

	if err != nil {
		log.Println("Failed to open a DB connection: ", err)
		return err
	}

	configSql := "SELECT * FROM streams"

	err = db.Select(&configs, configSql) //populate stream configurations into array of stream config structs
	if err != nil {
		log.Println("Failed to execute query: ", err)
		return err
	}

	fileStoreTypeSql := "SELECT * FROM file_store_types"
	err = db.Select(&fileStoreTypes, fileStoreTypeSql) //populate supported file store types
	if err != nil {
		log.Println("Failed to execute query: ", err)
		return err
	}

	partitionTimesSql := "SELECT * FROM partition_times"
	err = db.Select(&partitionTimes, partitionTimesSql) //populate supported file store types
	if err != nil {
		log.Println("Failed to execute query: ", err)
		return err
	}

	compressionTypesSql := "SELECT * from compression_types"
	err = db.Select(&compressionTypes, compressionTypesSql)
	if err != nil {
		log.Println("Failed to execute query: ", err)
		return err
	}

	defer db.Close()
	log.Println("No. of config records retrieved : " + strconv.Itoa(len(configs)))
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

	}
	return ""

}

//use reflection to study incoming generic payload and construct schema necessary for Parquet
//payload is passed recursively through the function to break down till the elemental level

func generateSchema(payload map[string]interface{}, messageType string, jsonSchema string) string {

	if jsonSchema == "" {
		jsonSchema = `{"Tag": "name=` + messageType + `, repetitiontype=REQUIRED",`
		jsonSchema += `"Fields": [`
	}

	for key, value := range payload {

		jsonSchema += `{"Tag": "name=` + key

		dataType := reflect.TypeOf(value).String()
		//log.Println(fmt.Sprintf("%s is %s",key, dataType))

		//special processing for nested object structures
		if strings.HasPrefix(dataType, "map[string]interface") {

			jsonSchema += `, repetitiontype=REQUIRED", "Fields" : [`
			jsonSchema = generateSchema(value.(map[string]interface{}), messageType, jsonSchema) //need recursion
			jsonSchema = strings.TrimRight(jsonSchema, ",")                                      //remove trailing comma
			jsonSchema += `]},`

		} else if strings.HasPrefix(dataType, "[]interface") { //special processing for arrays as well

			jsonSchema += `, type=LIST, repetitiontype=REQUIRED", "Fields" : [`
			arrayItemDataType := reflect.TypeOf(value.([]interface{})[0]).String()
			if strings.HasPrefix(arrayItemDataType, "map[string]interface") { //if array consists of objects then same have to be recursed

				jsonSchema = generateSchema(value.([]interface{})[0].(map[string]interface{}), messageType, jsonSchema)

			} else { //arrays composed of native data types can be handled directly

				jsonSchema += `{"Tag": "name=element, type=` + getParquetDataType(reflect.TypeOf(value.([]interface{})[0]).String())
				jsonSchema += `, repetitiontype=REQUIRED"},`
			}
			jsonSchema = strings.TrimRight(jsonSchema, ",")
			jsonSchema += `]},`
		} else { //native data type

			jsonSchema += `, type=` + getParquetDataType(dataType)
			jsonSchema += `, repetitiontype=REQUIRED"},`

		}

	}

	return jsonSchema

}

func generateSubFolderName(messageType string, configRecord Config) string {

	var subFolderName string

	for _, partitionTimeRecord := range partitionTimes { //need to find out the write partition

		if partitionTimeRecord.PartitionTimeId == configRecord.PartitionTimeId.Int64 { //match found

			switch partitionTimeRecord.PartitionTimeName {

			case "Hourly":

				subFolderName = messageType + "_" + time.Now().Format("2006-01-02-15")

			case "Daily":
				subFolderName = messageType + "_" + time.Now().Format("2006-01-02")

			case "Weekly":
				year, week := time.Now().ISOWeek()
				subFolderName = messageType + "_" + string(year) + "-" + string(week)

			case "Monthly":
				subFolderName = messageType + "_" + time.Now().Format("2006-01")

			case "Quarterly":
				quarter := int((time.Now().Month() + 2) / 3)
				subFolderName = messageType + "_" + time.Now().Format("2006") + "-" + string(quarter)
			}

		}

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
func WriteToFile(schema string, fw source.ParquetFile, payload []byte) error {

	log.Println("payload : ", string(payload))

	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		log.Println("Can't create json writer", err)
		return err
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

//Write local Parquet
func WriteLocalParquet(messageType string, schema string, payload []byte, configRecord Config) error {

	//write
	folderName := configRecord.FolderName.String
	if folderName == "" { //default
		folderName = "datastore"
	}

	folderName += "/" + generateSubFolderName(messageType, configRecord)

	err := os.MkdirAll(folderName, os.ModePerm)
	if err != nil {
		log.Println("Can't create output directory", err)
		return err
	}

	fileName := folderName + "/" + generateLeafLevelFileName()

	fw, err := local.NewLocalFileWriter(fileName)

	if err != nil {
		log.Println("Can't create file", err)
		return err
	}

	return WriteToFile(schema, fw, payload)

}

func WriteAWSParquet(messageType string, schema string, payload []byte, configRecord Config) error {

	var key string

	subFolderName := generateSubFolderName(messageType, configRecord)
	leafLevelFileName := generateLeafLevelFileName()

	if configRecord.Region.String == "" {
		return errors.New("AWS Region cannot be null or empty")
	}

	region := strings.TrimSpace(configRecord.Region.String)
	awsAccessKeyId := strings.TrimSpace(configRecord.AWSAcessKeyID.String)
	awsSecretAccessKey := strings.TrimSpace(configRecord.AWSSecretAcessKey.String)
	

	//log.Println("AWS Parquet writing implementation pending")
	bucketName := configRecord.BucketName.String
	if bucketName == "" {
		return errors.New("S3 bucket name cannot be null or empty")
	}

	if configRecord.FolderName.String != "" {

		key = configRecord.FolderName.String + "/" + subFolderName + "/" + leafLevelFileName

	} else {

		key = subFolderName + "/" + leafLevelFileName
	}

	fw, err := local.NewLocalFileWriter(leafLevelFileName)
	err = WriteToFile(schema, fw, payload) //write temporary local file
	if err != nil {
		log.Println("Unable to write temporary local file", err)
		return err
	}

	awsSession, err := session.NewSession(&aws.Config{
											Region: aws.String(region),
											Credentials: credentials.NewStaticCredentials(awsAccessKeyId,awsSecretAccessKey,""),
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

	return err

}

func WriteGCPParquet(messageType string, schema string, payload []byte, configRecord Config) error {

	var path string
	
	subFolderName := generateSubFolderName(messageType, configRecord)
	leafLevelFileName := generateLeafLevelFileName()
	
	//replace all \n	with \\n to preserve them
	jsonCreds := strings.Replace(configRecord.GCPJsonCredentials.String,"\n","\\n",-1) 
	//jsonCreds := configRecord.GCPJsonCredentials.String
	
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
	
	bucketName := configRecord.BucketName.String
	if bucketName == "" {
		return errors.New("GCS bucket name cannot be null or empty")
	}
	
	if configRecord.FolderName.String != "" {

		path = configRecord.FolderName.String + "/" + subFolderName + "/" + leafLevelFileName

	} else {

		path = subFolderName + "/" + leafLevelFileName
	}

	fw, err := local.NewLocalFileWriter(leafLevelFileName)
	err = WriteToFile(schema, fw, payload) //write temporary local file
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
	return nil
}

//Parquet writing logic
func writeParquet(request IncomingMessage) error {

	//log.Println(generateSchema(request.Payload,request.MessageType, "")+"]}")

	schema := strings.TrimRight(generateSchema(request.Payload, request.MessageType, ""), ",") + "]}"

	log.Println(schema)

	payload, _ := json.Marshal(request.Payload) //convert generic payload structure to JSON string

	//first retrieve relevant destination information from config array
	for _, configRecord := range configs {

		if configRecord.StreamId.String == request.SourceKey && configRecord.MessageType.String == request.MessageType { //config found with matching stream id and message type

			for _, fileStoreTypeRecord := range fileStoreTypes { //similar logic for file store types

				if fileStoreTypeRecord.FileStoreTypeId == configRecord.FileStoreTypeId.Int64 {

					switch fileStoreTypeRecord.FileStoreTypeName {
					case "Local":
						return WriteLocalParquet(request.MessageType, schema, payload, configRecord)
					case "AWS":
						return WriteAWSParquet(request.MessageType, schema, payload, configRecord)
					case "GCP":
						return WriteGCPParquet(request.MessageType, schema, payload, configRecord)

					}
				}

			}
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

		err := loadConfig()

		if err != nil {
			log.Println(err)
			return err
		}

		return nil
	}

	err := writeParquet(request)
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

	//load configuration at the outset
	//should panic if unable to do source
	err := loadConfig()

	if err != nil {
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
