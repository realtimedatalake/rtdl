package main

import (
	// "database/sql"
	"encoding/json"
	//"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	//"strings"
	"github.com/google/uuid"
	//"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type stream_json struct {
	StreamID                string                 `db:"stream_id" json:"stream_id,omitempty"`
	StreamAltID             string                 `db:"stream_alt_id" json:"stream_alt_id,omitempty"`
	Active                  *bool                  `db:"active" json:"active,omitempty"`
	MessageType             string                 `db:"message_type" json:"message_type,omitempty"`
	FileStoreTypeID         int                    `db:"file_store_type_id" json:"file_store_type_id,omitempty"`
	Region                  string                 `db:"region" json:"region,omitempty"`
	BucketName              string                 `db:"bucket_name" json:"bucket_name,omitempty"`
	FolderName              string                 `db:"folder_name" json:"folder_name,omitempty"`
	PartitionTimeID         int                    `db:"partition_time_id" json:"partition_time_id,omitempty"`
	CompressionTypeID       int                    `db:"compression_type_id" json:"compression_type_id,omitempty"`
	AWSAcessKeyID           string                 `db:"aws_access_key_id" json:"aws_access_key_id,omitempty"`
	AWSSecretAcessKey       string                 `db:"aws_secret_access_key" json:"aws_secret_access_key,omitempty"`
	GCPJsonCredentials      map[string]interface{} `db:"gcp_json_credentials" json:"gcp_json_credentials,omitempty"`
	AzureStorageAccountname string                 `db:"azure_storage_account_name" json:"azure_storage_account_name, omitempty"`
	AzureStorageAccessKey   string                 `db:"azure_storage_access_key" json:"azure_storage_access_key, omitempty"`
	NamenodeHost            string                 `db:"namenode_host" json:"namenode_host, omitempty"`
	NamenodePort            int                    `db:"namenode_port" json:"namenode_port, omitempty"`
	GlueEnabled             *bool                  `db:"glue_enabled" json:"glue_enabled, omitempty"`
	GlueRole                string                 `db:"glue_role" json:"glue_role, omitempty"`
	GlueScheduleCron        string                 `db:"glue_schedule_cron" json:"glue_schedule_chron, omitempty"`
	SnowflakeEnabled        *bool                  `db:"snowflake_enabled" json:"snowflake_enabled, omitempty"`
	SnowflakeAccount        string                 `db:"snowflake_account" json:"snowflake_account, omitempty"`
	SnowflakeUsername       string                 `db:"snowflake_username" json:"snowflake_username, omitempty"`
	SnowflakePassword       string                 `db:"snowflake_password" json:"snowflake_password, omitempty"`
	SnowflakeDatabase       string                 `db:"snowflake_database" json:"snowflake_database, omitempty"`
	Functions               string                 `db:"functions" json:"functions, omitempty"`
}

//	FUNCTION
// 	main
//	created by Gavin
//	on 20220109
//	last updated 20220111
//	by Gavin
//	Description:	main exposes port 80 and creates endpoints that point to handler
//					functions to facilitate managment of data streams into your data
//					lake.
func main() {

	// Add handler functions
	http.HandleFunc("/getStream", getStreamHandler())                           // POST; `stream_id` required
	http.HandleFunc("/getAllStreams", getAllStreamsHandler())                   // GET
	http.HandleFunc("/getAllActiveStreams", getAllActiveStreamsHandler())       //GET
	http.HandleFunc("/createStream", createStreamHandler())                     // POST; `message_type` and `folder_name` required
	http.HandleFunc("/updateStream", updateStreamHandler())                     // PUT; all fields required (will replace all fields)
	http.HandleFunc("/deleteStream", deleteStreamHandler())                     // DELETE; `stream_id` required
	http.HandleFunc("/activateStream", activateStreamHandler())                 // PUT; `stream_id` required
	http.HandleFunc("/deactivateStream", deactivateStreamHandler())             // PUT; `stream_id` required
	http.HandleFunc("/getAllFileStoreTypes", getAllFileStoreTypesHandler())     // GET
	http.HandleFunc("/getAllPartitionTimes", getAllPartitionTimesHandler())     // GET
	http.HandleFunc("/getAllCompressionTypes", getAllCompressionTypesHandler()) // GET

	// Run the web server
	log.Fatal(http.ListenAndServe(":80", nil))
}

////////// HANDLER FUNCTIONS - Start //////////
func getStreamHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}

			if reqStream.StreamID != "" {

				configJson, err := ioutil.ReadFile("configs/" + reqStream.StreamID + ".json")
				if err != nil {
					wrt.WriteHeader(http.StatusBadRequest)
					http.Error(wrt, "Bad Request", http.StatusBadRequest)
					CheckError(err)
				}
				if len(configJson) <= 0 {
					wrt.WriteHeader(http.StatusNoContent)
				} else {
					wrt.WriteHeader(http.StatusOK)
					wrt.Write(configJson)
				}
			} else {
				http.Error(wrt, "`stream_id` is required", http.StatusUnprocessableEntity)
			}
		case http.MethodGet:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllStreamsHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			streamConfigs := make([]map[string]interface{}, 0)
			configFiles, err := ioutil.ReadDir("configs")
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			//read all the config files and load them into the array of map[string]interface{}
			for _, configFile := range configFiles {

				configString, err2 := ioutil.ReadFile("configs/" + configFile.Name())
				if err2 != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}
				var configObject map[string]interface{}
				json.Unmarshal(configString, &configObject)
				streamConfigs = append(streamConfigs, configObject)

			}
			log.Println("No. of configs loaded " + strconv.Itoa(len(streamConfigs)))
			if len(streamConfigs) <= 0 {
				wrt.WriteHeader(http.StatusNoContent)
			} else {
				jsonData, err := json.MarshalIndent(streamConfigs, "", "    ")
				if err != nil {
					jsonData = nil
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(jsonData)
			}
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllActiveStreamsHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:

			streamConfigs := make([]map[string]interface{}, 0)
			configFiles, err := ioutil.ReadDir("configs")

			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}
			//read all the config files and load them into the array of map[string]interface{}
			for _, configFile := range configFiles {

				configString, err2 := ioutil.ReadFile("configs/" + configFile.Name())
				if err2 != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}
				var configObject map[string]interface{}
				json.Unmarshal(configString, &configObject)
				if configObject["active"].(bool) {
					streamConfigs = append(streamConfigs, configObject)
				}

			}

			if len(streamConfigs) <= 0 {
				wrt.WriteHeader(http.StatusNoContent)
			} else {
				jsonData, err := json.MarshalIndent(streamConfigs, "", "    ")
				if err != nil {
					jsonData = nil
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(jsonData)
			}
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func createStreamHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			//generate UUID and persist
			id := uuid.New()
			reqStream.StreamID = id.String()

			//persist and also send back updated JSON
			resp, errRet := json.MarshalIndent(reqStream, "", "    ")
			if errRet == nil {
				errRet = ioutil.WriteFile("configs/"+reqStream.StreamID+".json", resp, 0644)
			}
			if errRet != nil {
				resp = nil
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			wrt.WriteHeader(http.StatusOK)
			wrt.Write(resp)

			// Refresh the cache on the `ingest` service
			refreshIngestCache()
		case http.MethodGet:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func updateStreamHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {

		switch req.Method {
		case http.MethodPut:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			log.Println(reqStream.Active)
			resp, errRet := json.MarshalIndent(reqStream, "", "    ")
			if errRet == nil {
				errRet = ioutil.WriteFile("configs/"+reqStream.StreamID+".json", resp, 0644)
			}
			if errRet != nil {
				resp = nil
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(resp)

			// Refresh the cache on the `ingest` service
			refreshIngestCache()
		case http.MethodGet:
		case http.MethodPost:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func deleteStreamHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodDelete:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			if reqStream.StreamID != "" {

				configJson, err := ioutil.ReadFile("configs/" + reqStream.StreamID + ".json")
				if err != nil {
					wrt.WriteHeader(http.StatusBadRequest)
					http.Error(wrt, "Bad Request", http.StatusBadRequest)
					CheckError(err)
				}

				err = os.Remove("configs/" + reqStream.StreamID + ".json")
				if err != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(configJson)

				// Refresh the cache on the `ingest` service
				refreshIngestCache()
			} else {
				http.Error(wrt, "`stream_id` is required", http.StatusUnprocessableEntity)
			}
		case http.MethodGet:
		case http.MethodPost:
		case http.MethodPut:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func activateStreamHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			if reqStream.StreamID != "" {
				configString, err2 := ioutil.ReadFile("configs/" + reqStream.StreamID + ".json")
				if err2 != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Bad Request", http.StatusBadRequest)
					CheckError(err)
				}
				var configObject map[string]interface{}
				json.Unmarshal(configString, &configObject)
				configObject["active"] = true

				jsonData, err := json.MarshalIndent(configObject, "", "    ")
				if err != nil {
					jsonData = nil
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}

				errRet := ioutil.WriteFile("configs/"+reqStream.StreamID+".json", jsonData, 0644)

				if errRet != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)

				} else {
					wrt.WriteHeader(http.StatusOK)
					wrt.Write(jsonData)
				}

				// Refresh the cache on the `ingest` service
				refreshIngestCache()
			} else {
				http.Error(wrt, "`stream_id` is required", http.StatusUnprocessableEntity)
			}
		case http.MethodGet:
		case http.MethodPost:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func deactivateStreamHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Bad Request", http.StatusBadRequest)
				CheckError(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				wrt.WriteHeader(http.StatusInternalServerError)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			if reqStream.StreamID != "" {
				configString, err2 := ioutil.ReadFile("configs/" + reqStream.StreamID + ".json")
				if err2 != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Bad Request", http.StatusBadRequest)
					CheckError(err)
				}
				var configObject map[string]interface{}
				json.Unmarshal(configString, &configObject)
				configObject["active"] = false
				jsonData, err := json.MarshalIndent(configObject, "", "    ")
				if err != nil {
					jsonData = nil
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)
				}

				errRet := ioutil.WriteFile("configs/"+reqStream.StreamID+".json", jsonData, 0644)
				if errRet != nil {
					wrt.WriteHeader(http.StatusInternalServerError)
					http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
					CheckError(err)

				} else {
					wrt.WriteHeader(http.StatusOK)
					wrt.Write(jsonData)

				}

				// Refresh the cache on the `ingest` service
				refreshIngestCache()
			} else {
				http.Error(wrt, "`stream_id` is required", http.StatusUnprocessableEntity)
			}
		case http.MethodGet:
		case http.MethodPost:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllFileStoreTypesHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:

			storageTypesConstants, err := ioutil.ReadFile("constants/file_store_types.json")
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			if len(storageTypesConstants) <= 0 {
				wrt.WriteHeader(http.StatusNoContent)
			} else {
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(storageTypesConstants)
			}
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllPartitionTimesHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:

			partitionTimesConstants, err := ioutil.ReadFile("constants/partition_times.json")
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}
			if len(partitionTimesConstants) <= 0 {
				wrt.WriteHeader(http.StatusNoContent)
			} else {
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(partitionTimesConstants)
			}
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllCompressionTypesHandler() func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:

			compressionTypesConstants, err := ioutil.ReadFile("constants/compression_types.json")
			if err != nil {
				wrt.WriteHeader(http.StatusBadRequest)
				http.Error(wrt, "Internal Server Error", http.StatusInternalServerError)
				CheckError(err)
			}

			if len(compressionTypesConstants) <= 0 {
				wrt.WriteHeader(http.StatusNoContent)
			} else {
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(compressionTypesConstants)
			}
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

////////// HANDLER FUNCTIONS - End //////////

////////// HELPER FUNCTIONS - Start //////////

////////// HELPER FUNCTIONS - Start //////////
//	FUNCTION
// 	refreshIngestCacher
//	created by Gavin
//	on 20220111
//	last updated 20220111
//	by Gavin
//	Description:	Calls the `refreshCache` endpoint on the `ingest` service
func refreshIngestCache() {
	refreshCacheResp, err := http.Get("http://ingest:8080/refreshCache")
	if err != nil {
		CheckError(err)
	}
	if refreshCacheResp != nil {

	}
}

func CheckError(err error) {
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

////////// HELPER FUNCTIONS - End //////////
