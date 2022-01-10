package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	db_host_def     = "rtdl-db"
	db_port_def     = 5433
	db_user_def     = "rtdl"
	db_password_def = "rtdl"
	db_dbname_def   = "rtdl_db"
)

type fileStoreType struct {
	FileStoreTypeID   int    `db:"file_store_type_id" json:"file_store_type_id,omitempty"`
	FileStoreTypeName string `db:"file_store_type_name" json:"file_store_type_name,omitempty"`
}

type partitionTime struct {
	PartitionTimeId   int    `db:"partition_time_id" json:"partition_time_id,omitempty"`
	PartitionTimeName string `db:"partition_time_name" json:"partition_time_name,omitempty"`
}

type compressionType struct {
	CompressionTypeId   int    `db:"compression_type_id" json:"compression_type_id,omitempty"`
	CompressionTypeName string `db:"compression_type_name" json:"compression_type_name,omitempty"`
}

type stream_json struct {
	StreamID          string `db:"stream_id" json:"stream_id,omitempty"`
	StreamAltID       string `db:"stream_alt_id" json:"stream_alt_id,omitempty"`
	Active            bool   `db:"active" json:"active,omitempty"`
	MessageType       string `db:"message_type" json:"message_type,omitempty"`
	FileStoreTypeID   int    `db:"file_store_type_id" json:"file_store_type_id,omitempty"`
	Region            string `db:"region" json:"region,omitempty"`
	BucketName        string `db:"bucket_name" json:"bucket_name,omitempty"`
	FolderName        string `db:"folder_name" json:"folder_name,omitempty"`
	PartitionTimeID   int    `db:"partition_time_id" json:"partition_time_id,omitempty"`
	CompressionTypeID int    `db:"compression_type_id" json:"compression_type_id,omitempty"`
	IamARN            string `db:"iam_arn" json:"iam_arn,omitempty"`
	Credentials       string `db:"credentials" json:"credentials,omitempty"`
}

type stream_sql struct {
	StreamID          sql.NullString `db:"stream_id" json:"stream_id,omitempty"`
	StreamAltID       sql.NullString `db:"stream_alt_id" json:"stream_alt_id,omitempty"`
	Active            sql.NullBool   `db:"active" json:"active,omitempty"`
	MessageType       sql.NullString `db:"message_type" json:"message_type,omitempty"`
	FileStoreTypeID   sql.NullInt64  `db:"file_store_type_id" json:"file_store_type_id,omitempty"`
	Region            sql.NullString `db:"region" json:"region,omitempty"`
	BucketName        sql.NullString `db:"bucket_name" json:"bucket_name,omitempty"`
	FolderName        sql.NullString `db:"folder_name" json:"folder_name,omitempty"`
	PartitionTimeID   sql.NullInt64  `db:"partition_time_id" json:"partition_time_id,omitempty"`
	CompressionTypeID sql.NullInt64  `db:"compression_type_id" json:"compression_type_id,omitempty"`
	IamARN            sql.NullString `db:"iam_arn" json:"iam_arn,omitempty"`
	Credentials       sql.NullString `db:"credentials" json:"credentials,omitempty"`
}

func main() {

	// connection string
	psqlconn := getDBConnectionString()
	// open database
	db, err := sqlx.Open("postgres", psqlconn)
	if err != nil {
		CheckError(err)
	}
	// close database
	defer db.Close()

	// Add handler functions
	http.HandleFunc("/getStream", getStreamHandler(db))                           // POST; `stream_id` required
	http.HandleFunc("/getAllStreams", getAllStreamsHandler(db))                   // GET
	http.HandleFunc("/getAllActiveStreams", getAllActiveStreamsHandler(db))       //GET
	http.HandleFunc("/createStream", createStreamHandler(db))                     // POST; `message_type` and `folder_name` required
	http.HandleFunc("/updateStream", updateStreamHandler(db))                     // PUT; all fields required (will replace all fields)
	http.HandleFunc("/deleteStream", deleteStreamHandler(db))                     // DELETE; `stream_id` required
	http.HandleFunc("/activateStream", activateStreamHandler(db))                 // PUT; `stream_id` required
	http.HandleFunc("/deactivateStream", deactivateStreamHandler(db))             // PUT; `stream_id` required
	http.HandleFunc("/getAllFileStoreTypes", getAllFileStoreTypesHandler(db))     // GET
	http.HandleFunc("/getAllPartitionTimes", getAllPartitionTimesHandler(db))     // GET
	http.HandleFunc("/getAllCompressionTypes", getAllCompressionTypesHandler(db)) // GET

	// Run the web server
	log.Fatal(http.ListenAndServe(":80", nil))
}

////////// HANDLER FUNCTIONS - Start //////////
func getStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}

			// Query database
			streams := []stream_sql{}
			queryStr := "select * from getStream("
			if reqStream.StreamID != "" {
				queryStr = queryStr + "'" + reqStream.StreamID + "')"
				err := db.Select(&streams, queryStr)
				if err != nil {
					fmt.Println("Error fetching stream record")
					CheckError(err)
				}
				jsonData, err := json.MarshalIndent(streams, "", "    ")
				if err != nil {
					jsonData = nil
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(jsonData)
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

func getAllStreamsHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			streams := []stream_sql{}
			err := db.Select(&streams, "select * from getAllStreams()")
			if err != nil {
				fmt.Println("Error fetching stream records")
				CheckError(err)
			}
			jsonData, err := json.MarshalIndent(streams, "", "    ")
			if err != nil {
				jsonData = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(jsonData)
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllActiveStreamsHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			streams := []stream_sql{}
			err := db.Select(&streams, "select * from getAllActiveStreams()")
			if err != nil {
				fmt.Println("Error fetching stream records")
				CheckError(err)
			}
			jsonData, err := json.MarshalIndent(streams, "", "    ")
			if err != nil {
				jsonData = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(jsonData)
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func createStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}

			// Send to database function
			retStreams := []stream_sql{}
			queryStr := buildQueryString_createStream(reqStream)
			err = db.Select(&retStreams, queryStr)
			if err != nil {
				fmt.Println("Error creating stream record")
				CheckError(err)
			}
			resp, errRet := json.MarshalIndent(retStreams, "", "    ")
			if errRet != nil {
				resp = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(resp)
		case http.MethodGet:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func updateStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}

			// Send to database function
			retStreams := []stream_sql{}
			queryStr := buildQueryString_updateStream(reqStream)
			err = db.Select(&retStreams, queryStr)
			if err != nil {
				fmt.Println("Error updating stream record")
				CheckError(err)
			}
			resp, errRet := json.MarshalIndent(retStreams, "", "    ")
			if errRet != nil {
				resp = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(resp)
		case http.MethodGet:
		case http.MethodPost:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func deleteStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodDelete:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}

			// Query database
			streams := []stream_sql{}
			queryStr := "select * from deleteStream("
			if reqStream.StreamID != "" {
				queryStr = queryStr + "'" + reqStream.StreamID + "')"
				err := db.Select(&streams, queryStr)
				if err != nil {
					fmt.Println("Error deleting stream record")
					CheckError(err)
				}
				jsonData, err := json.MarshalIndent(streams, "", "    ")
				if err != nil {
					jsonData = nil
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(jsonData)
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

func activateStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}

			// Query database
			streams := []stream_sql{}
			queryStr := "select * from activateStream("
			if reqStream.StreamID != "" {
				queryStr = queryStr + "'" + reqStream.StreamID + "')"
				err := db.Select(&streams, queryStr)
				if err != nil {
					fmt.Println("Error activating stream record")
					CheckError(err)
				}
				jsonData, err := json.MarshalIndent(streams, "", "    ")
				if err != nil {
					jsonData = nil
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(jsonData)
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

func deactivateStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			// Read json
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			var reqStream stream_json
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}

			// Query database
			streams := []stream_sql{}
			queryStr := "select * from deactivateStream("
			if reqStream.StreamID != "" {
				queryStr = queryStr + "'" + reqStream.StreamID + "')"
				err := db.Select(&streams, queryStr)
				if err != nil {
					fmt.Println("Error deactivating stream record")
					CheckError(err)
				}
				jsonData, err := json.MarshalIndent(streams, "", "    ")
				if err != nil {
					jsonData = nil
					CheckError(err)
				}
				wrt.WriteHeader(http.StatusOK)
				wrt.Write(jsonData)
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

func getAllFileStoreTypesHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			fst := []fileStoreType{}
			err := db.Select(&fst, "select * from getAllFileStoreTypes()")
			if err != nil {
				fmt.Println("Error fetching file_store_type records")
				CheckError(err)
			}
			jsonData, err := json.MarshalIndent(fst, "", "    ")
			if err != nil {
				jsonData = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(jsonData)
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllPartitionTimesHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			pt := []partitionTime{}
			err := db.Select(&pt, "select * from getAllPartitionTimes()")
			if err != nil {
				fmt.Println("Error fetching partition_time records")
				CheckError(err)
			}
			jsonData, err := json.MarshalIndent(pt, "", "    ")
			if err != nil {
				jsonData = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(jsonData)
		case http.MethodPost:
		case http.MethodPut:
		case http.MethodDelete:
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllCompressionTypesHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			ct := []compressionType{}
			err := db.Select(&ct, "select * from getAllCompressionTypes()")
			if err != nil {
				fmt.Println("Error fetching compression_type records")
				CheckError(err)
			}
			jsonData, err := json.MarshalIndent(ct, "", "    ")
			if err != nil {
				jsonData = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(jsonData)
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
func getDBConnectionString() (psqlconn string) {
	var db_host, db_port, db_user, db_password, db_dbname = db_host_def, db_port_def, db_user_def, db_password_def, db_dbname_def
	var db_host_env, db_user_env, db_password_env, db_dbname_env = os.Getenv("RTDL_DB_HOST"), os.Getenv("RTDL_DB_USER"), os.Getenv("RTDL_DB_PASSWORD"), os.Getenv("RTDL_DB_DBNAME")
	db_port_env, err := strconv.Atoi(os.Getenv("RTDL_DB_PORT"))
	if err != nil {
		db_port_env = 0
	}

	if db_host_env != "" {
		db_host = db_host_env
	}
	if db_port_env != 0 {
		db_port = db_port_env
	}
	if db_user_env != "" {
		db_user = db_user_env
	}
	if db_password_env != "" {
		db_password = db_password_env
	}
	if db_dbname_env != "" {
		db_dbname = db_dbname_env
	}
	psqlconn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_host, db_port, db_user, db_password, db_dbname)
	return
}

func buildQueryString_createStream(reqStream stream_json) (queryStr string) {
	queryStr = "select * from createStream("
	if reqStream.StreamAltID != "" {
		queryStr = queryStr + "'" + reqStream.StreamAltID + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	queryStr = queryStr + strconv.FormatBool(reqStream.Active) + ", "
	if reqStream.MessageType != "" {
		queryStr = queryStr + "'" + reqStream.MessageType + "', "
	} else {
		// TODO: Required field. Return error.
		queryStr = queryStr + "NULL, "
	}
	if reqStream.FileStoreTypeID < 1 {
		reqStream.FileStoreTypeID = 1
	}
	queryStr = queryStr + strconv.Itoa(reqStream.FileStoreTypeID) + ", "
	if reqStream.Region != "" {
		queryStr = queryStr + "'" + reqStream.Region + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	if reqStream.BucketName != "" {
		queryStr = queryStr + "'" + reqStream.BucketName + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	if reqStream.FolderName != "" {
		queryStr = queryStr + "'" + reqStream.FolderName + "', "
	} else {
		// TODO: Required field. Return error.
		queryStr = queryStr + "NULL, "
	}
	if reqStream.PartitionTimeID < 1 {
		reqStream.PartitionTimeID = 1
	}
	queryStr = queryStr + strconv.Itoa(reqStream.PartitionTimeID) + ", "
	if reqStream.CompressionTypeID < 1 {
		reqStream.CompressionTypeID = 1
	}
	queryStr = queryStr + strconv.Itoa(reqStream.CompressionTypeID) + ", "
	if reqStream.IamARN != "" {
		queryStr = queryStr + "'" + reqStream.IamARN + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	if reqStream.Credentials != "" {
		queryStr = queryStr + "'" + reqStream.Credentials + "')"
	} else {
		queryStr = queryStr + "NULL)"
	}

	return queryStr
}

func buildQueryString_updateStream(reqStream stream_json) (queryStr string) {
	queryStr = "select * from updateStream("
	if reqStream.StreamID != "" {
		queryStr = queryStr + "'" + reqStream.StreamID + "', "
	} else {
		// TODO: Required field. Return error.
		queryStr = queryStr + "NULL, "
	}
	if reqStream.StreamAltID != "" {
		queryStr = queryStr + "'" + reqStream.StreamAltID + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	queryStr = queryStr + strconv.FormatBool(reqStream.Active) + ", "
	if reqStream.MessageType != "" {
		queryStr = queryStr + "'" + reqStream.MessageType + "', "
	} else {
		// TODO: Required field. Return error.
		queryStr = queryStr + "NULL, "
	}
	if reqStream.FileStoreTypeID < 1 {
		reqStream.FileStoreTypeID = 1
	}
	queryStr = queryStr + strconv.Itoa(reqStream.FileStoreTypeID) + ", "
	if reqStream.Region != "" {
		queryStr = queryStr + "'" + reqStream.Region + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	if reqStream.BucketName != "" {
		queryStr = queryStr + "'" + reqStream.BucketName + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	if reqStream.FolderName != "" {
		queryStr = queryStr + "'" + reqStream.FolderName + "', "
	} else {
		// TODO: Required field. Return error.
		queryStr = queryStr + "NULL, "
	}
	if reqStream.PartitionTimeID < 1 {
		reqStream.PartitionTimeID = 1
	}
	queryStr = queryStr + strconv.Itoa(reqStream.PartitionTimeID) + ", "
	if reqStream.CompressionTypeID < 1 {
		reqStream.CompressionTypeID = 1
	}
	queryStr = queryStr + strconv.Itoa(reqStream.CompressionTypeID) + ", "
	if reqStream.IamARN != "" {
		queryStr = queryStr + "'" + reqStream.IamARN + "', "
	} else {
		queryStr = queryStr + "NULL, "
	}
	if reqStream.Credentials != "" {
		queryStr = queryStr + "'" + reqStream.Credentials + "')"
	} else {
		queryStr = queryStr + "NULL)"
	}

	return queryStr
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

////////// HELPER FUNCTIONS - End //////////
