package main

import (
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
	//db_host_def = "rtdl-db"
	db_host_def     = "localhost"
	db_port_def     = 5433
	db_user_def     = "rtdl"
	db_password_def = "rtdl"
	db_dbname_def   = "rtdl_db"
)

type fileStoreType struct {
	FileStoreTypeID   int    `db:"file_store_type_id" json:"file_store_type_id,omitempty"`
	FileStoreTypeName string `db:"file_store_type_name" json:"file_store_type_name,omitempty"`
}

type stream struct {
	StreamID          string `db:"stream_id" json:"stream_id,omitempty"`
	StreamAltID       string `db:"stream_alt_id" json:"stream_alt_id,omitempty"`
	Active            bool   `db:"active" json:"active,omitempty"`
	FileStoreTypeID   int    `db:"file_store_type_id" json:"file_store_type_id,omitempty"`
	FileStoreTypeName string `db:"file_store_type_name" json:"file_store_type_name,omitempty"`
	Region            string `db:"region" json:"region,omitempty"`
	BucketName        string `db:"bucket_name" json:"bucket_name,omitempty"`
	FolderName        string `db:"folder_name" json:"folder_name,omitempty"`
	IamARN            string `db:"iam_arn" json:"iam_arn,omitempty"`
	Credentials       string `db:"credentials" json:"credentials,omitempty"`
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
	http.HandleFunc("/getStream", getStreamHandler(db))
	http.HandleFunc("/getAllStreams", getAllStreamsHandler(db))
	http.HandleFunc("/getAllActiveStreams", getAllActiveStreamsHandler(db))
	http.HandleFunc("/createStream", createStreamHandler(db))
	http.HandleFunc("/updateStream", updateStreamHandler(db))
	http.HandleFunc("/deleteStream", deleteStreamHandler(db))
	http.HandleFunc("/getAllFileStoreTypes", getAllFileStoreTypesHandler(db))

	// Run the web server
	log.Fatal(http.ListenAndServe(":80", nil))
}

func getStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	// getStream -- streamId
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		// body, err := ioutil.ReadAll(req.Body)
		// if err != nil {
		// 	log.Fatalln(err)
		// }
		switch req.Method {
		case http.MethodGet:
			// Serve the resource.
		case http.MethodPost:
			// Create a new record.
		case http.MethodPut:
			// Update an existing record.
		case http.MethodDelete:
			// Remove the record.
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllStreamsHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	// getAllStreams -- N/A
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			streams := []stream{}
			err := db.Select(&streams, "select * from getAllStreams()")
			if err != nil {
				fmt.Println("Error fetching rows")
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
	// getAllActiveStreams -- N/A
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			streams := []stream{}
			err := db.Select(&streams, "select * from getAllActiveStreams()")
			if err != nil {
				fmt.Println("Error fetching rows")
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
			// Create a new record.
		case http.MethodPut:
			// Update an existing record.
		case http.MethodDelete:
			// Remove the record.
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func createStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	// createStream -- active, streamAltId, fileStoreType, region (AWS), bucket(AWS, GCP), folder, IAM ARN (AWS w/ IAM), credentials JSON (GCP)
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatalln(err)
			}
			resp := "Received a POST request"
			jsonData, err := json.MarshalIndent(resp, "", "    ")
			if err != nil {
				jsonData = nil
				CheckError(err)
			}
			wrt.WriteHeader(http.StatusOK)
			wrt.Write(jsonData)

			var reqStream stream
			err = json.Unmarshal(body, &reqStream)
			if err != nil {
				CheckError(err)
			}
			fmt.Printf("%s\n", reqStream.Credentials)
		case http.MethodGet:
			// Serve the resource.
		case http.MethodPut:
			// Update an existing record.
		case http.MethodDelete:
			// Remove the record.
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func updateStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	// updateStream -- streamId, active, streamAltId, fileStoreType, region (AWS), bucket(AWS, GCP), folder, IAM ARN (AWS w/ IAM), credentials JSON (GCP)
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		// body, err := ioutil.ReadAll(req.Body)
		// if err != nil {
		// 	log.Fatalln(err)
		// }
		switch req.Method {
		case http.MethodGet:
			// Serve the resource.
		case http.MethodPost:
			// Create a new record.
		case http.MethodPut:
			// Update an existing record.
		case http.MethodDelete:
			// Remove the record.
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func deleteStreamHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	// deleteStream -- streamId
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		// body, err := ioutil.ReadAll(req.Body)
		// if err != nil {
		// 	log.Fatalln(err)
		// }
		switch req.Method {
		case http.MethodGet:
			// Serve the resource.
		case http.MethodPost:
			// Create a new record.
		case http.MethodPut:
			// Update an existing record.
		case http.MethodDelete:
			// Remove the record.
		default:
			wrt.WriteHeader(http.StatusMethodNotAllowed)
			http.Error(wrt, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func getAllFileStoreTypesHandler(db *sqlx.DB) func(http.ResponseWriter, *http.Request) {
	// getAllFileStoreTypes -- N/A
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			fst := []fileStoreType{}
			err := db.Select(&fst, "select * from getAllFileStoreTypes()")
			if err != nil {
				fmt.Println("Error fetching rows")
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

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
