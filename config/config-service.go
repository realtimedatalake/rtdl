package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/lib/pq"
)

const (
	db_host_def     = "rtdl-db"
	db_port_def     = 5433
	db_user_def     = "rtdl"
	db_password_def = "rtdl"
	db_dbname_def   = "rtdl_db"
)

func main() {

	// connection string
	psqlconn := getDBConnectionString()
	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)
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

func getStreamHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
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

func getAllStreamsHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
	// getAllStreams -- N/A
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
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

func getAllActiveStreamsHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
	// getAllActiveStreams -- N/A
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
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

func createStreamHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
	// createStream -- active, streamAltId, fileStoreType, region (AWS), bucket(AWS, GCP), folder, IAM ARN (AWS w/ IAM), credentials JSON (GCP)
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

func updateStreamHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
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

func deleteStreamHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
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

func getAllFileStoreTypesHandler(db *sql.DB) func(http.ResponseWriter, *http.Request) {
	// getAllFileStoreTypes -- N/A
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			rows, err := db.Query(`select * from getAllFileStoreTypes()`)
			if err != nil {
				fmt.Println("Error fetching rows")
				CheckError(err)
			}
			jsonData := sqlRowsToJSON(rows)
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

func sqlRowsToJSON(rows *sql.Rows) (jsonData []byte) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		CheckError(err)
	}

	count := len(columnTypes)
	finalRows := []interface{}{}

	for rows.Next() {
		scanArgs := make([]interface{}, count)

		for i, v := range columnTypes {
			switch v.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
				scanArgs[i] = new(sql.NullString)
				break
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
				break
			case "INT4":
				scanArgs[i] = new(sql.NullInt64)
				break
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			CheckError(err)
		}

		masterData := map[string]interface{}{}

		for i, v := range columnTypes {
			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				masterData[v.Name()] = z.Bool
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name()] = z.String
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name()] = z.Int64
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[v.Name()] = z.Float64
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				masterData[v.Name()] = z.Int32
				continue
			}
			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	z, err := json.MarshalIndent(finalRows)
	if err != nil {
		jsonData = nil
		CheckError(err)
	} else {
		jsonData = z
	}

	return
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
