# rtdl - The Real-Time Data Lake
rtdl makes it easy to build and maintain a real-time data lake. You configure a data stream 
with a source (from a tool like Segment) and a cloud storage destination, and rtdl builds you 
a real-time data lake in Parquet format cataloged in Apache Hive Metastore – so you can access 
your real-time data with common BI and ML tools. You provide the streams, rtdl builds your lake.

## Quickstart
1. Run `docker compose -f docker-compose.init.yml up -d`.
    * **Note:** This configuration should be fault-tolerant, but if any containers or 
    processes fail when running this, run `docker compose -f docker-compose.init.yml down` 
    and retry.
2.  After containers `rtdl_rtdl-db-init`, `rtdl_catalog-db-init`, and `rtdl_catalog-init` 
    exit and complete with `EXITED (0)`, kill and delete the rtdl container set by running 
    `docker compose -f docker-compose.init.yml down`
3. Run `docker compose up -d` every time after.
    * `docker compose down` to stop.

**Note:** To start from scratch, first run the below commands from the rtdl root folder.
```
% rm -rf storage/
% docker image rm rtdl/rtdl-config rtdl/rtdl-ingest rtdl/process-stateful-function
``` 

## Architecture
rtdl has a multi-service architecture composed of tested and trusted open source tools 
to process and catalog your data and custom-built services to interact with them more easily.

### config services

#### config
API service written in Go. Use the API to create, read, update, acvitivate, deactivate, 
and delete `stream` records. `stream` records store the configuration information for 
the different data streams you want to send to your data lake. This service can also be 
used to lookup master data necessary for creating successful `stream` records like 
`file_store_types`, `partition_times`, and `compression_types`.  
**Environment Variables:** RTDL_DB_HOST, RTDL_DB_USER, RTDL_DB_PASSWORD, RTDL_DB_DBNAME  
**Public Port:** 80  
**Endpoints:**
  * /getStream -- POST; `stream_id` required
  * /getAllStreams -- GET
  * /getAllActiveStreams -- GET
  * /createStream -- POST; `message_type` and `folder_name` required
  * /updateStream -- PUT; all fields required (any missing fields will be replaced with NULL 
    values)
  * /deleteStream -- DELETE; `stream_id` required
  * /activateStream -- PUT; `stream_id` required
  * /deactivateStream -- PUT; `stream_id` required
  * /getAllFileStoreTypes -- GET
  * /getAllPartitionTimes -- GET
  * /getAllCompressionTypes -- GET

#### rtdl-db
YugabyteDB or PostgreSQL (both configurations included in the docker compose files). This service 
stores the `stream` configuration data written by the `config` service and read by the `ingester` 
stateful function  
  * **Database Name:** rtdl_db
  * **Username:** rtdl
  * **Password:** rtdl

**Tables**
  * file_store_types
    * file_store_type_id SERIAL,
    * file_store_type_name VARCHAR,
    * PRIMARY KEY (file_store_type_id)
  * partition_times
    * partition_time_id SERIAL,
    * partition_time_name VARCHAR,
    * PRIMARY KEY (partition_time_id)
  * compression_types
    * compression_type_id SERIAL,
    * compression_type_name VARCHAR,
    * PRIMARY KEY (compression_type_id)
  * streams
    * stream_id uuid DEFAULT gen_random_uuid(),
    * stream_alt_id VARCHAR,
    * active BOOLEAN DEFAULT FALSE,
    * message_type VARCHAR NOT NULL,
    * file_store_type_id INTEGER DEFAULT 1,
    * region VARCHAR,
    * bucket_name VARCHAR,
    * folder_name VARCHAR NOT NULL,
    * partition_time_id INTEGER DEFAULT 1,
    * compression_type_id INTEGER DEFAULT 1,
    * aws_access_key_id VARCHAR,
    * aws_secret_access_key VARCHAR,
    * gcp_json_credentials VARCHAR,
    * created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    * updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    * PRIMARY KEY (stream_id),
    * FOREIGN KEY(file_store_type_id) REFERENCES file_store_types(file_store_type_id),
    * FOREIGN KEY(partition_time_id) REFERENCES partition_times(partition_time_id),
    * FOREIGN KEY(compression_type_id) REFERENCES compression_types(compression_type_id)

### ingest service
Service written in Go that accepts a JSON payload and writes it to Kafka for processing by the 
`ingester` stateful function. 
**Public Port:** 8080  
**Endpoints:**
  * /ingest -- POST; accepts JSON payload along with a write key
  * /refreshCache -- GET; triggers a refresh of the streams cache in the `ingester` stateful function

### kafka services
Standard Kafka services. Creates data streams that can be read by a Stateful Function. Images from Bitnami.
  * kafka-zookeeper - Apache Zookeeper service
  * kafka - Apache Kafka service

### process services
Apache Flink [Stateful Functions](https://flink.apache.org/stateful-functions.html) cluster in a standard 
configuration – a job manager service with paired task manager and stateful function services.
  * statefun-manager - Apache Flink Stateful Functions manager service
  * statefun-worker - Apache Flink Stateful Functions task manager service
  * statefun-functions - Apache Flink Stateful function written in Go named `ingester`. Reads JSON 
  payloads posted to Kafka, processes and stores the data in Parque format based on the configuration 
  in the associated streams record.
    * **Environment Variables:** RTDL_DB_HOST, RTDL_DB_USER, RTDL_DB_PASSWORD, RTDL_DB_DBNAME

### catalog services
Apache Hive Standalone Metastore containerized and backed by a PostgreSQL-compatible database.
  * catalog - Apache Hive Standalone Metastore service built from the most recent release of the [Hive 
  Standalone Metastore on on Maven](https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/).
  * catalog-db - YugabyteDB or PostgreSQL (both configurations included in the docker compose files). This 
  service stores all of the data required by Apache Hive Standalone Metastore.
    * **Database Name:** rtdl_catalog_db
    * **Username:** rtdl
    * **Password:** rtdl

