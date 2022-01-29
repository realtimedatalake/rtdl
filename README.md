# rtdl - The Real-Time Data Lake
rtdl makes it easy to build and maintain a real-time data lake. You configure a data stream 
with a source (from a tool like Segment) and a cloud storage destination, and rtdl builds you 
a real-time data lake in Parquet format cataloged in Apache Hive Metastore – so you can access 
your real-time data with common BI and ML tools. You provide the streams, rtdl builds your lake.

## V0.0.1 - Current status -- what works and what doesn't

### What works?
rtdl is not full-featured yet, but it is currently functional. You can configure streams that 
ingest json from an rtdl endpoint, process them into Parquet, and save the files to a destination 
configured in your stream. rtdl can write files locally, to AWS S3, and to GCP Cloud Storage.

### What doesn't work/what's next on the roadmap?
  * Add CONTRIBUTING.md and a contributor license agreement
  * Cataloging data in Hive Metastore
    * This will let you use your data with a much broader range of data tools.
  * Adding Presto to the stack
    * This will make connecting into a whole ecosystem of analytics, BI, and data science tools 
    much easier
  * Adding support for Azure Blob Storage
  * Add support for more compressions - currently default Snappy compression is supported


## Quickstart
### Initialize the rtdl services
1.  Run `docker compose -f docker-compose.init.yml up -d`.
    * **Note:** This configuration should be fault-tolerant, but if any containers or 
      processes fail when running this, run `docker compose -f docker-compose.init.yml down` 
      and retry.
2.  After containers `rtdl_rtdl-db-init`, `rtdl_catalog-db-init`, and `rtdl_catalog-init` 
    exit and complete with `EXITED (0)`, kill and delete the rtdl container set by running 
    `docker compose -f docker-compose.init.yml down`
3.  Run `docker compose up -d` every time after.
    * `docker compose down` to stop.

### Interact with rtdl services and create a data lake
All API calls used to interact with rtdl have Postman examples in our [postman-rtdl-public repo](https://github.com/realtimedatalake/postman-rtdl-public).
1.  If you are building your data lake on AWS or GCP, configure your storage buckets and access 
    by following the [RudderStack docs for AWS S3](https://www.rudderstack.com/docs/destinations/storage-platforms/amazon-s3/) or the [Segment docs for Google Cloud Storage](https://segment.com/docs/connections/storage/catalog/google-cloud-storage/). 
    * For AWS S3 storage, you will need your bucket name, your AWS access key id, and your AWS
      secret access key.
    * For GCP Cloud Storage, you will need your credentials in flattened json (remove all the 
      newlines).
3.  Instrument your website with [analytics-next-cc] - our fork of [Segment's Analytics.js 2.0](https://segment.com/docs/connections/sources/catalog/libraries/website/javascript/) 
    that let's you cc all of the events you send to Segment to rtdl's ingest endpoint. Its 
    snippet is a drop-in replacement of Analytics.js 2.0/Analytics.js. Using this makes it really 
    easy to build your data lake with existing Segment instrumentation. Enter your ingest endpoint
    as the `ccUrl` value and rtdl will handle the payload. Make sure you enter your writeKey in the 
    `stream_alt_id` of your `stream` configuration (below).
    * Alternatively, you can send ***any*** json with just ```stream_id``` in the payload and rtdl will add it to your lake.
      ```
      {
          "stream_id":"837a8d07-cd06-4e17-bcd8-aef0b5e48d31",
          "name":"user1",
          "array":[1,2,3],
          "properties":{"age":20}
      }
      ```
	You can optionally add ```message_type``` should you choose to override the ```message_type``` specified while creating the stream.
	rtdl will default to a message type ```rtdl_default``` if message type is absent in both stream definition and actual message
	
	
4.  Create/read/update/delete `stream` configurations that define a source data stream into 
    your data lake and the destination data store as well as configure folder partitioning and 
    file compression. It also allows for activating/deactivating a stream.
    * For any json data being sent to the ingest endpoint, the generated `stream_id` or the 
      manually input `stream_alt_id` values are required in the payload.

**Note:** To start from scratch, first run `rm -rf storage/` from the rtdl root folder.


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
  
  Sample ```createStream``` payload for creating Parquet file in AWS S3
  ```	
  {
	"active": true,
    "message_type": "test-msg-aws",
	"file_store_type_id": 2,
	"region": "us-east-1",
	"bucket_name": "testBucketAWS",
	"folder_name": "testFolderAWS",
    "partition_time_id": 1,
    "compression_type_id": 1,
	"aws_access_key_id": "[aws_access_key_id]",
    "aws_secret_access_key": "[aws_secret_access_key]"
  }
  ```
  
  ```file_store_type_id``` - 1 for Local, 2 for AWS, 3 for GCS
  ```partiion_time_id``` - 1 - HOURLY, 2 - DAILY, 3 - WEEKLY, 4 - MONTHLY, 5 - QAURTERLY
  
  For cloud storage - final file path would be 
  ```<bucket>/<folder>/<message type>/<time partition>/*.parquet```
  
  ```time partition``` part can look like 
	```2021-06-15-13```(Hourly), 
	```2021-06-15```(Daily),
	```2021-48```(Weekly - ISOWeek), 
	```2021-06```(Monthly),
	```2021-02```(Quarterly)
	
  The leaf-level file would have timestamp upto milliseconds as the file name
  

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

