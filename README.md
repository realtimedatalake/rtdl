# rtdl - The Real-Time Data Lake

## Quickstart
1. Run `docker compose -f docker-compose.init.yml up -d`.
    * **Note:** This configuration should be fault-tolerant, but if any containers or processes fail when running this, run `docker compose -f docker-compose.init.yml down` and retry.
2. After the `schemaTool` completes running in the `rtdl_catalog-init` container, kill and delete the rtdl container set by running `docker compose -f docker-compose.init.yml down`
3. Run `docker compose up -d` every time after.
    * `docker compose down` to stop.


## Architecture
rtdl has a multi-service architecture composed of tested and trusted open source tools to process and catalog your data and custom-built services to interact with them more easily.

### config
Written in Go
#### config-db
YugabyteDB

### ingest
Written in Go

### process
#### process-jobmanager
Apache Flink
#### process-taskmanager
Apache Flink

### catalog
Apache Hive Metastore
#### catalog-db
YugabyteDB
