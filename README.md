# rtdl - The Real-Time Data Lake

## Quickstart
1. Run `docker compose -f docker-compose.init.yml up -d`.
    * **Note:** If any containers or processes fail when running this, run `docker compose -f docker-compose.init.yml down` and retry. The `catalog-db-init` service in the `rtdl_catalog-db-init` container sometimes fails on first run (it most likely has something to do with the healthcheck for `catalog-db` service returning `service_healthy` too early).
2. After the `schemaTool` completes running in the `rtdl_catalog-init` container, kill and delete the rtdl container set by running `docker compose -f docker-compose.init.yml down`
3. Run `docker compose up -d` every time after.
    * `docker compose down` to stop.


## Architecture
rtdl has a multi-service architecture composed of tested and trusted open source tools to process and catalog your data and custom-built services to interact with them more easily.

### ingest
Written in Go

### process-jobmanager
Apache Flink

### process-taskmanager
Apache Flink

### catalog
Apache Hive Metastore

### catalog-db
YugabyteDB
