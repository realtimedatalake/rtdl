# rtdl - The Real-Time Data Lake

## Quickstart
1. Run `docker compose -f docker-compose.init.yml up -d`.
2. After the `schemaTool` completes running in the `rtdl_catalog-init` container, kill and delete the rtdl container set.
    * `docker container stop rtdl_catalog-db`
    * `docker container rm rtdl_catalog-db rtdl_catalog-db-init rtdl_catalog-init`
3. Run `docker compose up -d` every time after.
    * `docker compose down` to stop.


## Architecture
rtdl has a multi-service architecture composed of tested and trusted open source tools to process and catalog your data and custom-built services to interact with them more easily.

### ingest

### process

### catalog


## How to Build

### ingest
**Prerequisites**
* OpenJDK 11
* [Quarkus](https://quarkus.io/get-started/)

**Build container image**
* `cd ingest`
* For a shorter build time but a bigger image that starts slower, build a JVM image
    ```
    ./mvnw clean package \
    -Dquarkus.container-image.build=true \
    -Dquarkus.container-image.group=rtdl \
    -Dquarkus.container-image.name=rtdl-ingest \
    -Dquarkus.container-image.additional-tags=latest
    ```
* For a a smaller image that starts faster but takes much longer to build, build a native image
    ```
    ./mvnw clean package -Pnative \
    -Dquarkus.native.container-build=true \
    -Dquarkus.native.builder-image=quay.io/quarkus/ubi-quarkus-mandrel:21.3-java11 \
    -Dquarkus.container-image.build=true \
    -Dquarkus.container-image.group=rtdl \
    -Dquarkus.container-image.name=rtdl-ingest \
    -Dquarkus.container-image.additional-tags=latest
    ```


### process

### catalog
