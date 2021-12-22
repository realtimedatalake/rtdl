## ingest
* Install Go (go@1.17)
* In `./ingest`, `go mod init rtdl/ingest-service`
* Write `ingest-service.go`
* `go get github.com/gin-gonic/gin@v1.7.2`
* `go mod tidy`
* `go build -o ./ingest-service` (won't be visible in finder)
* run executable `./ingest-service` 
* `docker build --no-cache -t rtdl/rtdl-ingest:latest -t rtdl/rtdl-ingest:0.1.0 .`
* `docker push -a rtdl/rtdl-ingest`
* Run docker image using
    ```
    docker run -d \
    --name rtdl-ingest \
    -p 8080:8080 \
    rtdl/rtdl-ingest:latest
    ```
* [DockerSlim](https://dockersl.im/) - small, fast Docker images

## catalog
* `docker build --no-cache -t rtdl/hive-metastore:latest -t rtdl/hive-metastore:3.1.2 .`
* `docker push -a rtdl/hive-metastore`

## process
* `curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.14.2/flink-sql-connector-hive-3.1.2_2.12-1.14.2.jar --output flink-sql-connector-hive-3.1.2_2.12-1.14.2.jar`
*   ```
    volumes:
      - ./process/lib/flink-sql-connector-hive-3.1.2_2.12-1.14.2.jar:/lib/flink-sql-connector-hive-3.1.2_2.12-1.14.2.jar
    ```

## psql-client
* `docker build --no-cache -t rtdl/psql-client:latest -t rtdl/psql-client:1.0.0 .`
* `docker push -a rtdl/psql-client`

# General Reading
1. [An Introduction to Big Data Architectures](https://www.quastor.org/p/an-introduction-to-big-data-architectures)
2. [Why Not to Become a Data Engineer](https://medium.com/coriers/why-not-to-become-a-data-engineer-3533286bf642)
3. [What is AWS Glue? A Detailed Introductory Guide](https://www.lastweekinaws.com/blog/what-is-aws-glue-a-detailed-introductory-guide/)

## Hive Metastore
1. **Use this one** [GH](https://github.com/arempter/hive-metastore-docker)
2. [Hive Metastore Standalone Admin Manual](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+3.0+Administration)
3. [YBDB JDBC Driver docs](https://docs.yugabyte.com/latest/integrations/jdbc-driver/)
4. [SO Metastore WH directory](https://stackoverflow.com/questions/30518130/how-to-set-hive-metastore-warehouse-dir-in-hivecontext)
5. [GH](https://github.com/IBM/docker-hive)
6. [I built a working Hadoop-Spark-Hive cluster on Docker. Here is how.](https://marcel-jan.eu/datablog/2020/10/25/i-built-a-working-hadoop-spark-hive-cluster-on-docker-here-is-how/)
    * [docker-compose](https://github.com/Marcel-Jan/docker-hadoop-spark/blob/master/docker-compose.yml)
7. Big Data Europe
    * [Hive Docker image](https://hub.docker.com/r/bde2020/hive)
    * [Hive Metastore Docker image](https://hub.docker.com/layers/bde2020/hive/2.1.0-postgresql-metastore/images/sha256-c08e4c07c5d670ccfed2fc5123b2fe536d3678347f65f46629b8d2d98564c1d5?context=explore)
    * [Hive Docker GH repo](https://github.com/big-data-europe/docker-hive)
8. [Old but gold: implementing a Hive Metastore Infrastructure](https://medium.com/quintoandar-tech-blog/old-but-gold-implementing-a-hive-metastore-infrastructure-225a8056fea8)

## Flink
1. [Docker Hub](https://hub.docker.com/_/flink)
2. [GH](https://github.com/apache/flink)
3. [Catalogs](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/catalogs/) - how to use Flink w/ Hive Metastore
4. [FileSystem](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/filesystem/) - Streaming Sink is how you can write data files including auto-compaction

## ingest-java
1. Install OpenJDK 11 (via homebrew), [Quarkus](https://quarkus.io/get-started/),
2. Build docker image using
    ```
    // For bigger image, JVM build (short build time)
    ./mvnw clean package \
    -Dquarkus.container-image.build=true \
    -Dquarkus.container-image.group=rtdl \
    -Dquarkus.container-image.name=rtdl-ingest \
    -Dquarkus.container-image.additional-tags=latest

    // For lighter image, native build (long build time)
    ./mvnw clean package -Pnative \
    -Dquarkus.native.container-build=true \
    -Dquarkus.native.builder-image=quay.io/quarkus/ubi-quarkus-mandrel:21.3-java11 \
    -Dquarkus.container-image.build=true \
    -Dquarkus.container-image.group=rtdl \
    -Dquarkus.container-image.name=rtdl-ingest \
    -Dquarkus.container-image.additional-tags=latest
    ```