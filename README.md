# rtdl
1. Install Go (go@1.17)
2. In `./ingest`, `go mod init rtdl/ingest-service`
3. Write `ingest-service.go`
4. `go get github.com/gin-gonic/gin@v1.7.2`
5. `go mod tidy`

# General Reading
1. [An Introduction to Big Data Architectures](https://www.quastor.org/p/an-introduction-to-big-data-architectures)
2. [Why Not to Become a Data Engineer](https://medium.com/coriers/why-not-to-become-a-data-engineer-3533286bf642)
3. [What is AWS Glue? A Detailed Introductory Guide](https://www.lastweekinaws.com/blog/what-is-aws-glue-a-detailed-introductory-guide/)

## ingest-service
1. [Tutorial](https://golang.org/doc/tutorial/web-service-gin)
2. [SO Post](https://stackoverflow.com/questions/42247978/go-gin-gonic-get-text-from-post-request)
3. [SO Post](https://stackoverflow.com/questions/61919830/go-gin-get-request-body-json)
3. ["Distroless" Docker Images](https://github.com/GoogleContainerTools/distroless) - images containing only your application and its runtime dependencies
4. [DockerSlim](https://dockersl.im/) - small, fast Docker images

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