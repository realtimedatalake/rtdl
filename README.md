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
1. [GH](https://github.com/arempter/hive-metastore-docker)
2. [GH](https://github.com/IBM/docker-hive)
3. [I built a working Hadoop-Spark-Hive cluster on Docker. Here is how.](https://marcel-jan.eu/datablog/2020/10/25/i-built-a-working-hadoop-spark-hive-cluster-on-docker-here-is-how/)
4. [Old but gold: implementing a Hive Metastore Infrastructure](https://medium.com/quintoandar-tech-blog/old-but-gold-implementing-a-hive-metastore-infrastructure-225a8056fea8)

## Flink
1. [Docker Hub](https://hub.docker.com/_/flink)
2. [GH](https://github.com/apache/flink)
3. [Catalogs](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/catalogs/) - how to use Flink w/ Hive Metastore
4. [FileSystem](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/filesystem/) - Streaming Sink is how you can write data files including auto-compaction