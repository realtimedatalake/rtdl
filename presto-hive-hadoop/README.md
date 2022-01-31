This image offers Presto with other necessary components in a single image. This image is only
useful for quick tests since the metadata (e.g., schemas, tables) will be gone when the container is
removed.


# Features

1. Unlike most other Docker images for the Presto database, this image offers multiple necessary 
components (e.g., Hadoop, Hive, PostgreSQL) in a single image. Thus, much easier to install without
setting network bridges to other containers.
2. The classpath for Amazon S3 connections are properly set. As a result, large data files can 
be easily stored in the cloud.
3. Presto's `drop table` is enabled.


# How to run

For regular connections to Presto:

```bash
docker run -d -p 8080:8080 --name presto verdictproject/presto-with-hadoop
```

Note that `8080` is the default port on which Presto runs. Opening the port will enable other applications to connect Presto.

To check if Presto server has started, run
```bash
docker logs --tail 10 presto
```

If the server has started, the following messages will be printed
```bash
starting yarn daemons
starting resourcemanager, logging to /root/hadoop-2.9.2/logs/yarn--resourcemanager-9bb175f0919b.out
localhost: starting nodemanager, logging to /root/hadoop-2.9.2/logs/yarn-root-nodemanager-9bb175f0919b.out
Started as 844
2019-08-28 18:03:28: Starting Hive Metastore Server
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/root/apache-hive-2.3.6-bin/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/root/hadoop-2.9.2/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
```


## S3 Configuration

For additional connection to S3:

```bash
docker run -d -p 8080:8080 --name presto \
-e AWS_ACCESS_KEY_ID={YourAccessKey} \
-e AWS_SECRET_ACCESS_KEY={YourSecretAccessKey} \
verdictproject/presto-with-hadoop
```

Then, the AWS keys passed as environment variables are set to Presto's config file at when starting
a container.


## Need more memory?

Provide the following environment variables. The startup script picks those variables and set
configuration files accordingly.

```bash
docker run -d -p 8080:8080 --name presto \
-e AWS_ACCESS_KEY_ID={YourAccessKey} \
-e AWS_SECRET_ACCESS_KEY={YourSecretAccessKey} \
-e QUERY_MAX_MEMORY='50GB' \
-e QUERY_MAX_MEMORY_PER_NODE='40GB' \
-e QUERY_MAX_TOTAL_MEMORY_PER_NODE='40GB' \
-e JAVA_HEAP_SIZE='60G'
verdictproject/presto-with-hadoop
```


# Command-line interface for Presto

After starting a container as described above, you can use Presto's command-line interface as
follows:

```bash
docker exec -it presto presto-cli
```


# S3 examples

You can create a schema to point to a S3 bucket. The tables created in this schema are automatically
pushed to the bucket.

```sql
CREATE SCHEMA hive.web
WITH (location = 's3a://yourbucket/');
```

An example of inserting data from the tpch catalog.

```sql
CREATE TABLE hive.web.lineitem
WITH (format = 'PARQUET')
AS 
SELECT * FROM tpch.sf1.lineitem;
```

You can also read existing data from an existing S3 bucket as follows:

```sql
CREATE TABLE hive.default.lineitem
(LIKE hive.web.lineitem)
WITH (external_location='s3a://yourbucket_to_table/', format='PARQUET');
```

Note:
1. The s3 path must end with `/`.
2. THe protocol should be `s3a:` for good performance. I didn't test the other older protocols, such
as `s3:` and `s3n:`.



# Versions

All components are located in `/root/`.

- Hadoop: hadoop-2.9.2
- PostgreSQL (for Hive Metastore): 10
- Hive: 2.3.6
- Presto: 318
- Ubuntu: 18.04

Note that as of the time of creating this image, Presto is only compatible with Hadoop2 
(not Hadoop3).


# Source code

The original Dockerfile and other files are available at:
https://github.com/verdict-project/docker-presto-with-hadoop

