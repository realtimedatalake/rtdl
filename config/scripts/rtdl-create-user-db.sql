SELECT 'CREATE DATABASE rtdl_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'rtdl_db')\gexec

DO
$do$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE  rolname = 'rtdl') THEN
        CREATE ROLE rtdl LOGIN PASSWORD 'rtdl';
    END IF;
END
$do$;

ALTER DATABASE rtdl_db OWNER TO rtdl;
GRANT ALL PRIVILEGES ON DATABASE rtdl_db to rtdl;

\c rtdl_db

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS file_store_types (
  file_store_type_id SERIAL,
  file_store_type_name VARCHAR,
  PRIMARY KEY (file_store_type_id)
);

CREATE TABLE IF NOT EXISTS partition_times (
  partition_time_id SERIAL,
  partition_time_name VARCHAR,
  PRIMARY KEY (partition_time_id)
);

CREATE TABLE IF NOT EXISTS compression_types (
  compression_type_id SERIAL,
  compression_type_name VARCHAR,
  PRIMARY KEY (compression_type_id)
);

CREATE TABLE IF NOT EXISTS streams (
  stream_id uuid DEFAULT gen_random_uuid(),
  stream_alt_id VARCHAR,
  active BOOLEAN DEFAULT FALSE,
  message_type VARCHAR NOT NULL,
  file_store_type_id INTEGER DEFAULT 1,
  region VARCHAR,
  bucket_name VARCHAR,
  folder_name VARCHAR NOT NULL,
  partition_time_id INTEGER DEFAULT 1,
  compression_type_id INTEGER DEFAULT 1,
  iam_arn VARCHAR,
  credentials JSON,
  PRIMARY KEY (stream_id),
  CONSTRAINT fk_file_store_type
    FOREIGN KEY(file_store_type_id)
      REFERENCES file_store_types(file_store_type_id)
  CONSTRAINT fk_partition_time
    FOREIGN KEY(partition_time_id)
      REFERENCES partition_times(partition_time_id)
  CONSTRAINT fk_compression_type
    FOREIGN KEY(compression_type_id)
      REFERENCES compression_types(compression_type_id)
);

INSERT INTO file_store_types (file_store_type_name)
VALUES
    ('AWS'),
    ('GCP');

INSERT INTO partition_times (partition_time_name)
VALUES
    ('Hourly'),
    ('Daily'),
    ('Weekly'),
    ('Monthly'),
    ('Quarterly');

INSERT INTO compression_types (compression_type_name)
VALUES
    ('snappy'),
    ('gzip'),
    ('lzo');

CREATE OR REPLACE FUNCTION getAllStreams()
    RETURNS TABLE (
        stream_id uuid,
        stream_alt_id VARCHAR,
        active BOOLEAN,
        message_type VARCHAR,
        file_store_type_name VARCHAR,
        region VARCHAR,
        bucket_name VARCHAR,
        folder_name VARCHAR,
        partition_time_name VARCHAR,
        compression_type_name VARCHAR,
        iam_arn VARCHAR,
        credentials JSON
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, fst.file_store_type_name, s.region, s.bucket_name, s.folder_name, pt.partition_time_name, ct.compression_type_name, s.iam_arn, s.credentials
        FROM streams s
        LEFT OUTER JOIN file_store_types fst
            ON s.file_store_type_id = fst.file_store_type_id
        LEFT OUTER JOIN partition_times pt
            ON s.partition_time_id = pt.partition_time_id
        LEFT OUTER JOIN compression_types ct
            ON s.compression_type_id = ct.compression_type_id
        ORDER BY s.stream_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllActiveStreams()
    RETURNS TABLE (
        stream_id uuid,
        stream_alt_id VARCHAR,
        active BOOLEAN,
        message_type VARCHAR,
        file_store_type_name VARCHAR,
        region VARCHAR,
        bucket_name VARCHAR,
        folder_name VARCHAR,
        partition_time_name VARCHAR,
        compression_type_name VARCHAR,
        iam_arn VARCHAR,
        credentials JSON
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, fst.file_store_type_name, s.region, s.bucket_name, s.folder_name, pt.partition_time_name, ct.compression_type_name, s.iam_arn, s.credentials
        FROM streams s
        LEFT OUTER JOIN file_store_types fst
            ON s.file_store_type_id = fst.file_store_type_id
        LEFT OUTER JOIN partition_times pt
            ON s.partition_time_id = pt.partition_time_id
        LEFT OUTER JOIN compression_types ct
            ON s.compression_type_id = ct.compression_type_id
        WHERE s.active = TRUE
        ORDER BY s.stream_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION createStream(stream_alt_id_arg VARCHAR, active_arg BOOLEAN, message_type_arg VARCHAR, file_store_type_id_arg INTEGER, region_arg VARCHAR, bucket_name_arg VARCHAR, folder_name_arg VARCHAR, partition_time_id_arg INTEGER, compression_type_id_arg INTEGER, iam_arn_arg VARCHAR, credentials_arg JSON)
    RETURNS TABLE (
        stream_id uuid,
        stream_alt_id VARCHAR,
        active BOOLEAN,
        message_type VARCHAR,
        file_store_type_id INTEGER,
        region VARCHAR,
        bucket_name VARCHAR,
        folder_name VARCHAR,
        partition_time_id INTEGER,
        compression_type_id INTEGER,
        iam_arn VARCHAR,
        credentials JSON
    )
AS $$
BEGIN
    RETURN QUERY
        INSERT INTO streams  (stream_alt_id, active, message_type, file_store_type_id, region, bucket_name, folder_name, partition_time_id, compression_type_id, iam_arn, credentials)
        VALUES
            (stream_alt_id_arg, active_arg, message_type_arg, file_store_type_id_arg, region_arg, bucket_name_arg, folder_name_arg, partition_time_id_arg, compression_type_id_arg, iam_arn_arg, credentials_arg)
        RETURNING *;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllFileStoreTypes()
    RETURNS TABLE (
        file_store_type_id INTEGER,
        file_store_type_name VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT fst.file_store_type_id, fst.file_store_type_name
        FROM file_store_types fst
        ORDER BY fst.file_store_type_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllPartitionTimes()
    RETURNS TABLE (
        partition_time_id INTEGER,
        partition_time_name VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT pt.partition_time_id, pt.partition_time_name
        FROM partition_times pt
        ORDER BY pt.partition_time_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllCompressionTypes()
    RETURNS TABLE (
        compression_type_id INTEGER,
        compression_type_name VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT ct.compression_type_id, ct.compression_type_name
        FROM compression_types ct
        ORDER BY ct.compression_type_id ASC;
END;
$$ LANGUAGE plpgsql;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO rtdl;

