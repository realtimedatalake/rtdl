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

CREATE TABLE IF NOT EXISTS streams (
  stream_id uuid DEFAULT gen_random_uuid(),
  stream_alt_id VARCHAR,
  active BOOLEAN DEFAULT FALSE,
  file_store_type_id INTEGER NOT NULL,
  region VARCHAR,
  bucket_name VARCHAR,
  folder_name VARCHAR NOT NULL,
  iam_arn VARCHAR,
  credentials JSON,
  PRIMARY KEY (stream_id),
  CONSTRAINT fk_file_store_type
    FOREIGN KEY(file_store_type_id)
      REFERENCES file_store_types(file_store_type_id)
);

INSERT INTO file_store_types (file_store_type_name)
VALUES
    ('AWS'),
    ('GCP');

CREATE OR REPLACE FUNCTION getAllStreams()
    RETURNS TABLE (
        stream_id uuid,
        stream_alt_id VARCHAR,
        active BOOLEAN,
        file_store_type_name VARCHAR,
        region VARCHAR,
        bucket_name VARCHAR,
        folder_name VARCHAR,
        iam_arn VARCHAR,
        credentials JSON
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, fst.file_store_type_name, s.region, s.bucket_name, s.folder_name, s.iam_arn, s.credentials
        FROM streams s
        LEFT OUTER JOIN file_store_types fst
            ON s.file_store_type_id = fst.file_store_type_id
        ORDER BY s.stream_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllActiveStreams()
    RETURNS TABLE (
        stream_id uuid,
        stream_alt_id VARCHAR,
        active BOOLEAN,
        file_store_type_name VARCHAR,
        region VARCHAR,
        bucket_name VARCHAR,
        folder_name VARCHAR,
        iam_arn VARCHAR,
        credentials JSON
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, fst.file_store_type_name, s.region, s.bucket_name, s.folder_name, s.iam_arn, s.credentials
        FROM streams s
        LEFT OUTER JOIN file_store_types fst
            ON s.file_store_type_id = fst.file_store_type_id
        WHERE s.active = TRUE
        ORDER BY s.stream_id ASC;
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

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO rtdl;

