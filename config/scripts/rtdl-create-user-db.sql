-- create database rtdl_db if it doesn't exist
SELECT 'CREATE DATABASE rtdl_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'rtdl_db')\gexec

-- create user rtdl if it doesn't exist
DO
$do$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE  rolname = 'rtdl') THEN
        CREATE ROLE rtdl LOGIN PASSWORD 'rtdl';
    END IF;
END
$do$;

-- make user rtdl the owner of database rtdl_db and grant it all privileges
ALTER DATABASE rtdl_db OWNER TO rtdl;
GRANT ALL PRIVILEGES ON DATABASE rtdl_db to rtdl;

-- switch to database rtdl_db
\c rtdl_db

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- create a function to help automate setting `updated_at` fields
CREATE OR REPLACE FUNCTION triggerSetTS()
    RETURNS TRIGGER
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- create `file_store_types` table
CREATE TABLE IF NOT EXISTS file_store_types (
  file_store_type_id SERIAL,
  file_store_type_name VARCHAR,
  PRIMARY KEY (file_store_type_id)
);

-- create `partition_times` table
CREATE TABLE IF NOT EXISTS partition_times (
  partition_time_id SERIAL,
  partition_time_name VARCHAR,
  PRIMARY KEY (partition_time_id)
);

-- create `compression_types` table
CREATE TABLE IF NOT EXISTS compression_types (
  compression_type_id SERIAL,
  compression_type_name VARCHAR,
  PRIMARY KEY (compression_type_id)
);

-- create `streams` table
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
  aws_access_key_id VARCHAR,
  aws_secret_access_key VARCHAR,
  gcp_json_credentials VARCHAR,
  azure_storage_account_name VARCHAR,
  azure_storage_access_key VARCHAR,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (stream_id),
  FOREIGN KEY(file_store_type_id) REFERENCES file_store_types(file_store_type_id),
  FOREIGN KEY(partition_time_id) REFERENCES partition_times(partition_time_id),
  FOREIGN KEY(compression_type_id) REFERENCES compression_types(compression_type_id)
);

-- create trigger to automate setting `updated_at` field when `streams` records are updated
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON streams
FOR EACH ROW
EXECUTE PROCEDURE triggerSetTS();

-- populate master data - start
INSERT INTO file_store_types (file_store_type_name)
VALUES
    ('Local'),
    ('AWS'),
    ('GCP'),
	('Azure');

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
-- populate master data - end

-- create API handler functions - start
CREATE OR REPLACE FUNCTION getStream(stream_id_arg VARCHAR)
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
		
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, s.message_type, s.file_store_type_id, s.region, s.bucket_name, s.folder_name, s.partition_time_id, s.compression_type_id, s.aws_access_key_id, s.aws_secret_access_key, s.gcp_json_credentials, s.azure_storage_account_name, s.azure_storage_access_key
        FROM streams s
        WHERE s.stream_id = (stream_id_arg)::uuid
        ORDER BY s.stream_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllStreams()
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, s.message_type, s.file_store_type_id, s.region, s.bucket_name, s.folder_name, s.partition_time_id, s.compression_type_id, s.aws_access_key_id, s.aws_secret_access_key, s.gcp_json_credentials, s.azure_storage_account_name, s.azure_storage_access_key
        FROM streams s
        ORDER BY s.stream_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getAllActiveStreams()
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
		
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT s.stream_id, s.stream_alt_id, s.active, s.message_type, s.file_store_type_id, s.region, s.bucket_name, s.folder_name, s.partition_time_id, s.compression_type_id, s.aws_access_key_id, s.aws_secret_access_key, s.gcp_json_credentials, s.azure_storage_account_name, s.azure_storage_access_key
        FROM streams s
        WHERE s.active = TRUE
        ORDER BY s.stream_id ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION createStream(stream_alt_id_arg VARCHAR, active_arg BOOLEAN, message_type_arg VARCHAR, file_store_type_id_arg INTEGER, region_arg VARCHAR, bucket_name_arg VARCHAR, folder_name_arg VARCHAR, partition_time_id_arg INTEGER, compression_type_id_arg INTEGER, aws_access_key_id_arg VARCHAR, aws_secret_access_key_arg VARCHAR, gcp_json_credentials_arg VARCHAR, azure_storage_account_name_arg VARCHAR, azure_storage_access_key_arg VARCHAR)
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
		
    )
AS $$
BEGIN
    RETURN QUERY
        INSERT INTO streams  (stream_alt_id, active, message_type, file_store_type_id, region, bucket_name, folder_name, partition_time_id, compression_type_id, aws_access_key_id, aws_secret_access_key, gcp_json_credentials, azure_storage_account_name, azure_storage_access_key)
        VALUES
            (stream_alt_id_arg, active_arg, message_type_arg, file_store_type_id_arg, region_arg, bucket_name_arg, folder_name_arg, partition_time_id_arg, compression_type_id_arg, aws_access_key_id_arg, aws_secret_access_key_arg, gcp_json_credentials_arg, azure_storage_account_name_arg, azure_storage_access_key_arg)
        RETURNING streams.stream_id, streams.stream_alt_id, streams.active, streams.message_type, streams.file_store_type_id, streams.region, streams.bucket_name, streams.folder_name, streams.partition_time_id, streams.compression_type_id, streams.aws_access_key_id, streams.aws_secret_access_key, streams.gcp_json_credentials, streams.azure_storage_account_name, streams.azure_storage_access_key;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION updateStream(stream_id_arg VARCHAR, stream_alt_id_arg VARCHAR, active_arg BOOLEAN, message_type_arg VARCHAR, file_store_type_id_arg INTEGER, region_arg VARCHAR, bucket_name_arg VARCHAR, folder_name_arg VARCHAR, partition_time_id_arg INTEGER, compression_type_id_arg INTEGER, aws_access_key_id_arg VARCHAR, aws_secret_access_key_arg VARCHAR, gcp_json_credentials_arg VARCHAR, azure_storage_account_name_arg VARCHAR, azure_storage_access_key_arg VARCHAR)
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        UPDATE streams
        SET stream_alt_id = stream_alt_id_arg,
            active = active_arg,
            message_type = message_type_arg,
            file_store_type_id = file_store_type_id_arg,
            region = region_arg,
            bucket_name = bucket_name_arg,
            folder_name = folder_name_arg,
            partition_time_id = partition_time_id_arg,
            compression_type_id = compression_type_id_arg,
            aws_access_key_id = aws_access_key_id_arg,
            aws_secret_access_key = aws_secret_access_key_arg,
            gcp_json_credentials = gcp_json_credentials_arg,
			azure_storage_account_name = azure_storage_account_name_arg,
			azure_storage_access_key = azure_storage_access_key_arg
			
			
        WHERE streams.stream_id = (stream_id_arg)::uuid
        RETURNING streams.stream_id, streams.stream_alt_id, streams.active, streams.message_type, streams.file_store_type_id, streams.region, streams.bucket_name, streams.folder_name, streams.partition_time_id, streams.compression_type_id, streams.aws_access_key_id, streams.aws_secret_access_key, streams.gcp_json_credentials, streams.azure_storage_account_name, streams.azure_storage_access_key;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION deleteStream(stream_id_arg VARCHAR)
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        DELETE FROM streams
        WHERE streams.stream_id = (stream_id_arg)::uuid
        RETURNING streams.stream_id, streams.stream_alt_id, streams.active, streams.message_type, streams.file_store_type_id, streams.region, streams.bucket_name, streams.folder_name, streams.partition_time_id, streams.compression_type_id, streams.aws_access_key_id, streams.aws_secret_access_key, streams.gcp_json_credentials, streams.azure_storage_account_name, streams.azure_storage_access_key;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION activateStream(stream_id_arg VARCHAR)
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
		
    )
AS $$
BEGIN
    RETURN QUERY
        UPDATE streams
        SET active = TRUE
        WHERE streams.stream_id = (stream_id_arg)::uuid
        RETURNING streams.stream_id, streams.stream_alt_id, streams.active, streams.message_type, streams.file_store_type_id, streams.region, streams.bucket_name, streams.folder_name, streams.partition_time_id, streams.compression_type_id, streams.aws_access_key_id, streams.aws_secret_access_key, streams.gcp_json_credentials, streams.azure_storage_account_name, streams.azure_storage_access_key;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION deactivateStream(stream_id_arg VARCHAR)
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
        aws_access_key_id VARCHAR,
        aws_secret_access_key VARCHAR,
        gcp_json_credentials VARCHAR,
		azure_storage_account_name VARCHAR,
		azure_storage_access_key VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        UPDATE streams
        SET active = FALSE
        WHERE streams.stream_id = (stream_id_arg)::uuid
        RETURNING streams.stream_id, streams.stream_alt_id, streams.active, streams.message_type, streams.file_store_type_id, streams.region, streams.bucket_name, streams.folder_name, streams.partition_time_id, streams.compression_type_id, streams.aws_access_key_id, streams.aws_secret_access_key, streams.gcp_json_credentials, streams.azure_storage_account_name, streams.azure_storage_access_key;
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
-- create API handler functions - end

-- grant user rtdl all privileges in the database rtdl_db
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO rtdl;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO rtdl;
