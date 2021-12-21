SELECT 'CREATE DATABASE metastore_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metastore_db')\gexec

DO
$do$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE  rolname = 'metastore') THEN
        CREATE ROLE metastore LOGIN PASSWORD 'metastore';
    END IF;
END
$do$;

ALTER DATABASE metastore_db OWNER TO metastore;
