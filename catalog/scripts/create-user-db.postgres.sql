SELECT 'CREATE DATABASE rtdl_catalog_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'rtdl_catalog_db')\gexec

DO
$do$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE  rolname = 'rtdl') THEN
        CREATE ROLE rtdl LOGIN PASSWORD 'rtdl';
    END IF;
END
$do$;

ALTER DATABASE rtdl_catalog_db OWNER TO rtdl;
