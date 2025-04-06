-- ETL için özel kullanıcı ve şema (Güncellenmiş syntax)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'etl_user') THEN
        CREATE ROLE etl_user WITH LOGIN PASSWORD 'etl_password';
    END IF;
END
$$;

-- Yetkilendirmeler
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO etl_user;

-- shipment_db için ayrı veritabanı (opsiyonel)
CREATE DATABASE IF NOT EXISTS shipment_db;
GRANT ALL PRIVILEGES ON DATABASE shipment_db TO etl_user;

-- Tablolar için şema oluşturma
\c airflow_db
CREATE SCHEMA IF NOT EXISTS shipment_schema;
GRANT USAGE ON SCHEMA shipment_schema TO etl_user;
GRANT CREATE ON SCHEMA shipment_schema TO etl_user;
ALTER ROLE etl_user SET search_path TO shipment_schema, public;

-- Test verisi (create_tables.sql çalıştıktan sonra işletilecek)
-- Bu kısım ayrı bir dosyada (test_data.sql) olmalı veya
-- create_tables.sql'in en altına eklenmeli

DO $$
BEGIN
    RAISE NOTICE '***** PostgreSQL initialization completed *****';
END $$;