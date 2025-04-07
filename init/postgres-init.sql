-- ETL için özel kullanıcı oluşturma
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'etl_user') THEN
        CREATE ROLE etl_user WITH LOGIN PASSWORD 'etl_password';
    END IF;
END $$;

-- Veritabanı ve yetkilendirmeler
CREATE DATABASE shipment_db;
GRANT ALL PRIVILEGES ON DATABASE shipment_db TO etl_user;

-- Şema ve tablo oluşturma
\c shipment_db

CREATE SCHEMA IF NOT EXISTS shipment_schema;
GRANT USAGE ON SCHEMA shipment_schema TO etl_user;
GRANT CREATE ON SCHEMA shipment_schema TO etl_user;
ALTER ROLE etl_user SET search_path TO shipment_schema, public;

-- Shipments tablosu
CREATE TABLE IF NOT EXISTS shipment_schema.shipments (
    shipment_id VARCHAR(36) PRIMARY KEY,
    date TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Parcels tablosu
CREATE TABLE IF NOT EXISTS shipment_schema.parcels (
    parcel_id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(36) NOT NULL REFERENCES shipment_schema.shipments(shipment_id) ON DELETE CASCADE,
    parcel_code VARCHAR(20) NOT NULL
);

-- Addresses tablosu
CREATE TABLE IF NOT EXISTS shipment_schema.addresses (
    address_id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(36) NOT NULL REFERENCES shipment_schema.shipments(shipment_id) ON DELETE CASCADE,
    street VARCHAR(100),
    city VARCHAR(50),
    zip VARCHAR(20),
    country VARCHAR(50)
);

-- Yetkilendirmeler
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA shipment_schema TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA shipment_schema TO etl_user;

-- Test verisi (Opsiyonel)
INSERT INTO shipment_schema.shipments (shipment_id, date) 
VALUES 
    ('test_123', NOW() - INTERVAL '1 DAY'),
    ('test_456', NOW());

INSERT INTO shipment_schema.parcels (shipment_id, parcel_code)
VALUES 
    ('test_123', 'PARCEL_001'),
    ('test_123', 'PARCEL_002');

INSERT INTO shipment_schema.addresses (shipment_id, street, city, zip, country)
VALUES 
    ('test_123', 'Atatürk Caddesi No:45', 'İstanbul', '34000', 'Türkiye');

-- Tamamlandı bilgilendirmesi
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL initialization successfully completed!';
END $$;