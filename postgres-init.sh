
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "airflow" --dbname "airflow_db" <<-EOSQL
    CREATE DATABASE shipment_db;
    GRANT ALL PRIVILEGES ON DATABASE shipment_db TO airflow;
    ALTER DATABASE shipment_db SET timezone TO 'Europe/Istanbul';
EOSQL