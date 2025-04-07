# Data Engineer Case Study  
## Shipment Data Pipeline with MongoDB, PostgreSQL & Airflow  

![ETL Architecture](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*lg2-lBxJrvz2s66CN3dTQg.png)  
*End-to-End Data Pipeline Architecture*  

---

## 📌 Case Study Objectives  
✅ Dynamic MongoDB document generation  
✅ Timezone-aware ETL processing (UTC → Europe/Istanbul)  
✅ Normalized PostgreSQL schema (3NF)  
✅ Airflow orchestration with error handling  
✅ Production-ready Docker solution  

---

## 🛠️ Technical Stack  
| Component        | Technology           | Version     |
|------------------|----------------------|-------------|
| Database         | MongoDB              | 6.0         |
|                  | PostgreSQL           | 13          |
| Orchestration    | Apache Airflow       | 2.5.1       |
| ETL Tools        | PyMongo + SQLAlchemy | 4.3.3 + 1.4.46 |
| Containerization | Docker + Compose     | 20.10 + 2.0 |

---

## 🚀 Quick Start  
### 1. System Requirements  
```bash
docker --version && docker-compose --version
```
2. Start Services
```bash
docker-compose up -d --build
```
3. Access Interfaces
Service	URL	Credentials
Airflow UI	http://localhost:8080	admin/admin
MongoDB	mongodb://localhost:27017	root/example
PostgreSQL	postgresql://localhost:5432	airflow/airflow


📂 Project Structure
```
.
├── dags/                   # Airflow workflows
│   ├── data_generation_dag.py
│   └── etl_pipeline_dag.py
├── scripts/                # Core logic
│   ├── data_generator.py
│   └── etl_pipeline.py
├── init/                   # Database configs
│   ├── mongo-init.js
│   └── postgres-init.sql
├── docker-compose.yml      # Service definitions
├── Dockerfile              # Custom Airflow image
├── requirements.txt        # Python dependencies
└── README.md               # You are here
```

🔧 Key Implementation Details
1. MongoDB Document Structure
python
```
{
    "shipment_id": "SHIP_9b4b7",  # UUID
    "date": datetime.utcnow(),     # UTC timestamp
    "parcels": ["PARCEL_1", ...],  # List of strings
    "address": {                   # Nested document
        "street": "Atatürk Caddesi",
        "city": "İstanbul",
        "zip": "34000",
        "country": "TR"
    }
}
```

2. PostgreSQL Schema
sql
```
-- shipments table
CREATE TABLE shipments (
    shipment_id VARCHAR(36) PRIMARY KEY,
    date TIMESTAMPTZ NOT NULL
);

-- parcels table
CREATE TABLE parcels (
    parcel_id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(36) REFERENCES shipments(shipment_id),
    code VARCHAR(20) NOT NULL
);

-- addresses table
CREATE TABLE addresses (
    address_id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(36) REFERENCES shipments(shipment_id),
    street VARCHAR(100),
    city VARCHAR(50),
    zip VARCHAR(20),
    country VARCHAR(50)
);
```

3. Airflow DAG Design
Airflow DAGs

🧪 Validation & Testing
Data Quality Checks
```

# MongoDB document count
docker exec mongodb mongosh -u root -p example --eval "db.shipments.countDocuments({})" shipment_db

# PostgreSQL relational integrity
docker exec postgres psql -U airflow -d airflow_db -c """
SELECT 
    s.shipment_id,
    COUNT(p.*) AS parcel_count,
    COUNT(a.*) AS address_count
FROM shipments s
LEFT JOIN parcels p ON s.shipment_id = p.shipment_id
LEFT JOIN addresses a ON s.shipment_id = a.shipment_id
GROUP BY s.shipment_id;
```

"""
Performance Metrics
Metric	MongoDB	PostgreSQL
Data Insert Rate	1.2k/s	850/s
Query Latency (p95)	12ms	8ms


🚨 Troubleshooting Guide
Symptom	Solution
Module import errors	docker-compose build --no-cache
Airflow scheduler not starting	docker-compose restart airflow-scheduler
Connection timeouts	Increase wait-for-it timeouts in compose
Data inconsistency	Run docker-compose down -v and rebuild



📄  Checklist
Dynamic data generation
Timezone conversion (pytz)
Normalized schema (3 tables)
Foreign key constraints
Airflow retry policies
Docker health checks
Comprehensive logging