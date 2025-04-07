from pymongo import MongoClient
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, ForeignKey, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
import pytz
from datetime import datetime
import logging
import os

# Enhanced Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def etl_process():
    # Environment Variables for Security
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:example@mongodb:27017/admin?authMechanism=SCRAM-SHA-256')
    POSTGRES_URI = os.getenv('POSTGRES_URI', 'postgresql://airflow:airflow@postgres:5432/shipment_db')
    
    try:
        # MongoDB Connection with Improved Settings
        mongo_client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=30000,
            connectTimeoutMS=30000,
            maxPoolSize=100
        )
        db = mongo_client.get_database('shipment_db')
        collection = db.get_collection('shipments')
        
        # PostgreSQL Connection with Connection Pooling
        engine = create_engine(
            POSTGRES_URI,
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        metadata = MetaData()

        # Optimized Table Definitions
        shipments = Table(
            'shipments', metadata,
            Column('id', Integer, primary_key=True),
            Column('shipment_id', String(36), unique=True, nullable=False),
            Column('date', DateTime(timezone=True)),
            Column('created_at', DateTime, server_default=text('NOW()')),
            schema='public'
        )
        
        parcels = Table(
            'parcels', metadata,
            Column('id', Integer, primary_key=True),
            Column('shipment_id', String(36), ForeignKey('public.shipments.shipment_id'), index=True),
            Column('parcel_code', String(20), index=True),
            Column('created_at', DateTime, server_default=text('NOW()')),
            schema='public'
        )
        
        addresses = Table(
            'addresses', metadata,
            Column('id', Integer, primary_key=True),
            Column('shipment_id', String(36), ForeignKey('public.shipments.shipment_id'), index=True),
            Column('street', String(100)),
            Column('city', String(50)),
            Column('zip', String(20)),
            Column('country', String(100)),
            Column('created_at', DateTime, server_default=text('NOW()')),
            schema='public'
        )

        # Create Tables with Improved Error Handling
        try:
            metadata.create_all(engine, checkfirst=True)
        except SQLAlchemyError as e:
            logger.error(f"Table creation failed: {e}")
            raise

        # Timezone Configuration
        ist_timezone = pytz.timezone('Europe/Istanbul')

        with engine.connect() as conn:
            transaction = conn.begin()
            
            try:
                # MongoDB Query with Projection for Efficiency
                cursor = collection.find(
                    {},
                    {
                        'shipment_id': 1,
                        'date': 1,
                        'parcels': 1,
                        'address': 1,
                        '_id': 0
                    }
                ).batch_size(1000)
                
                total_docs = collection.count_documents({})
                logger.info(f"Starting ETL for {total_docs} documents")

                # Batch Processing Variables
                shipment_batch = []
                parcel_batch = []
                address_batch = []
                batch_size = 500
                processed_count = 0

                for doc in cursor:
                    try:
                        # Timezone Conversion with Validation
                        raw_date = doc.get('date')
                        if raw_date:
                            if isinstance(raw_date, str):  # String gelirse datetime'a çevir
                                raw_date = datetime.fromisoformat(raw_date)
                            if not raw_date.tzinfo:  # Timezone bilgisi yoksa UTC kabul et
                                raw_date = pytz.utc.localize(raw_date)
                            ist_date = raw_date.astimezone(ist_timezone)
                        else:
                            ist_date = None
                        
                        shipment_id = doc['shipment_id']
                        
                        # Prepare batch data
                        shipment_batch.append({
                            'shipment_id': shipment_id,
                            'date': ist_date
                        })
                        
                        # Process Parcels
                        if doc.get('parcels'):
                            parcel_batch.extend([
                                {
                                    'shipment_id': shipment_id,
                                    'parcel_code': p
                                } for p in doc['parcels']
                            ])
                        
                        # Process Address
                        if doc.get('address'):
                            addr = doc['address']
                            address_data = {
                                'shipment_id': shipment_id,
                                'street': addr.get('street') or None,  # Boş string yerine None
                                'city': addr.get('city') or 'UNKNOWN',  # Default değer
                                'zip': addr.get('zip', '').strip() or None,
                                'country': addr.get('country', 'Turkey').upper()  # Standardizasyon
                            }
                            if any(address_data.values()):  # En az bir alan doluysa ekle
                                address_batch.append(address_data)
                        
                        # Execute batch inserts when threshold reached
                        if len(shipment_batch) >= batch_size:
                            _execute_batch_inserts(
                                conn,
                                shipments,
                                parcels,
                                addresses,
                                shipment_batch,
                                parcel_batch,
                                address_batch
                            )
                            processed_count += len(shipment_batch)
                            logger.info(f"Processed {processed_count}/{total_docs} documents")
                            
                            # Reset batches
                            shipment_batch = []
                            parcel_batch = []
                            address_batch = []
                            
                    except Exception as doc_error:
                        logger.error(f"Error processing document {doc.get('shipment_id')}: {doc_error}")
                        continue
                
                # Insert remaining records in batches
                if shipment_batch:
                    _execute_batch_inserts(
                        conn,
                        shipments,
                        parcels,
                        addresses,
                        shipment_batch,
                        parcel_batch,
                        address_batch
                    )
                    processed_count += len(shipment_batch)
                
                transaction.commit()
                logger.info(f"ETL completed. Total processed: {processed_count}")
                
                # Data Quality Check
                _perform_data_validation(conn, total_docs)
                
            except Exception as e:
                transaction.rollback()
                logger.error(f"Transaction failed: {e}", exc_info=True)
                raise

    except Exception as e:
        logger.error(f"ETL Process failed critically: {e}", exc_info=True)
        raise

    finally:
        # Enhanced Resource Cleanup
        try:
            if 'mongo_client' in locals():
                mongo_client.close()
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")
        
        try:
            if 'engine' in locals():
                engine.dispose()
        except Exception as e:
            logger.error(f"Error disposing SQLAlchemy engine: {e}")

def _execute_batch_inserts(conn, shipments, parcels, addresses, shipment_batch, parcel_batch, address_batch):
    """Helper function for batch inserts with error handling"""
    try:
        # Shipments upsert (500'erli batch)
        for i in range(0, len(shipment_batch), 500):
            batch = shipment_batch[i:i + 500]
            stmt = insert(shipments).values(batch).on_conflict_do_update(
                index_elements=['shipment_id'],
                set_={'date': shipments.c.date}
            )
            conn.execute(stmt)
        
        # Parcels bulk insert
        if parcel_batch:
            conn.execute(
                parcels.insert().prefix_with("ON CONFLICT DO NOTHING"),
                parcel_batch
            )
        
        # Addresses bulk insert with conflict handling
        if address_batch:
            conn.execute(
                addresses.insert().prefix_with("ON CONFLICT (shipment_id) DO UPDATE SET "
                "street=EXCLUDED.street, city=EXCLUDED.city, zip=EXCLUDED.zip"),
                address_batch
            )
            
    except SQLAlchemyError as e:
        logger.error(f"Batch insert failed: {e}")
        raise

def _perform_data_validation(conn, expected_count):
    """Enhanced data quality checks with metrics tracking"""
    validation_metrics = {
        'missing_shipments': 0,
        'null_critical_fields': 0,
        'address_integrity_issues': 0,
        'parcel_integrity_issues': 0,
        'timestamp_anomalies': 0
    }

    try:
        # 1. Count Validation (Shipments)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM public.shipments
        """))
        actual_count = result.scalar()
        validation_metrics['missing_shipments'] = max(0, expected_count - actual_count)

        if actual_count != expected_count:
            logger.warning(
                f"Data count mismatch. Expected: {expected_count}, Actual: {actual_count} "
                f"(Missing: {validation_metrics['missing_shipments']})"
            )

        # 2. Null Checks (Critical Fields)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM public.shipments
            WHERE shipment_id IS NULL OR date IS NULL
        """))
        validation_metrics['null_critical_fields'] = result.scalar()

        # 3. Address Integrity Check
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT s.shipment_id)
            FROM public.shipments s
            LEFT JOIN public.addresses a ON s.shipment_id = a.shipment_id
            WHERE a.shipment_id IS NULL
        """))
        validation_metrics['address_integrity_issues'] = result.scalar()

        # 4. Parcel Integrity Check
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT p.shipment_id)
            FROM public.parcels p
            WHERE p.parcel_code IS NULL OR p.parcel_code = ''
        """))
        validation_metrics['parcel_integrity_issues'] = result.scalar()

        # 5. Timestamp Anomalies (Future Dates)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM public.shipments
            WHERE date > NOW() + INTERVAL '1 day'
        """))
        validation_metrics['timestamp_anomalies'] = result.scalar()

        # Log all metrics
        logger.info("Data Quality Metrics:\n" + "\n".join(
            f"- {k.replace('_', ' ').title()}: {v}" 
            for k, v in validation_metrics.items()
        ))

        # Critical failure threshold
        if validation_metrics['null_critical_fields'] > 0:
            logger.error("CRITICAL: Null values in shipment_id/date fields")
            # raise ValueError("Data quality check failed")  # Uncomment to fail pipeline

    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    etl_process()