from pymongo import MongoClient
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, ForeignKey, text
from sqlalchemy.dialects.postgresql import insert  # PostgreSQL özel fonksiyonlar için
from sqlalchemy.exc import SQLAlchemyError
import pytz
from datetime import datetime
import logging

# Loglama ayarları
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def etl_process():
    try:
        # MongoDB Bağlantısı
        mongo_client = MongoClient(
            host='mongodb',
            port=27017,
            username='root',
            password='example',
            authSource='admin',
            authMechanism='SCRAM-SHA-256',
            serverSelectionTimeoutMS=5000
        )
        db = mongo_client['shipment_db']
        collection = db['shipments']
        
        # PostgreSQL Bağlantısı
        engine = create_engine(
            'postgresql://airflow:airflow@postgres:5432/shipment_db',
            pool_pre_ping=True
        )
        
        metadata = MetaData()

        # Tabloları Tanımla (Unique constraint eklendi)
        shipments = Table(
            'shipments', metadata,
            Column('id', Integer, primary_key=True),
            Column('shipment_id', String(36), unique=True, nullable=False),  # UNIQUE CONSTRAINT
            Column('date', DateTime),
            Column('created_at', DateTime, server_default=text('NOW()'))
        )
        
        parcels = Table(
            'parcels', metadata,
            Column('id', Integer, primary_key=True),
            Column('shipment_id', String(36), ForeignKey('shipments.shipment_id')),
            Column('parcel_code', String(20)),
            Column('created_at', DateTime, server_default=text('NOW()'))
        )
        
        addresses = Table(
            'addresses', metadata,
            Column('id', Integer, primary_key=True),
            Column('shipment_id', String(36), ForeignKey('shipments.shipment_id')),
            Column('street', String(100)),
            Column('city', String(50)),
            Column('zip', String(20)),
            Column('country', String(50)),
            Column('created_at', DateTime, server_default=text('NOW()'))
        )

        # Tablo oluşturma (Eğer yoksa)
        metadata.create_all(engine, checkfirst=True)

        # Zaman Dilimi Ayarları
        ist_timezone = pytz.timezone('Europe/Istanbul')

        with engine.connect() as conn:
            transaction = conn.begin()
            
            try:
                # MongoDB'den veri çekme
                cursor = collection.find({}).batch_size(1000)  # Batch boyutu optimize edildi
                total_docs = collection.count_documents({})
                logger.info(f"Processing {total_docs} documents from MongoDB")

                for idx, doc in enumerate(cursor, 1):
                    # Zaman Dilimi Dönüşümü
                    raw_date = doc['date']
                    if not raw_date.tzinfo:
                        raw_date = pytz.utc.localize(raw_date)
                    ist_date = raw_date.astimezone(ist_timezone)
                    
                    shipment_id = doc['shipment_id']
                    
                    # UPSERT İşlemi (Düzeltilmiş)
                    stmt = insert(shipments).values(
                        shipment_id=shipment_id,
                        date=ist_date
                    ).on_conflict_do_update(
                        index_elements=['shipment_id'],  # Unique constraint sütunu
                        set_={'date': ist_date}  # Güncellenecek alanlar
                    )
                    conn.execute(stmt)
                    
                    # Parcels Toplu Insert (Optimize)
                    if doc.get('parcels'):
                        parcel_values = [{'shipment_id': shipment_id, 'parcel_code': p} for p in doc['parcels']]
                        conn.execute(parcels.insert(), parcel_values)
                    
                    # Address Insert (Güncellenmiş)
                    if doc.get('address'):
                        addr = doc['address']
                        conn.execute(
                            addresses.insert(),
                            {
                                'shipment_id': shipment_id,
                                'street': addr.get('street'),
                                'city': addr.get('city'),
                                'zip': addr.get('zip'),
                                'country': addr.get('country')
                            }
                        )
                    
                    # İlerleme Logu (Optimize)
                    if idx % 100 == 0:
                        logger.info(f"Processed {idx}/{total_docs} documents")
                        transaction.commit()  # Ara commit
                        transaction = conn.begin()

                transaction.commit()
                logger.info(f"Successfully processed {total_docs} documents")
                
            except SQLAlchemyError as e:
                transaction.rollback()
                logger.error(f"Database error: {e}", exc_info=True)
                raise
                
            except Exception as e:
                transaction.rollback()
                logger.error(f"Unexpected error: {e}", exc_info=True)
                raise

    except Exception as e:
        logger.error(f"ETL Process failed: {e}", exc_info=True)
        raise

    finally:
        if 'mongo_client' in locals():
            mongo_client.close()
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    etl_process()