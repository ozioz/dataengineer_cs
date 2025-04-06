from pymongo import MongoClient
from faker import Faker
from datetime import datetime, timedelta
import random
import pytz  # Zaman dilimi desteği için

fake = Faker()

def generate_shipments(num=100):
    try:
        # MongoDB bağlantısı (Docker-compose'daki servis adı ve kimlik bilgileriyle)
        client = MongoClient(
            host='mongodb',  # Docker servis adı
            port=27017,
            username='root',     # MONGO_INITDB_ROOT_USERNAME
            password='example',  # MONGO_INITDB_ROOT_PASSWORD
            authSource='admin'   # Authentication database
        )
        
        db = client['shipment_db']
        collection = db['shipments']
        
        # Eski verileri temizle (opsiyonel)
        # collection.delete_many({})

        shipments = []
        for _ in range(num):
            # UTC zamanını Istanbul zaman dilimine çevir
            utc_date = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=random.randint(1, 365))
            ist_date = utc_date.astimezone(pytz.timezone('Europe/Istanbul'))

            shipment = {
                "shipment_id": fake.uuid4(),
                "date": ist_date,  # Zaman dilimi bilgisiyle
                "parcels": [fake.bothify(text='PARCEL-####') for _ in range(random.randint(1, 5))],
                "barcodes": [fake.bothify(text='BRCD-#####') for _ in range(random.randint(1, 10))],
                "address": {
                    "street": fake.street_address(),
                    "city": fake.city(),
                    "zip": fake.postcode(),
                    "country": fake.country()
                }
            }
            shipments.append(shipment)
        
        # Toplu ekleme yap
        result = collection.insert_many(shipments)
        print(f"Successfully inserted {len(result.inserted_ids)} documents")
        
        return True

    except Exception as e:
        print(f"MongoDB Error: {str(e)}")
        return False

    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    generate_shipments()