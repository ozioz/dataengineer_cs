// shipment_db veritabanını ve kullanıcıyı oluştur
db = db.getSiblingDB('shipment_db');

// ETL için özel kullanıcı
db.createUser({
  user: 'etl_user',
  pwd: 'etl_password',
  roles: [
    { role: 'readWrite', db: 'shipment_db' },
    { role: 'dbAdmin', db: 'shipment_db' }
  ]
});

// shipments koleksiyonu için indexler
db.createCollection('shipments');
db.shipments.createIndex({ shipment_id: 1 }, { unique: true });
db.shipments.createIndex({ date: 1 });

// Test verisi (opsiyonel)
db.shipments.insertOne({
  shipment_id: "test_123",
  date: new Date(),
  parcels: ["P001", "P002"],
  address: {
    street: "Örnek Mahalle",
    city: "İstanbul",
    zip: "34000",
    country: "Turkey"
  }
});

print('***** MongoDB initialization completed *****');