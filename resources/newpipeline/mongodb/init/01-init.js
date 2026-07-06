// Inizializzazione MongoDB — Winery
db = db.getSiblingDB("winery");

// Crea la collection con schema validation di base
db.createCollection("sensor_history", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["timestamp", "name"],
      properties: {
        timestamp: { bsonType: "date" },
        name: { bsonType: "string" },
      },
    },
  },
});

// Indice TTL: rimuove automaticamente documenti più vecchi di 30 giorni
db.sensor_history.createIndex(
  { timestamp: 1 },
  { expireAfterSeconds: 2592000 }
);

// Indice per query per macchina
db.sensor_history.createIndex({ name: 1, timestamp: -1 });

print("MongoDB winery inizializzato correttamente.");
