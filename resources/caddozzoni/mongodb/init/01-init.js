// Inizializzazione MongoDB — ACME / Monitoraggio parchi
// =====================================================
// Serve il CLIENTE di tipo 2: memorizzazione PERMANENTE dello stream, per i
// soli parchi a cui il cliente è abbonato, per analisi di lungo termine
// (es. dove aprire nuovi chioschi mobili — "caddozzoni").
db = db.getSiblingDB("acme");

// ── Registro degli abbonamenti del cliente ────────────────────────────────
// Documenta quali parchi il cliente ha selezionato. La stessa lista è
// applicata in telegraf.conf come filtro (tagpass) verso MongoDB.
db.createCollection("clienti_parchi", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["cliente", "parchi"],
      properties: {
        cliente: { bsonType: "string" },
        parchi: { bsonType: "array", items: { bsonType: "string" } },
      },
    },
  },
});

db.clienti_parchi.insertOne({
  cliente: "demo",
  parchi: ["Monte Urpinu", "Molentargius", "San Michele"],
  creato: new Date(),
});

// ── Collezione dello storico presenze ─────────────────────────────────────
// La collezione "presenze" (storage permanente) viene creata e popolata
// automaticamente da Telegraf (output MongoDB). A differenza della pipeline
// di riferimento NON creiamo alcun indice TTL: i dati devono restare
// permanentemente disponibili. Predisponiamo solo un indice utile alle
// analisi per parco / intervallo temporale del cliente 2.
db.createCollection("presenze");
db.presenze.createIndex({ "tags.parco": 1, "timestamp": -1 });

print("MongoDB ACME inizializzato: storage permanente parchi (nessun TTL).");
