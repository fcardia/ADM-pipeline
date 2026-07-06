# 🌳 Monitoraggio Parchi — Pipeline dati ACME

Prototipo per il caso di studio **"Monitoraggio parchi"** (ADM).
Il Comune di Cagliari fornisce in streaming, in forma anonima, il numero di
persone presenti in ogni parco cittadino. Ogni dato è una tupla
`<parco; timestamp; persone>` (es. `<Monte Urpinu; 1549275817.154; 118>`).
ACME usa lo stream per servire **due tipi di clienti**.

## Architettura

```
Producer Python  (stream del Comune: <parco; timestamp; persone>)
     │
     ▼
  Kafka (broker)  ◄──── Kafdrop (UI topic viewer)
     │
     ▼
  Telegraf (consumer + router)
     │
     ├──► InfluxDB (time-series, retention breve) ──► Grafana
     │        CLIENTE 1 — analisi real-time, niente storico
     │        · > 10 ingressi in 2 min   · > 3% uscite in 5 min
     │        · media / dev. std a 5, 10, 30 min
     │
     └──► MongoDB (storage permanente, SOLO parchi selezionati)
              CLIENTE 2 — memorizzazione di tutti i dati, analisi lungo termine
```

I due sink corrispondono uno-a-uno ai due tipi di cliente. La motivazione delle
scelte è in **[DESIGN.md](DESIGN.md)**.

## Avvio

```bash
docker compose up --build
```

La prima volta ci vogliono 2–3 minuti per il download delle immagini.

## Interfacce web

| Servizio | URL | Credenziali |
|---|---|---|
| Grafana (cliente 1) | http://localhost:3000 | admin / admin |
| Kafdrop | http://localhost:9000 | — |
| InfluxDB | http://localhost:8086 | admin / adminpassword |
| MongoDB | localhost:27017 | admin / adminpassword |

## Cliente 1 — Grafana

La dashboard **"🌳 Monitoraggio Parchi — ACME"** è pre-caricata in
`Dashboards → Parchi`. Contiene, sui dati grezzi di **tutti** i parchi:

- Presenze in tempo reale per parco;
- **(a)** tabella dei parchi con **> 10 ingressi** netti negli ultimi **2 min**;
- **(b)** tabella dei parchi con **> 3% di uscite** negli ultimi **5 min**;
- **(c)** media e deviazione standard delle presenze a **5, 10, 30 min**.

Refresh automatico a **5 secondi**. Tutte le analisi sono espresse come query
**Flux**: il calcolo è demandato a InfluxDB, non a codice applicativo.

## Cliente 2 — MongoDB

MongoDB conserva **permanentemente** i dati dei soli parchi selezionati dal
cliente (default: `Monte Urpinu`, `Molentargius`, `San Michele`). Esempi:

```bash
# apri una shell mongo
docker exec -it parchi-mongodb mongosh -u admin -p adminpassword

use acme
db.clienti_parchi.find()                       // parchi a cui il cliente è abbonato
db.presenze.countDocuments()                   // dati accumulati
db.presenze.find().sort({timestamp:-1}).limit(5)
```

Per cambiare i parchi ricevuti dal cliente, modificare la lista `parco` in
`telegraf/telegraf.conf` (output MongoDB) e riavviare Telegraf.

## Struttura del progetto

```
caddozzoni/
├── docker-compose.yml
├── README.md
├── DESIGN.md                   ← documentazione delle scelte di progetto
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             ← simulatore stream presenze parchi
├── telegraf/
│   └── telegraf.conf           ← consumer Kafka → InfluxDB + MongoDB
├── mongodb/
│   └── init/
│       └── 01-init.js          ← registro abbonamenti + storage permanente
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── influxdb.yml
    │   └── dashboards/
    │       └── dashboard.yml
    └── dashboards/
        └── parchi.json         ← dashboard cliente 1 (analisi real-time)
```

## Stop

```bash
docker compose down          # ferma i servizi
docker compose down -v       # + rimuove i volumi (reset completo)
```
