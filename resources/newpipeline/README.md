# 🍷 Simulazione Linea Imbottigliamento Vino

Caso di studio per lezione — sistema IoT con pipeline dati completa.

## Architettura

```
Producer Python
     │
     ▼
  Kafka (broker)  ◄──── Kafdrop (UI topic viewer)
     │
     ▼
  Telegraf (consumer + router)
     │
     ├──► InfluxDB (time-series) ──► Grafana (dashboard)
     │
     └──► MongoDB (storico statistico)
```

## Macchine e Sensori

| Macchina | Sensore | Unità | Range normale |
|---|---|---|---|
| Riempitrice | Temperatura liquido | °C | 18–22 |
| Riempitrice | Pressione circuito | bar | 1.5–2.5 |
| Riempitrice | Bottiglie riempite | pz/min | 40–60 |
| Tappatrice | Forza tappatura | N | 80–120 |
| Tappatrice | Vibrazione motore | mm/s | 0.5–2.0 |
| Tappatrice | Bottiglie tappate | pz/min | 40–60 |

## Anomalie simulate

- **Ogni ~60 s** — Riempitrice: surriscaldamento liquido (temperatura sale fino a 45°C)
- **Ogni ~90 s** — Tappatrice: vibrazione eccessiva del motore (sale fino a 15 mm/s)

Le anomalie hanno una forma sinusoidale (salita graduale, picco, discesa) per simulare un comportamento realistico.

## Avvio

```bash
docker compose up --build
```

La prima volta ci vogliono 2–3 minuti per il download delle immagini.

## Interfacce web

| Servizio | URL | Credenziali |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Kafdrop | http://localhost:9000 | — |
| InfluxDB | http://localhost:8086 | admin / adminpassword |
| MongoDB | localhost:27017 | admin / adminpassword |

## Grafana — Dashboard

La dashboard **"🍷 Linea Imbottigliamento Vino"** è pre-caricata in `Dashboards → Winery`.

Contiene:
- Gauge in tempo reale per ogni sensore (con soglie colorate)
- Grafici storici ultimi 30 minuti
- Sezione anomalie con evidenziazione delle soglie
- Indicatori di stato macchina (✅ Normale / 🔴 ANOMALIA)

Il refresh automatico è impostato a **5 secondi**.

## Struttura del progetto

```
newpipeline/
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py          ← simulatore sensori
├── telegraf/
│   └── telegraf.conf        ← consumer Kafka → InfluxDB + MongoDB
├── mongodb/
│   └── init/
│       └── 01-init.js       ← inizializzazione collection + indici
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── influxdb.yml
    │   └── dashboards/
    │       └── dashboard.yml
    └── dashboards/
        └── winery.json      ← dashboard pre-configurata
```

## Stop

```bash
docker compose down
```

Per rimuovere anche i volumi (reset completo):

```bash
docker compose down -v
```
