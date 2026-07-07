# Smart Farm — Modulo Grafana (Visualizzazione + Alerting)

Questo modulo copre **FR3 (Visualizzazione)** e **FR4 (Alerting)** del progetto
Smart Farm. Contiene la dashboard Grafana, le 3 regole di allarme e tutto il
provisioning automatico (datasource, dashboard, contact point, notification
policy), pronto per il deploy containerizzato definito in `docker-compose.yml`.

> **Scope:** solo Grafana. L'ingestion (simulatore → MQTT → Telegraf → InfluxDB)
> è coperta dagli altri moduli. Le query assumono la struttura del DB descritta
> in `PIPELINE.md` §5 (InfluxDB **2.7**, quindi query in **Flux**).

---

## 1. Struttura dei file

```
grafana/
├── README.md                         ← questo file
├── dashboards/
│   └── smart-farm.json               ← dashboard (JSON model, provisioning-ready)
└── provisioning/
    ├── datasources/
    │   └── influxdb.yaml              ← datasource InfluxDB (Flux), via env var
    ├── dashboards/
    │   └── dashboards.yaml            ← provider che carica dashboards/*.json
    └── alerting/
        ├── contact-points.yaml        ← contact point Slack (+ webhook Mongo opz.)
        ├── notification-policies.yaml ← routing degli alert → Slack
        └── alert-rules.yaml           ← le 3 regole di allarme (FR4)
.env.example                          ← template variabili d'ambiente (radice repo)
docker-compose.yml                    ← service "grafana" aggiunto (radice repo)
```

Tutto viene caricato **automaticamente all'avvio** di Grafana tramite i volumi
montati in `docker-compose.yml`:

- `./grafana/provisioning → /etc/grafana/provisioning` (datasource, dashboard-provider, alerting)
- `./grafana/dashboards   → /var/lib/grafana/dashboards` (il JSON della dashboard)

---

## 2. Dashboard (`smart-farm.json`)

7 pannelli, tutti su bucket `farm_raw` (tranne il trend storico):

| # | Pannello | Tipo | Metrica / Query |
|---|----------|------|-----------------|
| 1 | Umidità del suolo per zona | timeseries | `soil_moisture_pct`, soglia visiva a **20%** |
| 2 | PM2.5 per zona | timeseries | `pm25_ugm3`, soglia visiva a **50** |
| 3 | Consumi attuatori (`power_w`) | timeseries | potenza per attuatore, soglia **1125 W** sulle pompe |
| 4 | Stato attuatori (on/off) | state timeline | `status` mappato 1=on / 0=off |
| 5 | Vista riassuntiva per zona | table | ultima temp + umidità aria per zona (pivot) |
| 6 | Trend storico (medie orarie) | timeseries | da bucket **`farm_rollup`** (vedi §5) |
| 7 | Allarmi Smart Farm | alert list | stato live delle 3 regole |

Le query usano `v.timeRangeStart/Stop` e `v.windowPeriod`, quindi si adattano al
time-range e allo zoom della dashboard. Il raggruppamento `group(columns:["zone"])`
produce **una serie per zona** (legenda per zona = "vista per zona").

---

## 3. Alerting (`alert-rules.yaml`) — le 3 condizioni

Motore **Grafana Unified Alerting**, valutazione ogni **10s** (backend, indipendente
dalle dashboard aperte). Ogni regola: query Flux (`A`) → reduce last (`B`) →
threshold (`C`). Il `group by` in Flux genera **un'istanza di allarme per zona/attuatore**.

| Alert | Measurement | Condizione | Soglia Grafana |
|-------|-------------|------------|----------------|
| **Qualità aria** | `air_quality.pm25_ugm3` | `> 50` per ≥3 letture/zona | `count > 2` |
| **Stress idrico** | `soil_moisture.soil_moisture_pct` | `< 20` per ≥2 letture/zona | `count > 1` |
| **Sovraccarico pompe** | `power_consumption.power_w` | pompa `on` con `power_w > 1125` (1.5×750 W) | `count > 0` |

`noDataState: OK` → quando nessuna lettura supera la soglia la query è vuota
(NoData) = stato sano, nessun falso allarme.

**Perché queste soglie** (dai razionali di PDF §6 e PIPELINE.md):
- PM2.5 > 50 µg/m³ → coerente con indici OMS/EPA di qualità dell'aria.
- Umidità < 20% → sotto questa soglia le colture entrano in stress idrico.
- Pompa > 1.5× nominale → 750 W nominali × 1.5 = **1125 W**; l'alert è ristretto
  a `actuator_type == "pump"` (come da PDF: "solo per alcuni sensori di corrente").

### Verificare che gli allarmi scattino
Il simulatore inietta anomalie apposta. Per farle scattare rapidamente in demo,
abbassa l'intervallo di generazione:

```bash
docker compose run --rm sensor-simulator --interval 0.2 --csv-path data/sensor_data.csv
```

Poi osserva il pannello "Allarmi Smart Farm" e (se configurato) il canale Slack.

---

## 4. Deploy / import

### Opzione A — automatica (consigliata, provisioning)
```bash
cp .env.example .env          # compila SLACK_WEBHOOK_URL se vuoi le notifiche
docker compose up -d --build  # avvia tutto, Grafana incluso
```
Apri **http://localhost:3000** → login `admin` / `${GF_SECURITY_ADMIN_PASSWORD}`
(default `admin`). Datasource, dashboard e alert rules sono già caricati.
La dashboard è in cartella **"Smart Farm"**.

### Opzione B — import manuale della sola dashboard
In una Grafana esistente: **Dashboards → New → Import → Upload JSON** →
`grafana/dashboards/smart-farm.json`. Assicurati che esista un datasource
InfluxDB (Flux) con **uid `farm-influxdb`**, altrimenti seleziona il tuo datasource
in fase di import.

> ⚠️ **Cartelle sincronizzate col cloud:** non lanciare `docker compose` da
> Google Drive/OneDrive/Dropbox — i bind mount del provisioning non funzionano.
> Copia il progetto in una cartella locale reale (es. `C:\hackathon\...`).

---

## 5. Nota sul pannello "Trend storico" (`farm_rollup`)

Il pannello 6 interroga il bucket **`farm_rollup`** (medie orarie), che è
l'architettura corretta per lo storico a lungo termine (PIPELINE.md §7). Questo
bucket **non è creato di default** in questo compose: finché il task di rollup non
gira, il pannello resta vuoto (non è un errore).

**Per popolarlo** — crea bucket + task Flux (PIPELINE.md §7.1), org/token reali di
questo compose (`farm` / `farm-token`):
```bash
docker compose exec influxdb influx bucket create \
  --org farm --token farm-token --name farm_rollup --retention 0
docker compose exec influxdb influx task create \
  --org farm --token farm-token --file /work/rollup.flux
```

**Alternativa** (se non vuoi implementare il rollup): calcola le medie orarie
al volo da `farm_raw`, sostituendo la query del pannello 6 con:
```flux
from(bucket: "farm_raw")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "air_quality" and (r._field == "temperature_c" or r._field == "humidity_pct"))
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> group(columns: ["zone", "_field"])
```
(funziona subito, ma senza materializzazione: dopo la scadenza di `farm_raw` lo
storico non c'è più — da qui la scelta del bucket dedicato).

---

## 6. Collegare gli allarmi a **Slack** (dove/come intervenire)

Il contact point è già definito in
**`grafana/provisioning/alerting/contact-points.yaml`** (receiver `slack-farm`) e
la policy in **`notification-policies.yaml`** instrada già tutti gli alert lì.
L'unica cosa da fornire è l'**URL del webhook**, che **non è hardcoded**: arriva
dalla variabile d'ambiente `SLACK_WEBHOOK_URL`.

**Passi:**
1. **Lato Slack:** crea una Slack App ("from scratch") → **Incoming Webhooks → On**
   → **Add New Webhook to Workspace** → scegli il canale (es. `#farm-alerts`) →
   copia l'URL `https://hooks.slack.com/services/…`.
2. **Nel file `.env`** (radice del repo):
   ```
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T…/B…/…
   ```
3. **Riavvia Grafana** per rileggere l'env:
   ```bash
   docker compose up -d grafana
   ```

Titolo/testo della notifica si personalizzano nel campo `settings.title` /
`settings.text` del receiver in `contact-points.yaml`. Il raggruppamento delle
notifiche (una per zona/pompa) è in `notification-policies.yaml` (`group_by`).

---

## 7. Collegare gli allarmi a **MongoDB** (storico allarmi — opzionale, dove/come)

Obiettivo: salvare ogni evento di allarme in Mongo per uno storico interrogabile
(PIPELINE.md §10). Grafana non scrive su Mongo direttamente: si usa un **secondo
contact point di tipo `webhook`** che punta a un micro-servizio che fa l'insert.

**Interventi necessari (3 punti):**

1. **Micro-servizio webhook → Mongo.** Crea `alert-webhook/app.py` (FastAPI +
   pymongo) che riceve il payload di Grafana e inserisce in `farm.alerts`
   (esempio pronto in PIPELINE.md §10) + relativo `Dockerfile`.

2. **`contact-points.yaml`** → **decommenta** il receiver `mongo-archive` (già
   predisposto in fondo al file):
   ```yaml
   - orgId: 1
     name: mongo-archive
     receivers:
       - uid: mongo_webhook_receiver
         type: webhook
         settings:
           url: http://alert-webhook:8000/alert
           httpMethod: POST
   ```

3. **`notification-policies.yaml`** → **decommenta** la route figlia che inoltra
   in parallelo anche a Mongo (`continue: true` = va SIA a Slack SIA al webhook):
   ```yaml
   routes:
     - receiver: mongo-archive
       continue: true
   ```

4. **`docker-compose.yml`** → aggiungi i service `mongo` e `alert-webhook`
   (esempi in PIPELINE.md §11.2), sulla stessa `farm-net`, così l'hostname
   `alert-webhook` è risolvibile da Grafana.

Poi `docker compose up -d` e gli allarmi finiranno sia su Slack sia nella
collection `farm.alerts` di MongoDB.

---

## 8. Riferimenti di configurazione

| Cosa | Valore (default compose) | Dove cambiarlo |
|------|--------------------------|----------------|
| InfluxDB URL | `http://farm-influxdb:8086` | `datasources/influxdb.yaml` |
| Org / Bucket / Token | `farm` / `farm_raw` / `farm-token` | `.env` (`INFLUXDB_*`) |
| Datasource uid | `farm-influxdb` | riferito da dashboard + alert rules |
| Slack webhook | (da `.env`) | `SLACK_WEBHOOK_URL` |
| Password admin Grafana | `admin` | `.env` (`GF_SECURITY_ADMIN_PASSWORD`) |

> **Nota sull'allineamento con PIPELINE.md:** il documento §8 riportava org
> `agridata` / token `dev-token-change-me`; qui si usano i valori **realmente
> deployati** in `docker-compose.yml` (`farm` / `farm-token`) perché il datasource
> deve connettersi all'InfluxDB effettivo. Sono comunque parametrizzati via env.
