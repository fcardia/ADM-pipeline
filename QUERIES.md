# Query del progetto Smart Farm (InfluxDB / Flux)

Raccolta di **tutte** le query Flux impiegate nella pipeline, con il modulo in cui
vivono e una riga sul ruolo che svolgono. InfluxDB è la versione **2.7**, quindi il
linguaggio è **Flux**. Tutte le query di lettura girano sull'org `farm`; i dati grezzi
sono nel bucket `farm_raw` (retention 30g) e lo storico aggregato in `farm_rollup`
(retention ∞).

| # | Modulo / File | Ruolo (una riga) |
|---|---------------|------------------|
| T1 | InfluxDB — `influxdb/init/tasks/rollup_1m.flux` | Task di rollup: media al minuto di temp/umidità per zona → `farm_rollup`. |
| T2 | InfluxDB — `rollup_1m.flux` | Task di rollup: minimo al minuto (campi `*_min`) → `farm_rollup`. |
| T3 | InfluxDB — `rollup_1m.flux` | Task di rollup: massimo al minuto (campi `*_max`) → `farm_rollup`. |
| D1 | Grafana — dashboard pannello 1 | Serie temporale umidità suolo per zona (soglia visiva 20%). |
| D2 | Grafana — dashboard pannello 2 | Serie temporale PM2.5 per zona (soglia visiva 50). |
| D3 | Grafana — dashboard pannello 3 | Serie temporale potenza istantanea per attuatore (soglia pompe 1125 W). |
| D4 | Grafana — dashboard pannello 4 | State timeline on/off attuatori (status→1/0). |
| D5 | Grafana — dashboard pannello 5 | Tabella riassuntiva: ultima temp+umidità aria per zona (pivot). |
| D6a | Grafana — dashboard pannello 6 | Trend storico: medie da `farm_rollup` (linea piena). |
| D6b | Grafana — dashboard pannello 6 | Trend storico: massimi da `farm_rollup` (linea tratteggiata). |
| D6c | Grafana — dashboard pannello 6 | Trend storico: minimi da `farm_rollup` (linea tratteggiata). |
| A1 | Grafana — `alerting/alert-rules.yaml` | Alert qualità aria: conta letture PM2.5 > 50 per zona. |
| A2 | Grafana — `alerting/alert-rules.yaml` | Alert stress idrico: conta letture umidità < 20% per zona. |
| A3 | Grafana — `alerting/alert-rules.yaml` | Alert sovraccarico pompe: conta letture power_w > 1125 W (pompa on). |

---

## Modulo InfluxDB — Task di rollup (`influxdb/init/tasks/rollup_1m.flux`)

Materializzano lo storico aggregato leggendo `farm_raw` e scrivendo in `farm_rollup`.
Sorgente condivisa (`src`) più tre scritture (media / min / max).

**T1 — Media al minuto (ruolo: downsampling per lo storico, medie temp/umidità per zona):**
```flux
option task = {name: "rollup_1m", every: 1m}

src = () =>
  from(bucket: "farm_raw")
    |> range(start: -1m)
    |> filter(fn: (r) => r._measurement == "air_quality")
    |> filter(fn: (r) => r._field == "temperature_c" or r._field == "humidity_pct")

src() |> aggregateWindow(every: 1m, fn: mean, createEmpty: false) |> to(bucket: "farm_rollup")
```

**T2 — Minimo al minuto (ruolo: salva l'escursione minima, campi `*_min`):**
```flux
src() |> aggregateWindow(every: 1m, fn: min, createEmpty: false) |> map(fn: (r) => ({r with _field: r._field + "_min"})) |> to(bucket: "farm_rollup")
```

**T3 — Massimo al minuto (ruolo: salva l'escursione massima, campi `*_max`):**
```flux
src() |> aggregateWindow(every: 1m, fn: max, createEmpty: false) |> map(fn: (r) => ({r with _field: r._field + "_max"})) |> to(bucket: "farm_rollup")
```

---

## Modulo Grafana — Query dei pannelli dashboard (`grafana/dashboards/smart-farm.json`)

Query di lettura per la visualizzazione; usano `v.timeRangeStart/Stop` e `v.windowPeriod`
per adattarsi al time-range e allo zoom della dashboard.

**D1 — Umidità del suolo per zona (ruolo: monitoraggio umidità suolo, una serie per zona):**
```flux
from(bucket: "farm_raw") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "soil_moisture" and r._field == "soil_moisture_pct") |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false) |> group(columns: ["zone"]) |> keep(columns: ["_time", "_value", "zone"])
```

**D2 — PM2.5 per zona (ruolo: monitoraggio qualità dell'aria, una serie per zona):**
```flux
from(bucket: "farm_raw") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "air_quality" and r._field == "pm25_ugm3") |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false) |> group(columns: ["zone"]) |> keep(columns: ["_time", "_value", "zone"])
```

**D3 — Consumi attuatori `power_w` (ruolo: potenza istantanea per attuatore):**
```flux
from(bucket: "farm_raw") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "power_consumption" and r._field == "power_w") |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false) |> group(columns: ["actuator_id"]) |> keep(columns: ["_time", "_value", "actuator_id"])
```

**D4 — Stato attuatori on/off (ruolo: timeline di stato, mappa status→1.0/0.0):**
```flux
from(bucket: "farm_raw") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "power_consumption" and r._field == "power_w") |> map(fn: (r) => ({r with _value: if r.status == "on" then 1.0 else 0.0})) |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false) |> group(columns: ["actuator_id"]) |> keep(columns: ["_time", "_value", "actuator_id"])
```

**D5 — Vista riassuntiva per zona (ruolo: tabella con ultima temp+umidità aria, pivot per zona):**
```flux
from(bucket: "farm_raw") |> range(start: -15m) |> filter(fn: (r) => r._measurement == "air_quality" and (r._field == "temperature_c" or r._field == "humidity_pct")) |> last() |> keep(columns: ["zone", "_field", "_value"]) |> pivot(rowKey: ["zone"], columnKey: ["_field"], valueColumn: "_value") |> sort(columns: ["zone"])
```

**D6a — Trend storico, medie (ruolo: legge le medie materializzate da `farm_rollup`):**
```flux
from(bucket: "farm_rollup") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "air_quality" and (r._field == "temperature_c" or r._field == "humidity_pct")) |> group(columns: ["zone", "_field"]) |> keep(columns: ["_time", "_value", "zone", "_field"])
```

**D6b — Trend storico, massimi (ruolo: banda superiore dell'escursione, campi `*_max`):**
```flux
from(bucket: "farm_rollup") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "air_quality" and (r._field == "temperature_c_max" or r._field == "humidity_pct_max")) |> group(columns: ["zone", "_field"]) |> keep(columns: ["_time", "_value", "zone", "_field"])
```

**D6c — Trend storico, minimi (ruolo: banda inferiore dell'escursione, campi `*_min`):**
```flux
from(bucket: "farm_rollup") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "air_quality" and (r._field == "temperature_c_min" or r._field == "humidity_pct_min")) |> group(columns: ["zone", "_field"]) |> keep(columns: ["_time", "_value", "zone", "_field"])
```

---

## Modulo Grafana — Query delle regole di alerting (`grafana/provisioning/alerting/alert-rules.yaml`)

Query `A` di ogni regola (poi ridotte con `reduce last` → confrontate con `threshold`).
Il `group by` produce un'istanza di allarme per zona/attuatore.

**A1 — Qualità aria critica (ruolo: conta letture PM2.5 > 50 per zona; scatta con count > 2):**
```flux
from(bucket: "farm_raw") |> range(start: -1m) |> filter(fn: (r) => r._measurement == "air_quality" and r._field == "pm25_ugm3") |> filter(fn: (r) => r._value > 50.0) |> group(columns: ["zone"]) |> count()
```

**A2 — Stress idrico del suolo (ruolo: conta letture umidità < 20% per zona; scatta con count > 1):**
```flux
from(bucket: "farm_raw") |> range(start: -1m) |> filter(fn: (r) => r._measurement == "soil_moisture" and r._field == "soil_moisture_pct") |> filter(fn: (r) => r._value < 20.0) |> group(columns: ["zone"]) |> count()
```

**A3 — Sovraccarico pompe (ruolo: conta letture power_w > 1125 W su pompa accesa; scatta con count > 0):**
```flux
from(bucket: "farm_raw") |> range(start: -1m) |> filter(fn: (r) => r._measurement == "power_consumption" and r._field == "power_w") |> filter(fn: (r) => r.status == "on" and r.actuator_type == "pump" and r._value > 1125.0) |> group(columns: ["actuator_id"]) |> count()
```

---

## Nota — Query di riferimento in `PIPELINE.md`

`PIPELINE.md` (§7–§9) contiene le versioni **di progettazione** delle stesse query
(rollup orario con `every: 1h`, alert con `range(start: -10s)` e org `agridata`). Non
sono deployate: la versione realmente in esecuzione è quella nei file di configurazione
elencati sopra (`every: 1m`, `range(start: -1m)`, org `farm`).
