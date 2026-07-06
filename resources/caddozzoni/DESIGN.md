# Documentazione di progetto — Monitoraggio Parchi (ACME)

## 1. Il problema

Il Comune di Cagliari emette uno **stream** di tuple
`<parco; timestamp; persone>`: nome del parco, istante (epoch unix con frazioni
di secondo) e numero di persone **attualmente** presenti. ACME ha comprato la
licenza sullo stream e deve servire due tipi di cliente con esigenze opposte:

| | Cliente 1 — Agenzie di sicurezza | Cliente 2 — Società ("caddozzoni") |
|---|---|---|
| Natura | **Analisi real-time** sullo stream | **Memorizzazione permanente** di tutti i dati |
| Storico | **Non** serve | **Serve** (analisi di lungo termine) |
| Dati | Tutti i parchi | Solo i parchi **scelti** dal cliente |
| Esempi | > 10 ingressi in 2 min; > 3% uscite in 5 min; media/dev.std a 5/10/30 min | dove aprire nuovi chioschi mobili |

Le esigenze sono così diverse che **un unico DBMS sarebbe una forzatura**: la
scelta di progetto centrale è usare **due motori specializzati**, uno per
cliente, alimentati dallo stesso stream.

## 2. Architettura

Riuso (con adattamenti minimi) la pipeline a doppio sink della traccia di
riferimento, perché mappa in modo naturale sui due clienti:

```
Producer → Kafka → Telegraf ─┬─► InfluxDB → Grafana   (cliente 1: real-time)
                             └─► MongoDB              (cliente 2: permanente)
```

### 2.1 Sorgente — Producer + Kafka
- Il **producer** simula la sorgente del Comune: pubblica una tupla
  `<parco; timestamp; persone>` per parco ogni 2 s su un unico topic Kafka
  `presenze.parchi`. Il nome del parco è un **tag** (non un topic per parco):
  un solo schema copre tutti i parchi e la "selezione dei parchi" del cliente 2
  diventa un semplice **filtro su tag**.
- **Kafka** è il backbone dello stream: disaccoppia sorgente e consumatori,
  assorbe i picchi (buffer) e permette a più consumer di leggere lo stesso
  stream in modo indipendente — esattamente la topologia richiesta (un flusso,
  due destinazioni diverse). **Kafdrop** è incluso solo per ispezione.

### 2.2 Ingestione/routing — Telegraf
Telegraf consuma il topic e **instrada senza codice applicativo**:
- verso **InfluxDB**: i dati grezzi di **tutti** i parchi (cliente 1);
- verso **MongoDB**: i soli parchi selezionati (`tagpass` sul tag `parco`),
  in modo **permanente** (nessun TTL — cliente 2).

Usare Telegraf come router dichiarativo è coerente con l'obiettivo della
traccia: *demandare il più possibile al data management system, riducendo al
minimo la programmazione*.

### 2.3 Cliente 1 — InfluxDB + Grafana (real-time, niente storico)
- **InfluxDB** (time-series DB) è il motore ideale per analisi a **finestra
  temporale** su un flusso ad alta frequenza. Il bucket ha **retention 24h**:
  poiché al cliente 1 non serve lo storico, non conserviamo dati vecchi.
- Le tre analisi richieste sono **query Flux** (il calcolo è nel DBMS):
  - **(a) > 10 ingressi in 2 min** — per ogni parco, `last - first`
    dell'occupazione sulla finestra di 2 minuti, filtrato a `> 10`.
  - **(b) > 3% uscite in 5 min** — per ogni parco,
    `(first - last) / first * 100` sulla finestra di 5 minuti, filtrato a `> 3`.
  - **(c) media e dev. std a 5/10/30 min** — `mean()` e `stddev()` per parco
    sulle rispettive finestre.
- **Grafana** è l'"interfaccia per eseguire analisi in tempo reale": dashboard
  con refresh a 5 s, tabelle di notifica per (a) e (b) con evidenziazione a
  soglia, tabelle statistiche per (c). Le stesse condizioni possono essere
  collegate a **Grafana Alerting** per notifiche push (canale di consegna fuori
  scope della prova).

### 2.4 Cliente 2 — MongoDB (permanente, per parchi selezionati)
- Richiesta: *memorizzare permanentemente tutti i dati* dei parchi scelti, su un
  DBMS installato "sul server del cliente". **MongoDB** (document store) è adatto
  a scrittura continua di documenti append-only e ad analisi di lungo termine
  (aggregation pipeline).
- **Il pacchetto software del cliente 2** è, nel prototipo, l'insieme
  *Telegraf (selezione parchi) + MongoDB (storage) + registro abbonamenti*:
  - la **selezione dei parchi** è la lista `parco` nel `tagpass` di Telegraf ed è
    documentata nella collezione `clienti_parchi`;
  - lo **storage** è la collezione `presenze`, **senza indice TTL** (a differenza
    della pipeline di riferimento, che invece scadenzava i dati): qui i dati sono
    permanenti.

## 3. Corrispondenza alla specifica

| Requisito | Dove è soddisfatto |
|---|---|
| Stream `<parco; timestamp; persone>` | `producer/producer.py` → topic `presenze.parchi` |
| Cliente 1: interfaccia analisi real-time | Grafana `parchi.json` |
| 1a: > 10 ingressi in 2 min | Pannello/Flux `last-first` (2 min) |
| 1b: > 3% uscite in 5 min | Pannello/Flux `(first-last)/first` (5 min) |
| 1c: media e dev. std a 5/10/30 min | Pannelli/Flux `mean()`, `stddev()` |
| Cliente 1: niente storico | Bucket InfluxDB con retention 24h |
| Cliente 2: memorizzare tutti i dati | Output MongoDB, nessun TTL |
| Cliente 2: scelta dei parchi | `tagpass` su `parco` + `clienti_parchi` |
| Minima programmazione | Routing Telegraf + query Flux, nessun consumer custom |

## 4. Assunzioni

- Il campo `persone` è l'**occupazione istantanea** del parco. "Ingressi" e
  "uscite" sono quindi interpretati come **variazione netta** dell'occupazione
  sulla finestra (`last - first`), non come conteggi lordi di transiti.
- Il `timestamp` della tupla è un epoch unix con millisecondi ed è usato come
  timestamp della misura (parsing `unix` in Telegraf).
- Il producer inietta a rotazione un afflusso e un deflusso, così da rendere
  osservabili le notifiche (a) e (b) durante una demo.

## 5. Problemi aperti e possibili soluzioni

- **Lordo vs netto (ingressi/uscite).** Con la sola occupazione non si
  distinguono i transiti lordi da quelli netti. *Soluzione:* se il Comune
  esponesse anche i tornelli (ingressi/uscite separati), (a)/(b) diverrebbero
  esatti; in alternativa `increase()`/`difference()` di Flux stimano i soli
  incrementi.
- **Trasferimento verso il server del cliente 2.** Nel prototipo MongoDB è unico.
  In produzione i dati vanno replicati sul server del cliente: si possono usare i
  **MongoDB Change Streams** (il cliente si sottoscrive e riceve gli aggiornamenti
  in tempo reale) oppure un **consumer Kafka per-cliente** che scrive sul suo DB.
- **Semantica del tempo.** Con dati fuori-ordine o in ritardo, le finestre "ultimi
  N minuti" andrebbero valutate su *event-time* con watermark (es. Kafka Streams /
  Flink) anziché su *processing-time*.
- **Scalabilità.** Un solo broker e partizione singola: in produzione si
  aumentano partizioni e repliche del topic e si scala InfluxDB/Mongo.
- **Retention del cliente 1.** 24h è una scelta prudente per coprire finestre fino
  a 30 min; è regolabile via `DOCKER_INFLUXDB_INIT_RETENTION`.
```
