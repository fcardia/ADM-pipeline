#!/bin/bash

# Questo script bash viene eseguito AUTOMATICAMENTE dall'entrypoint dell'immagine
# influxdb:2.x (cartella /docker-entrypoint-initdb.d) SOLO al primo avvio,
# cioe' quando il volume influxdb-data è vuoto. 

set -e

# Bucket per lo storico aggregato (rollup orari): retention 0 = infinita
influx bucket create \
  --name farm_rollup \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --retention 0 \
  --description "Storico aggregato (rollup orari) - retention infinita"

echo "[init] bucket farm_rollup creato"

# crea una task per il rollup dei dati RAW in farm_raw verso il bucket farm_rollup, con frequenza 1 minuto
influx task create \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --file /docker-entrypoint-initdb.d/tasks/rollup_1m.flux

echo "[init] task rollup_1m creato"
