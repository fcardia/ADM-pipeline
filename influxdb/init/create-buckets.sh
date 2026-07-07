#!/bin/bash

# Eseguito dall'entrypoint di influxdb:2.x solo al primo avvio (volume vuoto).

set -e

# Bucket storico aggregato: retention 0 = infinita
influx bucket create \
  --name farm_rollup \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --retention 0 \
  --description "Storico aggregato (rollup orari) - retention infinita"

echo "[init] bucket farm_rollup creato"

# Task di rollup farm_raw -> farm_rollup (ogni 1 minuto)
influx task create \
  --org "$DOCKER_INFLUXDB_INIT_ORG" \
  --file /docker-entrypoint-initdb.d/tasks/rollup_1m.flux

echo "[init] task rollup_1m creato"
