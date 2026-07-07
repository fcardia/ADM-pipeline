#!/usr/bin/env python3
"""
sensor_simulator.py
====================

Simulatore di sensori per lo scenario "Smart Farm" (hackathon Advanced Data
Management).

Genera in tempo reale letture da 4 categorie di sensori distribuiti su 3
zone di un'azienda agricola:

  1. air_quality      - qualita' dell'aria (per zona)
  2. soil_quality      - qualita' del suolo: pH, NPK, conducibilita' (per zona)
  3. soil_moisture      - umidita' del suolo (per zona)
  4. power_consumption  - consumi elettrici real-time dei componenti di
                          automazione (pompe, ventole, riscaldatori, luci,
                          gateway) - non legati a una singola zona

Ogni lettura e' un oggetto JSON. Il generatore inietta periodicamente
anomalie realistiche (picco di inquinamento, stress idrico, sovraccarico
elettrico) in modo che i gruppi possano verificare le regole di alerting
che implementano.

OUTPUT
------
Questo script scrive i dati generati SOLO in due posti:

  1. stdout   - una riga JSON per ogni lettura (utile per il debug e per
                vedere il flusso "live" nei log del container).
  2. un file CSV (default: data/sensor_data.csv) - un'unica tabella con
     schema "largo" (union delle colonne di tutti i tipi di sensore); le
     colonne non pertinenti a un dato tipo di sensore restano vuote.

NON pubblica nulla su MQTT o Kafka: questo e' il compito che i gruppi
devono realizzare durante l'hackathon, implementando le funzioni 
publish_to_mqtt() / publish_to_kafka() qui sotto (gia' predisposte come 
"punto di estensione"), che vengono chiamate automaticamente per ogni 
lettura generata.


"""

import argparse
import csv
import json
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# --------------------------------------------------------------------------
# Configurazione dominio: zone e componenti di automazione
# --------------------------------------------------------------------------

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_AIR_QUALITY = os.getenv("TOPIC_AIR_QUALITY", "sensors/air_quality")
TOPIC_SOIL_QUALITY = os.getenv("TOPIC_SOIL_QUALITY", "sensors/soil_quality")
TOPIC_MOISTURE = os.getenv("TOPIC_MOISTURE", "sensors/soil_moisture")
TOPIC_POWER_CONSUMPTION = os.getenv("TOPIC_POWER_CONSUMPTION", "sensors/power_consumption")


ZONES = ["A", "B", "C"]
ACTUATORS = [
    # id, potenza nominale (W), tipo
    {"id": "irrigation_pump_1", "nominal_w": 750, "type": "pump", "zone": "A"},
    {"id": "irrigation_pump_2", "nominal_w": 750, "type": "pump", "zone": "B"},
    {"id": "irrigation_pump_3", "nominal_w": 750, "type": "pump", "zone": "C"},
    {"id": "greenhouse_fan_1", "nominal_w": 180, "type": "fan", "zone": "C"},
    {"id": "greenhouse_heater_1", "nominal_w": 2000, "type": "heater", "zone": "C"},
    {"id": "led_grow_light_1", "nominal_w": 400, "type": "lighting", "zone": "C"},
    {"id": "gateway_controller", "nominal_w": 15, "type": "gateway", "zone": None},
]

# Schema "largo" del CSV: union di tutte le colonne usate dai 4 tipi di
# sensore. Le colonne non pertinenti a una riga restano vuote.
CSV_FIELDNAMES = [
    "timestamp", "sensor_id", "type", "zone",
    "actuator_id", "actuator_type", "status",
    "temperature_c", "humidity_pct", "co2_ppm", "pm25_ugm3", "pm10_ugm3",
    "ph", "ec_dsm", "nitrogen_mgkg", "phosphorus_mgkg", "potassium_mgkg",
    "soil_temperature_c", "soil_moisture_pct",
    "power_w", "voltage_v", "current_a",
]

STOP = False

def _handle_sigint(signum, frame):
    global STOP
    STOP = True


signal.signal(signal.SIGINT, _handle_sigint)
signal.signal(signal.SIGTERM, _handle_sigint)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------
# Stato interno per rendere le serie realistiche (drift lento + rumore +
# eventi anomali temporanei, invece di rumore puramente indipendente)
# --------------------------------------------------------------------------

@dataclass
class ZoneState:
    zone: str
    temperature_c: float = field(default_factory=lambda: random.uniform(18, 26))
    humidity_pct: float = field(default_factory=lambda: random.uniform(45, 65))
    co2_ppm: float = field(default_factory=lambda: random.uniform(400, 550))
    pm25_ugm3: float = field(default_factory=lambda: random.uniform(5, 15))
    pm10_ugm3: float = field(default_factory=lambda: random.uniform(10, 25))
    ph: float = field(default_factory=lambda: random.uniform(6.0, 7.2))
    ec_dsm: float = field(default_factory=lambda: random.uniform(0.8, 1.6))
    n_mgkg: float = field(default_factory=lambda: random.uniform(20, 40))
    p_mgkg: float = field(default_factory=lambda: random.uniform(15, 30))
    k_mgkg: float = field(default_factory=lambda: random.uniform(100, 180))
    soil_temp_c: float = field(default_factory=lambda: random.uniform(15, 22))
    soil_moisture_pct: float = field(default_factory=lambda: random.uniform(35, 55))
    active_events: dict = field(default_factory=dict)


@dataclass
class ActuatorState:
    actuator_id: str
    nominal_w: float
    type: str
    zone: Optional[str]
    status: str = "off"
    ticks_in_state: int = 0
    overload_ticks_left: int = 0


# --------------------------------------------------------------------------
# Generatori di letture
# --------------------------------------------------------------------------

def _clip(value, lo, hi):
    return max(lo, min(hi, value))


def maybe_start_event(state: ZoneState, name: str, probability: float, duration_range):
    if name not in state.active_events and random.random() < probability:
        state.active_events[name] = random.randint(*duration_range)


def tick_events(state: ZoneState):
    expired = []
    for name, ticks in state.active_events.items():
        ticks -= 1
        if ticks <= 0:
            expired.append(name)
        else:
            state.active_events[name] = ticks
    for name in expired:
        del state.active_events[name]


AIR_QUALITY_BASELINE = {
    "co2_ppm": 475.0,
    "pm25_ugm3": 10.0,
    "pm10_ugm3": 17.0,
}
AIR_QUALITY_REVERSION_RATE = 0.05  # quota di rientro verso la baseline ad ogni ciclo


def gen_air_quality(state: ZoneState) -> dict:
    state.temperature_c = _clip(state.temperature_c + random.uniform(-0.3, 0.3), 5, 40)
    state.humidity_pct = _clip(state.humidity_pct + random.uniform(-1.5, 1.5), 10, 95)
    state.co2_ppm += (AIR_QUALITY_BASELINE["co2_ppm"] - state.co2_ppm) * AIR_QUALITY_REVERSION_RATE
    state.pm25_ugm3 += (AIR_QUALITY_BASELINE["pm25_ugm3"] - state.pm25_ugm3) * AIR_QUALITY_REVERSION_RATE
    state.pm10_ugm3 += (AIR_QUALITY_BASELINE["pm10_ugm3"] - state.pm10_ugm3) * AIR_QUALITY_REVERSION_RATE

    state.co2_ppm = _clip(state.co2_ppm + random.uniform(-8, 8), 350, 2500)
    state.pm25_ugm3 = _clip(state.pm25_ugm3 + random.uniform(-1.5, 1.5), 1, 300)
    state.pm10_ugm3 = _clip(state.pm10_ugm3 + random.uniform(-2, 2), 2, 400)

    maybe_start_event(state, "pollution_spike", probability=0.005, duration_range=(6, 20))
    if "pollution_spike" in state.active_events:
        state.pm25_ugm3 = _clip(state.pm25_ugm3 + random.uniform(40, 90), 1, 300)
        state.pm10_ugm3 = _clip(state.pm10_ugm3 + random.uniform(50, 110), 2, 400)
        state.co2_ppm = _clip(state.co2_ppm + random.uniform(100, 300), 350, 2500)

    return {
        "sensor_id": f"air_zone{state.zone}",
        "type": "air_quality",
        "zone": state.zone,
        "timestamp": now_iso(),
        "temperature_c": round(state.temperature_c, 2),
        "humidity_pct": round(state.humidity_pct, 2),
        "co2_ppm": round(state.co2_ppm, 1),
        "pm25_ugm3": round(state.pm25_ugm3, 1),
        "pm10_ugm3": round(state.pm10_ugm3, 1),
    }


def gen_soil_quality(state: ZoneState) -> dict:
    state.ph = _clip(state.ph + random.uniform(-0.03, 0.03), 4.0, 9.0)
    state.ec_dsm = _clip(state.ec_dsm + random.uniform(-0.05, 0.05), 0.1, 5.0)
    state.n_mgkg = _clip(state.n_mgkg + random.uniform(-1, 1), 0, 100)
    state.p_mgkg = _clip(state.p_mgkg + random.uniform(-1, 1), 0, 100)
    state.k_mgkg = _clip(state.k_mgkg + random.uniform(-3, 3), 0, 300)
    state.soil_temp_c = _clip(state.soil_temp_c + random.uniform(-0.2, 0.2), 2, 35)

    return {
        "sensor_id": f"soil_quality_zone{state.zone}",
        "type": "soil_quality",
        "zone": state.zone,
        "timestamp": now_iso(),
        "ph": round(state.ph, 2),
        "ec_dsm": round(state.ec_dsm, 2),
        "nitrogen_mgkg": round(state.n_mgkg, 1),
        "phosphorus_mgkg": round(state.p_mgkg, 1),
        "potassium_mgkg": round(state.k_mgkg, 1),
        "soil_temperature_c": round(state.soil_temp_c, 2),
    }


def gen_soil_moisture(state: ZoneState, irrigation_active: bool) -> dict:
    state.soil_moisture_pct -= random.uniform(0.05, 0.25)

    maybe_start_event(state, "dry_spell", probability=0.02, duration_range=(20, 60))
    is_dry_spell = "dry_spell" in state.active_events
    if is_dry_spell:
        state.soil_moisture_pct -= random.uniform(0.3, 0.8)

    if irrigation_active:
        if is_dry_spell:
            state.soil_moisture_pct += random.uniform(0.2, 0.6)
        else:
            state.soil_moisture_pct += random.uniform(1.5, 3.5)

    state.soil_moisture_pct = _clip(state.soil_moisture_pct, 2, 95)

    return {
        "sensor_id": f"soil_moisture_zone{state.zone}",
        "type": "soil_moisture",
        "zone": state.zone,
        "timestamp": now_iso(),
        "soil_moisture_pct": round(state.soil_moisture_pct, 2),
    }


def gen_power_consumption(actuator: ActuatorState, zone_state: Optional[ZoneState]) -> dict:
    if actuator.type == "pump" and zone_state is not None:
        turn_on = zone_state.soil_moisture_pct < 30
    elif actuator.type == "gateway":
        turn_on = True
    else:
        if actuator.ticks_in_state > random.randint(10, 40):
            turn_on = random.random() < 0.5
            actuator.ticks_in_state = 0
        else:
            turn_on = actuator.status == "on"

    new_status = "on" if turn_on else "off"
    if new_status == actuator.status:
        actuator.ticks_in_state += 1
    else:
        actuator.ticks_in_state = 0
    actuator.status = new_status

    if actuator.status == "off":
        power_w = random.uniform(0, 2)
        voltage_v = round(random.uniform(228, 232), 1)
        current_a = round(power_w / voltage_v, 3)
    else:
        power_w = actuator.nominal_w * random.uniform(0.92, 1.05)

        if actuator.overload_ticks_left == 0 and random.random() < 0.04:
            actuator.overload_ticks_left = random.randint(4, 12)
        if actuator.overload_ticks_left > 0:
            power_w *= random.uniform(1.6, 2.3)
            actuator.overload_ticks_left -= 1

        voltage_v = round(random.uniform(215, 232), 1)
        current_a = round(power_w / voltage_v, 3)

    return {
        "sensor_id": f"power_{actuator.actuator_id}",
        "type": "power_consumption",
        "actuator_id": actuator.actuator_id,
        "actuator_type": actuator.type,
        "zone": actuator.zone,
        "timestamp": now_iso(),
        "status": actuator.status,
        "power_w": round(power_w, 1),
        "voltage_v": voltage_v,
        "current_a": current_a,
    }


# --------------------------------------------------------------------------
# Output: stdout + CSV
# --------------------------------------------------------------------------

def open_csv_writer(csv_path: str):
    """Apre (creando le cartelle necessarie) il file CSV in append e
    restituisce (file_handle, DictWriter). Scrive l'header solo se il file
    e' nuovo/vuoto."""
    dirname = os.path.dirname(csv_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    f = open(csv_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, restval="")
    if write_header:
        writer.writeheader()
        f.flush()
    return f, writer


def write_csv_row(writer: "csv.DictWriter", csv_file, payload: dict):
    writer.writerow(payload)
    csv_file.flush()  # cosi' un `tail -f` sul file vede subito le nuove righe



import paho.mqtt.client as mqtt

def create_mqtt_client() -> mqtt.Client:
    """
    Crea un client MQTT compatibile sia con paho-mqtt 1.x sia con paho-mqtt 2.x.
    """
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        return mqtt.Client()

def wait_for_mqtt(host: str, port: int, retries: int = 20, delay: int = 5) -> mqtt.Client:
    """
    Attende che Mosquitto/MQTT sia pronto e restituisce un client connesso.
    """
    for attempt in range(1, retries + 1):
        client = create_mqtt_client()

        try:
            result_code = client.connect(host, port, keepalive=60)

            if result_code != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Connessione MQTT fallita con codice: {result_code}")

            client.loop_start()
            print(f"[OK] Connesso a Mosquitto/MQTT ({host}:{port})")
            return client

        except Exception as e:
            print(
                f"[WAIT] MQTT non pronto — tentativo {attempt}/{retries}, "
                f"riprovo tra {delay}s... ({e})"
            )

            try:
                client.disconnect()
            except Exception:
                pass

            time.sleep(delay)

    raise RuntimeError("Impossibile connettersi a Mosquitto/MQTT.")

def publish_to_mqtt(client: mqtt.Client, topic: str, payload: dict) -> None:
    """
    Pubblica un messaggio JSON su un topic MQTT.
    """
    message = json.dumps(payload)
    result = client.publish(topic, message, qos=0)

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(
            f"Errore pubblicazione MQTT su topic '{topic}', codice: {result.rc}"
        )

def emit(payload: dict, csv_file, csv_writer, mqtt_client: mqtt.Client, topic: str) -> None:
    # print(json.dumps(payload), flush=True)
    write_csv_row(csv_writer, csv_file, payload)
    publish_to_mqtt(mqtt_client, topic, payload)
    print(f"[MQTT] Pubblicato su topic '{topic}': {json.dumps(payload)}", flush=True)
    


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Simulatore di sensori Smart Farm per l'hackathon ADM."
    )
    parser.add_argument("--interval", type=float, default=5.0,
                         help="Secondi tra un ciclo di letture e il successivo (default: 5). "
                              "Controlla la cadenza sia dello stdout sia della scrittura su CSV.")
    parser.add_argument("--csv-path", default="data/sensor_data.csv",
                         help="Percorso del file CSV su cui appendere le letture (default: data/sensor_data.csv).")
    parser.add_argument("--duration", type=float, default=None,
                         help="Durata totale della simulazione in secondi (default: infinita, Ctrl+C per fermare).")
    parser.add_argument("--seed", type=int, default=None, help="Seed per generazione deterministica.")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Diagnostica di avvio: stampata SEMPRE, cosi' se il CSV non compare dove
    # ci si aspetta, basta guardare i log (es. `docker compose logs
    # sensor-simulator`) per capire subito quale percorso assoluto sta usando
    # il processo e se la cartella e' scrivibile.
    abs_csv_path = os.path.abspath(args.csv_path)
    abs_dir = os.path.dirname(abs_csv_path) or "."
    print(f"# [diagnostica] cwd corrente: {os.getcwd()}", file=sys.stderr)
    print(f"# [diagnostica] --csv-path ricevuto: {args.csv_path!r}", file=sys.stderr)
    print(f"# [diagnostica] percorso assoluto del CSV: {abs_csv_path}", file=sys.stderr)
    print(f"# [diagnostica] cartella scrivibile? {os.access(abs_dir, os.W_OK) if os.path.isdir(abs_dir) else 'cartella non ancora creata'}", file=sys.stderr)

    try:
        csv_file, csv_writer = open_csv_writer(args.csv_path)
    except OSError as exc:
        print(f"# [ERRORE] impossibile aprire/creare il file CSV in {abs_csv_path!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    zone_states = {z: ZoneState(zone=z) for z in ZONES}
    actuator_states = [
        ActuatorState(actuator_id=a["id"], nominal_w=a["nominal_w"], type=a["type"], zone=a["zone"])
        for a in ACTUATORS
    ]

    print(
        f"# Avvio simulazione | interval={args.interval}s csv={args.csv_path} "
        f"duration={'inf' if args.duration is None else args.duration}s",
        file=sys.stderr,
    )

    start_time = time.time()
    tick = 0
    mqtt_client = wait_for_mqtt(host=MQTT_HOST, port=MQTT_PORT)
    try:
        while not STOP:
            if args.duration is not None and (time.time() - start_time) >= args.duration:
                break

            for zone in ZONES:
                zs = zone_states[zone]
                tick_events(zs)

                emit(gen_air_quality(zs), csv_file, csv_writer, mqtt_client, TOPIC_AIR_QUALITY)
                emit(gen_soil_quality(zs), csv_file, csv_writer, mqtt_client, TOPIC_SOIL_QUALITY)

                irrigation_active = any(
                    a.status == "on" for a in actuator_states
                    if a.type == "pump" and a.zone == zone
                )
                emit(gen_soil_moisture(zs, irrigation_active), csv_file, csv_writer, mqtt_client, TOPIC_MOISTURE)

            for actuator in actuator_states:
                zone_state = zone_states.get(actuator.zone) if actuator.zone else None
                emit(gen_power_consumption(actuator, zone_state), csv_file, csv_writer, mqtt_client, TOPIC_POWER_CONSUMPTION)

            tick += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        csv_file.close()
        mqtt_client.disconnect()

    print(f"# Simulazione terminata dopo {tick} cicli.", file=sys.stderr)


if __name__ == "__main__":
    main()
