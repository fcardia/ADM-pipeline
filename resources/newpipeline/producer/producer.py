"""
Simulatore sensori — Linea di imbottigliamento vino
====================================================
Macchina 1 — Riempitrice:
  - temperatura_liquido  (°C)       normale: 18–22  | anomalia: fino a 45°C
  - pressione_circuito   (bar)      normale: 1.5–2.5
  - bottiglie_riempite   (pz/min)   normale: 40–60

Macchina 2 — Tappatrice:
  - forza_tappatura      (N)        normale: 80–120
  - vibrazione_motore    (mm/s)     normale: 0.5–2.0 | anomalia: fino a 15 mm/s
  - bottiglie_tappate    (pz/min)   normale: 40–60

Il producer si comporta come un semplice sensore: pubblica solo valori numerici.
Le anomalie sono simulate variando i valori in modo ciclico.
"""

import json
import math
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configurazione ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RIEMPITRICE = os.getenv("TOPIC_RIEMPITRICE", "sensors.riempitrice")
TOPIC_TAPPATRICE = os.getenv("TOPIC_TAPPATRICE", "sensors.tappatrice")
INTERVAL = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "2"))

# Anomalie cicliche (in secondi)
ANOMALY_TEMP_PERIOD = 60    # ogni 60s parte il surriscaldamento
ANOMALY_TEMP_DURATION = 20  # dura 20s
ANOMALY_VIB_PERIOD = 90     # ogni 90s parte la vibrazione eccessiva
ANOMALY_VIB_DURATION = 25   # dura 25s


def wait_for_kafka(bootstrap: str, retries: int = 20, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[OK] Connesso a Kafka ({bootstrap})")
            return producer
        except NoBrokersAvailable:
            print(f"[WAIT] Kafka non pronto — tentativo {attempt}/{retries}, riprovo tra {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Impossibile connettersi a Kafka.")


def read_temperatura(elapsed: float) -> float:
    """Temperatura liquido in °C. Sale ciclicamente fino a ~45°C."""
    phase = elapsed % ANOMALY_TEMP_PERIOD
    if phase < ANOMALY_TEMP_DURATION:
        ramp = math.sin(math.pi * phase / ANOMALY_TEMP_DURATION)
        return round(random.uniform(18.0, 22.0) + ramp * random.uniform(13, 23), 2)
    return round(random.uniform(18.0, 22.0), 2)


def read_pressione() -> float:
    """Pressione circuito in bar."""
    return round(random.uniform(1.5, 2.5), 3)


def read_bottiglie_riempite() -> int:
    """Contatore bottiglie riempite al minuto."""
    return random.randint(40, 60)


def read_forza_tappatura() -> float:
    """Forza di tappatura in N."""
    return round(random.uniform(80.0, 120.0), 2)


def read_vibrazione(elapsed: float) -> float:
    """Vibrazione motore in mm/s. Sale ciclicamente fino a ~15 mm/s."""
    phase = elapsed % ANOMALY_VIB_PERIOD
    if phase < ANOMALY_VIB_DURATION:
        ramp = math.sin(math.pi * phase / ANOMALY_VIB_DURATION)
        return round(random.uniform(0.5, 2.0) + ramp * random.uniform(6, 13), 3)
    return round(random.uniform(0.5, 2.0), 3)


def read_bottiglie_tappate() -> int:
    """Contatore bottiglie tappate al minuto."""
    return random.randint(40, 60)


def main():
    print("=" * 50)
    print("  Simulatore Linea Imbottigliamento Vino")
    print("=" * 50)
    print(f"  Kafka:    {KAFKA_BOOTSTRAP}")
    print(f"  Intervallo: {INTERVAL}s")
    print("=" * 50)

    producer = wait_for_kafka(KAFKA_BOOTSTRAP)
    start_time = time.time()

    try:
        while True:
            elapsed = time.time() - start_time
            ts = datetime.now(timezone.utc).isoformat()

            # ── Riempitrice ──────────────────────────────────────
            temp = read_temperatura(elapsed)
            msg_r = {
                "machine_id": "riempitrice",
                "timestamp": ts,
                "temperatura_liquido_C": temp,
                "pressione_circuito_bar": read_pressione(),
                "bottiglie_riempite_pz_min": read_bottiglie_riempite(),
            }
            producer.send(TOPIC_RIEMPITRICE, value=msg_r)
            print(
                f"[RIEMPITRICE] T={msg_r['temperatura_liquido_C']:5.1f}°C | "
                f"P={msg_r['pressione_circuito_bar']:.2f}bar | "
                f"Bot={msg_r['bottiglie_riempite_pz_min']}pz/min"
            )

            # ── Tappatrice ───────────────────────────────────────
            vib = read_vibrazione(elapsed)
            msg_t = {
                "machine_id": "tappatrice",
                "timestamp": ts,
                "forza_tappatura_N": read_forza_tappatura(),
                "vibrazione_motore_mm_s": vib,
                "bottiglie_tappate_pz_min": read_bottiglie_tappate(),
            }
            producer.send(TOPIC_TAPPATRICE, value=msg_t)
            print(
                f"[TAPPATRICE ] F={msg_t['forza_tappatura_N']:6.1f}N | "
                f"V={msg_t['vibrazione_motore_mm_s']:.3f}mm/s | "
                f"Bot={msg_t['bottiglie_tappate_pz_min']}pz/min"
            )
            print()

            producer.flush()
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n[STOP] Producer fermato.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
