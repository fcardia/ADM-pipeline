"""
Simulatore stream presenze parchi — Comune di Cagliari (fittizio)
=================================================================
Il comune fornisce in streaming, in forma anonima, il numero di persone
attualmente presente in ogni parco cittadino. Ogni dato dello stream è una
tupla <parco; timestamp; persone>:

    parco       nome del parco                (string, usato come tag)
    timestamp   istante a cui si riferisce    (float, epoch unix con frazioni)
    persone     numero di persone nel parco   (int)

Esempio (come da specifica):  <Monte Urpinu; 1549275817.154; 118>

Il producer si comporta come la sorgente del comune: pubblica solo la tupla.
L'occupazione di ogni parco segue una passeggiata aleatoria con ritorno verso
un valore di base (mean-reverting). Per rendere osservabili le analisi real-time
del cliente di tipo 1, a rotazione un parco subisce:

  - un AFFLUSSO   (surge)  → ingresso netto di molte persone in ~2 minuti
                             (fa scattare la notifica "> 10 ingressi in 2 min")
  - un DEFLUSSO   (exodus) → uscita di una quota di persone in ~5 minuti
                             (fa scattare la notifica "> 3% uscite in 5 min")
"""

import json
import os
import random
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configurazione ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC_PRESENZE", "presenze.parchi")
INTERVAL = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "2"))

# Parchi cittadini e loro occupazione media di riferimento.
PARCHI_BASE = {
    "Monte Urpinu": 120,
    "Molentargius": 200,
    "San Michele": 80,
    "Giardini Pubblici": 150,
    "Monte Claro": 100,
    "Terramaini": 60,
}

# Finestre degli eventi (in secondi). A rotazione un parco è in surge e uno in
# exodus; gli indici sono sfasati così da colpire parchi diversi.
SURGE_WINDOW = 120     # un parco "in afflusso" per 2 minuti
EXODUS_WINDOW = 300    # un parco "in deflusso" per 5 minuti


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


def step(parco: str, stato: float, base: float, surge: bool, exodus: bool) -> float:
    """Aggiorna l'occupazione di un parco per un tick."""
    # Rumore di fondo + ritorno graduale verso il valore di base.
    delta = (base - stato) / 40.0 + random.uniform(-2.0, 2.0)
    if surge:
        delta += random.uniform(1.5, 3.0)     # forte ingresso netto
    if exodus:
        delta -= random.uniform(1.5, 3.0)     # forte uscita netta
    nuovo = stato + delta
    return max(0.0, min(nuovo, base * 3.0))   # clamp in [0, 3x base]


def main():
    print("=" * 56)
    print("  Stream presenze parchi — Comune di Cagliari (sim.)")
    print("=" * 56)
    print(f"  Kafka:      {KAFKA_BOOTSTRAP}")
    print(f"  Topic:      {TOPIC}")
    print(f"  Intervallo: {INTERVAL}s")
    print(f"  Parchi:     {', '.join(PARCHI_BASE)}")
    print("=" * 56)

    producer = wait_for_kafka(KAFKA_BOOTSTRAP)
    parchi = list(PARCHI_BASE)
    stato = {p: float(v) for p, v in PARCHI_BASE.items()}
    start_time = time.time()

    try:
        while True:
            elapsed = time.time() - start_time
            surge_parco = parchi[int(elapsed // SURGE_WINDOW) % len(parchi)]
            exodus_parco = parchi[(int(elapsed // EXODUS_WINDOW) + 3) % len(parchi)]

            for parco in parchi:
                stato[parco] = step(
                    parco,
                    stato[parco],
                    PARCHI_BASE[parco],
                    surge=(parco == surge_parco),
                    exodus=(parco == exodus_parco),
                )
                persone = int(round(stato[parco]))
                # Tupla dello stream: <parco; timestamp; persone>
                msg = {
                    "parco": parco,
                    "timestamp": round(time.time(), 3),  # epoch unix con ms
                    "persone": persone,
                }
                producer.send(TOPIC, value=msg)

            flags = f"↑{surge_parco}  ↓{exodus_parco}"
            righe = "  ".join(f"{p}={int(round(stato[p]))}" for p in parchi)
            print(f"[{flags}]\n  {righe}\n")

            producer.flush()
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n[STOP] Producer fermato.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
