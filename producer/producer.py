import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "heart-records")
CSV_PATH = os.getenv("PRODUCER_CSV_PATH", "data/heart.csv")
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "2"))
LOOP = os.getenv("PRODUCER_LOOP", "true").lower() == "true"

FEATURE_COLS = [
    "Age", "Sex", "ChestPainType", "RestingBP", "Cholesterol",
    "FastingBS", "RestingECG", "MaxHR", "ExerciseAngina", "Oldpeak", "ST_Slope",
]
INT_COLS = {"Age", "RestingBP", "Cholesterol", "FastingBS", "MaxHR"}
FLOAT_COLS = {"Oldpeak"}


def cast(col: str, val: str):
    if col in INT_COLS:
        return int(val)
    if col in FLOAT_COLS:
        return float(val)
    return val


def make_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
        except NoBrokersAvailable:
            print(f"[Producer] Kafka no disponible, reintento {attempt}/{retries} en {delay}s...")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Kafka después de varios intentos.")


def publish_csv(producer: KafkaProducer, pass_num: int = 1):
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            features = {col: cast(col, row[col]) for col in FEATURE_COLS}
            message = {
                "patient_id": f"PAT-{idx:05d}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "features": features,
            }
            producer.send(TOPIC, value=message)
            print(
                f"[Pass {pass_num}] PAT-{idx:05d} → topic={TOPIC}"
                f" | HeartDisease(oculto)={row.get('HeartDisease', '?')}"
            )
            time.sleep(INTERVAL)


def main():
    producer = make_producer()
    print(f"[Producer] Conectado a {BOOTSTRAP_SERVERS}, topic={TOPIC}, loop={LOOP}")

    pass_num = 1
    while True:
        publish_csv(producer, pass_num)
        producer.flush()
        if not LOOP:
            print("[Producer] CSV completo. Saliendo.")
            break
        print(f"[Producer] Pass {pass_num} completo. Reiniciando CSV...")
        pass_num += 1


if __name__ == "__main__":
    main()
