"""
Consumidor Spark Structured Streaming para KafkaMed.

Lee del topic heart-records, aplica el modelo ML y escribe en MongoDB.
Las escrituras usan upsert sobre message_id para semántica effectively-once:
garantiza que no haya duplicados si Spark re-ejecuta un batch, pero NO
constituye exactly-once transaccional al carecer de coordinación atómica
entre el checkpoint de Kafka y la escritura en MongoDB.
"""

import json
import os
from datetime import datetime, timezone

import joblib
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Configuración desde variables de entorno ──────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "heart-records")
CHECKPOINT_PATH = os.getenv("SPARK_CHECKPOINT_PATH", "/checkpoints/spark-consumer")
MODEL_PATH = os.getenv("MODEL_PATH", "/artifacts/heart_pipeline.pkl")
FEATURE_COLUMNS_PATH = os.getenv("FEATURE_COLUMNS_PATH", "/artifacts/feature_columns.json")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "kafkamed")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "predictions")
TRIGGER_SECONDS = int(os.getenv("SPARK_TRIGGER_SECONDS", "5"))
APP_NAME = os.getenv("SPARK_APP_NAME", "KafkaMedConsumer")

# ── Artefactos ML cargados una sola vez al arrancar ───────────────────────────
print(f"[Consumer] Cargando modelo desde {MODEL_PATH}...")
pipeline_model = joblib.load(MODEL_PATH)

with open(FEATURE_COLUMNS_PATH, encoding="utf-8") as f:
    FEATURE_COLS = json.load(f)  # list[str] — anotación omitida: Python 3.8 no soporta list[str]

print(f"[Consumer] Modelo cargado. Features: {FEATURE_COLS}")

# Índice único en message_id — previene duplicados si Spark re-ejecuta un batch.
# Se crea al arrancar; es idempotente (no falla si ya existe).
_mongo_init = MongoClient(MONGO_URI)
_mongo_init[MONGO_DB][MONGO_COLLECTION].create_index(
    "message_id", unique=True, background=True
)
_mongo_init.close()
print("[Consumer] Indice message_id (unique) asegurado en MongoDB.")

# ── Schema del mensaje Kafka ──────────────────────────────────────────────────
MESSAGE_SCHEMA = StructType([
    StructField("patient_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("features", StructType([
        StructField("Age", IntegerType()),
        StructField("Sex", StringType()),
        StructField("ChestPainType", StringType()),
        StructField("RestingBP", IntegerType()),
        StructField("Cholesterol", IntegerType()),
        StructField("FastingBS", IntegerType()),
        StructField("RestingECG", StringType()),
        StructField("MaxHR", IntegerType()),
        StructField("ExerciseAngina", StringType()),
        StructField("Oldpeak", DoubleType()),
        StructField("ST_Slope", StringType()),
    ])),
])


def process_batch(batch_df, batch_id: int):
    """Aplica el modelo ML y escribe en MongoDB con upsert idempotente."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    operations = []
    for row in rows:
        if row.patient_id is None:
            continue

        features = {
            "Age": row.Age,
            "Sex": row.Sex,
            "ChestPainType": row.ChestPainType,
            "RestingBP": row.RestingBP,
            "Cholesterol": row.Cholesterol,
            "FastingBS": row.FastingBS,
            "RestingECG": row.RestingECG,
            "MaxHR": row.MaxHR,
            "ExerciseAngina": row.ExerciseAngina,
            "Oldpeak": float(row.Oldpeak) if row.Oldpeak is not None else 0.0,
            "ST_Slope": row.ST_Slope,
        }

        input_df = pd.DataFrame([features])[FEATURE_COLS]
        prediction = int(pipeline_model.predict(input_df)[0])
        probability = round(float(pipeline_model.predict_proba(input_df)[0][1]), 4)

        message_id = f"{TOPIC}-{row.partition}-{row.offset}"
        doc = {
            "patient_id": row.patient_id,
            "message_id": message_id,
            "features": features,
            "prediction": prediction,
            "probability": probability,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        # Upsert: inserta solo si el message_id no existe (idempotencia)
        operations.append(
            UpdateOne(
                {"message_id": message_id},
                {"$setOnInsert": doc},
                upsert=True,
            )
        )

    if operations:
        result = collection.bulk_write(operations, ordered=False)
        print(
            f"[Batch {batch_id}] upserted={result.upserted_count} "
            f"matched={result.matched_count} total={len(operations)}"
        )

    client.close()


def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config("spark.jars.ivy", "/tmp/ivy2")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            col("partition"),
            col("offset"),
            from_json(col("value").cast("string"), MESSAGE_SCHEMA).alias("msg"),
        )
        .select(
            col("partition"),
            col("offset"),
            col("msg.patient_id"),
            col("msg.timestamp"),
            col("msg.features.*"),
        )
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(f"[Consumer] Stream iniciado. Topic={TOPIC}, Checkpoint={CHECKPOINT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
