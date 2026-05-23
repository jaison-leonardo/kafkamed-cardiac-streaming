"""
Flask API REST para KafkaMed.

Endpoints:
  GET /patients      → pacientes únicos procesados
  GET /predictions   → listado paginado de predicciones
  GET /stats         → métricas agregadas del sistema
  GET /risk-summary  → pacientes de alto riesgo
"""

import os

from flask import Flask, jsonify, request
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "kafkamed")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "predictions")

app = Flask(__name__)
_client = MongoClient(MONGO_URI)
collection = _client[MONGO_DB][MONGO_COLLECTION]


@app.route("/patients")
def patients():
    patient_ids = collection.distinct("patient_id")
    return jsonify({"count": len(patient_ids), "patients": sorted(patient_ids)})


@app.route("/predictions")
def predictions():
    limit = min(int(request.args.get("limit", 100)), 500)
    offset = int(request.args.get("offset", 0))
    patient_id = request.args.get("patient_id")

    query = {"patient_id": patient_id} if patient_id else {}
    total = collection.count_documents(query)

    cursor = (
        collection.find(query, {"_id": 0, "features": 0})
        .sort("processed_at", -1)
        .skip(offset)
        .limit(limit)
    )
    docs = list(cursor)
    return jsonify({"count": len(docs), "total": total, "predictions": docs})


@app.route("/stats")
def stats():
    total = collection.count_documents({})
    if total == 0:
        return jsonify({
            "total_processed": 0,
            "high_risk_count": 0,
            "low_risk_count": 0,
            "high_risk_percentage": 0.0,
            "last_processed_at": None,
        })

    high_risk = collection.count_documents({"prediction": 1})
    low_risk = total - high_risk

    last_doc = collection.find_one(
        {}, {"processed_at": 1, "_id": 0}, sort=[("processed_at", -1)]
    )
    last_ts = last_doc["processed_at"] if last_doc else None

    # Tasa: documentos en el último minuto
    from datetime import datetime, timedelta, timezone
    one_minute_ago = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    recent = collection.count_documents({"processed_at": {"$gte": one_minute_ago}})

    return jsonify({
        "total_processed": total,
        "high_risk_count": high_risk,
        "low_risk_count": low_risk,
        "high_risk_percentage": round(high_risk / total * 100, 2),
        "last_processed_at": last_ts,
        "processing_rate_per_minute": recent,
    })


@app.route("/risk-summary")
def risk_summary():
    threshold = float(request.args.get("threshold", 0.7))
    limit = min(int(request.args.get("limit", 50)), 200)

    pipeline = [
        {"$match": {"prediction": 1, "probability": {"$gte": threshold}}},
        {
            "$group": {
                "_id": "$patient_id",
                "max_probability": {"$max": "$probability"},
                "prediction_count": {"$sum": 1},
                "last_seen": {"$max": "$processed_at"},
            }
        },
        {"$sort": {"max_probability": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "patient_id": "$_id",
                "max_probability": 1,
                "prediction_count": 1,
                "last_seen": 1,
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    return jsonify({"threshold": threshold, "high_risk_patients": results})


if __name__ == "__main__":
    port = int(os.getenv("FLASK_PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    print(f"[API] Iniciando en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
