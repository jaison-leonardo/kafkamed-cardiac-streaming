# KafkaMed — Plataforma de Monitoreo Cardiaco en Streaming

Sistema de alerta temprana para riesgo de falla cardíaca usando Apache Kafka, Spark Structured Streaming, MongoDB y Flask. Corre completamente en Docker.

---

## Arquitectura

```
heart.csv
    │
    ▼
[Producer]  ──────→  [Kafka Broker]  ──────→  [Spark Consumer]  ──────→  [MongoDB]
kafka-python         apache/kafka:3.7.1        PySpark 3.5.3               mongo:7.0
                     topic: heart-records       + scikit-learn ML           colección: predictions
                                                                                │
                                                                                ▼
                                                                         [Flask API]
                                                                         /patients  /predictions
                                                                         /stats     /risk-summary
                                                                                │
                                                                                ▼
                                                                          [Power BI]
```

| Servicio       | Imagen base          | Puerto host |
|----------------|----------------------|-------------|
| Kafka (KRaft)  | apache/kafka:3.7.1   | 29092       |
| MongoDB        | mongo:7.0            | 27017       |
| Flask API      | python:3.11-slim     | 5000        |
| Producer       | python:3.11-slim     | —           |
| Consumer Spark | python:3.11-slim     | —           |

---

## Prerrequisitos

- **Docker Desktop** con WSL2 habilitado (Windows 10/11) o Docker Engine (Linux/Mac)
- **Python 3.11+** instalado localmente (solo para el paso de entrenamiento)
- El archivo `data/heart.csv` ya está incluido en el repositorio

---

## Despliegue — Paso a Paso

### Paso 1 — Clonar el repositorio

```bash
git clone https://github.com/jaison-leonardo/kafkamed-cardiac-streaming.git
cd kafkamed-cardiac-streaming
```

### Paso 2 — Entrenar el modelo ML (una sola vez)

Este paso genera los artefactos que usa el consumer en tiempo de ejecución.

```bash
pip install -r requirements-train.txt
python train.py
```

Salida esperada:
```
[Train] Dataset: 918 filas x 12 columnas
[Train] Train=734, Test=184

-- Metricas de evaluacion --
  Accuracy : 0.8859
  Precision: 0.8857
  Recall   : 0.9118
  F1       : 0.8986
  AUC-ROC  : 0.9327

[Train] Modelo guardado en artifacts/heart_pipeline.pkl
[Train] Columnas guardadas en artifacts/feature_columns.json
```

Verificar que existan los artefactos:
```bash
# Linux/Mac
ls artifacts/

# Windows PowerShell
dir artifacts\
```

Ambos archivos deben estar presentes:
- `artifacts/heart_pipeline.pkl`
- `artifacts/feature_columns.json`

> **Nota:** Si el repositorio ya incluye los artefactos en `artifacts/`, este paso es opcional.

### Paso 3 — Construir las imágenes Docker

```bash
docker compose build
```

Construye tres imágenes personalizadas: producer, consumer, api. Kafka y MongoDB usan imágenes oficiales y no requieren build.

### Paso 4 — Levantar toda la plataforma

```bash
docker compose up
```

Para ejecutar en segundo plano:
```bash
docker compose up -d
```

**Orden de arranque automático:**
1. `kafka` y `mongo` arrancan en paralelo con healthcheck
2. `kafka-init` espera a que Kafka esté healthy → crea el topic `heart-records` → termina
3. `producer`, `consumer` y `api` arrancan después de que `kafka-init` completó

> **Primera ejecución:** El consumer descarga los JARs de Kafka desde Maven Central (~200 MB). Puede tardar 2–5 minutos según la conexión a internet.

### Paso 5 — Verificar que el sistema funciona

```bash
# Estadísticas generales (total procesados, % alto riesgo, tasa/min)
curl http://localhost:5000/stats

# Últimas 10 predicciones
curl "http://localhost:5000/predictions?limit=10"

# Pacientes de alto riesgo (probabilidad >= 0.7)
curl "http://localhost:5000/risk-summary?threshold=0.7"

# Lista de pacientes únicos
curl http://localhost:5000/patients
```

El sistema está funcionando correctamente cuando `/stats` devuelve `total_processed > 0`.

---

## Verificación por Componente

### Kafka

```bash
# Listar topics activos
docker exec kafkamed-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Ver los últimos 3 mensajes publicados
docker exec kafkamed-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic heart-records \
  --from-beginning \
  --max-messages 3
```

### Producer

```bash
docker logs kafkamed-producer -f
```

Salida esperada:
```
[Producer] Conectado a kafka:9092, topic=heart-records, loop=True
[Pass 1] PAT-00001 → topic=heart-records | HeartDisease(oculto)=0
[Pass 1] PAT-00002 → topic=heart-records | HeartDisease(oculto)=1
```

### Consumer (Spark)

```bash
docker logs kafkamed-consumer -f
```

Salida esperada (después de la descarga de JARs):
```
[Consumer] Modelo cargado. Features: ['Age', 'Sex', ...]
[Consumer] Indice message_id (unique) asegurado en MongoDB.
[Consumer] Stream iniciado. Topic=heart-records
[Batch 0] upserted=5 matched=0 total=5
[Batch 1] upserted=5 matched=0 total=5
```

### MongoDB

```bash
# Contar documentos insertados
docker exec kafkamed-mongo mongosh kafkamed \
  --quiet --eval "db.predictions.countDocuments({})"

# Ver el último documento insertado
docker exec kafkamed-mongo mongosh kafkamed \
  --quiet --eval "printjson(db.predictions.findOne({},{_id:0}))"
```

---

## Referencia de la API

Base URL: `http://localhost:5000`

### GET /patients

Retorna todos los `patient_id` únicos que han pasado por el sistema.

```json
{
  "count": 918,
  "patients": ["PAT-00001", "PAT-00002", "..."]
}
```

### GET /predictions

Retorna las últimas N predicciones, ordenadas por fecha descendente.

**Query params:** `limit` (default=100, max=500) · `offset` (default=0) · `patient_id` (opcional)

```json
{
  "count": 100,
  "total": 918,
  "predictions": [
    {
      "patient_id": "PAT-00042",
      "message_id": "heart-records-0-41",
      "prediction": 1,
      "probability": 0.823,
      "processed_at": "2026-05-21T14:32:14.201Z"
    }
  ]
}
```

### GET /stats

Métricas agregadas del sistema en tiempo real.

```json
{
  "total_processed": 3360,
  "high_risk_count": 1456,
  "low_risk_count": 1904,
  "high_risk_percentage": 43.33,
  "last_processed_at": "2026-05-23T15:10:22.000Z",
  "processing_rate_per_minute": 30
}
```

### GET /risk-summary

Pacientes de alto riesgo agrupados con su probabilidad máxima y número de apariciones.

**Query params:** `threshold` (default=0.7) · `limit` (default=50, max=200)

```json
{
  "threshold": 0.7,
  "high_risk_patients": [
    {
      "patient_id": "PAT-00042",
      "max_probability": 0.952,
      "prediction_count": 3,
      "last_seen": "2026-05-23T15:10:22.000Z"
    }
  ]
}
```

---

## Power BI — Conexión al Dashboard

### Fuente de datos

1. Abrir Power BI Desktop
2. **Inicio → Obtener datos → Web**
3. Conectar cada uno de estos endpoints:
   - `http://localhost:5000/stats`
   - `http://localhost:5000/predictions?limit=500`
   - `http://localhost:5000/risk-summary?threshold=0.5&limit=100`

### Visualizaciones implementadas

| N° | Tipo            | Descripción                                      | Fuente        |
|----|-----------------|--------------------------------------------------|---------------|
| 1  | Tarjetas KPI    | Total procesados, alto riesgo, bajo riesgo, tasa | `/stats`      |
| 2  | Gráfico de dona | Distribución alto riesgo vs sin riesgo (%)       | `/stats`      |
| 3  | Tabla           | Últimas predicciones con columna de riesgo       | `/predictions`|
| 4  | Tabla           | Pacientes alto riesgo con barras de probabilidad | `/risk-summary`|

### Actualización en tiempo real

Para actualizar el dashboard mientras el producer publica:
- En Power BI Desktop: **Inicio → Actualizar**
- Configurar refresh automático: **Vista → Actualización de página** → cada 30 segundos (Cuando se cuenta con licencia de Power BI Premium)

---

## Comandos de Operación

```bash
# Detener todos los servicios
docker compose down

# Detener y borrar volúmenes (resetea Kafka y MongoDB completamente)
docker compose down -v

# Resetear checkpoints de Spark (permite releer desde el inicio del topic)
# Linux/Mac:
rm -rf checkpoints/spark-consumer
# Windows PowerShell:
Remove-Item -Recurse -Force checkpoints\spark-consumer -ErrorAction SilentlyContinue

# Reconstruir solo una imagen
docker compose up --build consumer

# Ver logs de todos los servicios
docker compose logs -f

# Ver logs de un servicio específico
docker compose logs kafkamed-consumer -f
```

---

## Solución de Problemas

| Síntoma | Causa | Solución |
|---------|-------|----------|
| Consumer crash inmediato | `heart_pipeline.pkl` no existe | Ejecutar `python train.py` |
| Consumer tarda 5+ min en arrancar | Descargando JARs Kafka de Maven | Esperar; requiere internet en el primer arranque |
| `UnknownTopicOrPartitionException` | Topic no existe aún | Verificar que `kafka-init` completó: `docker logs kafkamed-kafka-init` |
| Producer: `NoBrokersAvailable` | Kafka aún inicializando | El producer reintenta automáticamente hasta 10 veces |
| API devuelve `total_processed: 0` | Consumer procesando primer batch | Esperar ~10 segundos |
| `ClassNotFoundError` en consumer | JAR Kafka con versión Scala incorrecta | El JAR `_2.12` es correcto para esta imagen |
| `ps: command not found` en logs | PySpark en imagen slim sin procps | Advertencia visual; no afecta el funcionamiento |

---

## Estructura del Repositorio

```
kafkamed-cardiac-streaming/
│
├── producer/
│   ├── producer.py            Productor Kafka (lee CSV, publica JSON)
│   └── requirements.txt       kafka-python, python-dotenv
│
├── consumer/
│   ├── consumer.py            Spark Structured Streaming + inferencia ML
│   └── requirements.txt       pyspark, pymongo, scikit-learn, joblib, pandas
│
├── api/
│   ├── app.py                 Flask API REST (4 endpoints)
│   └── requirements.txt       flask, pymongo, python-dotenv
│
├── docker/
│   ├── Dockerfile.producer    Imagen del productor
│   ├── Dockerfile.consumer    Imagen del consumer (incluye Java 17)
│   └── Dockerfile.api         Imagen de Flask
│
├── data/
│   └── heart.csv              Dataset clínico (918 registros, 12 columnas)
│
├── artifacts/
│   ├── heart_pipeline.pkl     Pipeline scikit-learn serializado (generado por train.py)
│   └── feature_columns.json   Orden de features para el consumer
│
├── checkpoints/               Estado de Spark Streaming (generado en runtime)
├── powerbi/
│   └── kafkamed_dashboard.pbix  Dashboard Power BI con 4 visualizaciones
│
├── train.py                   Entrena y serializa el pipeline ML
├── requirements-train.txt     Dependencias para entrenamiento local
├── docker-compose.yml         Orquestación completa de 6 servicios
├── .env.example               Plantilla de variables de entorno
├── .gitignore
└── README.md
```

---

## Tecnologías y Versiones

| Componente           | Versión    | Justificación                                              |
|----------------------|------------|------------------------------------------------------------|
| Apache Kafka KRaft   | 3.7.1      | Última estable; modo KRaft elimina dependencia de Zookeeper |
| Spark Structured Streaming | 3.5.3 | LTS actual; compatible con Python 3.11 y Kafka 3.x      |
| Python               | 3.11       | Compatible con PySpark 3.5 y scikit-learn 1.4.x            |
| MongoDB              | 7.0        | LTS actual; wire protocol estable con PyMongo 4.7          |
| Flask                | 3.0.3      | Ligero; suficiente para una API REST de lectura            |
| scikit-learn         | 1.4.2      | Pipeline serializable con joblib, usado sin cambios en Spark |
| kafka-python         | 2.0.2      | Cliente Python puro; adecuado para producer básico         |
