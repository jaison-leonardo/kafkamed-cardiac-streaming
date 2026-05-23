"""
Entrena el pipeline ML sobre heart.csv y guarda los artefactos en artifacts/.

Uso (desde la raíz del proyecto):
    pip install -r requirements-train.txt
    python train.py

Salida:
    artifacts/heart_pipeline.pkl      Pipeline scikit-learn serializado
    artifacts/feature_columns.json    Lista ordenada de las 11 features
"""

import json
import os

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

CSV_PATH = os.path.join("data", "heart.csv")
MODEL_OUT = os.path.join("artifacts", "heart_pipeline.pkl")
COLS_OUT = os.path.join("artifacts", "feature_columns.json")

FEATURE_COLS = [
    "Age", "Sex", "ChestPainType", "RestingBP", "Cholesterol",
    "FastingBS", "RestingECG", "MaxHR", "ExerciseAngina", "Oldpeak", "ST_Slope",
]
TARGET_COL = "HeartDisease"
NUMERIC_COLS = ["Age", "RestingBP", "Cholesterol", "FastingBS", "MaxHR", "Oldpeak"]
CATEGORICAL_COLS = ["Sex", "ChestPainType", "RestingECG", "ExerciseAngina", "ST_Slope"]


def main():
    print(f"[Train] Cargando {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    print(f"[Train] Dataset: {df.shape[0]} filas x {df.shape[1]} columnas")

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"[Train] Train={len(X_train)}, Test={len(X_test)}")

    # OrdinalEncoder asigna enteros a categorías nominales (ej. Sex M=0/F=1).
    # Para modelos lineales esto introduce orden artificial inexistente.
    # RandomForest tolera este encoding porque sus splits no asumen linealidad,
    # pero OneHotEncoder sería más correcto conceptualmente para variables
    # como ChestPainType (ATA/NAP/ASY/TA) sin orden natural.
    # Decisión: OrdinalEncoder elegido por simplicidad de serialización en Spark.
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUMERIC_COLS),
            (
                "cat",
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                CATEGORICAL_COLS,
            ),
        ],
        remainder="drop",
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", RandomForestClassifier(n_estimators=100, random_state=42)),
        ]
    )

    print("[Train] Entrenando RandomForestClassifier...")
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]

    print("\n-- Metricas de evaluacion ---------------------------")
    print(f"  Accuracy : {accuracy_score(y_test, y_pred):.4f}")
    print(f"  Precision: {precision_score(y_test, y_pred):.4f}")
    print(f"  Recall   : {recall_score(y_test, y_pred):.4f}")
    print(f"  F1       : {f1_score(y_test, y_pred):.4f}")
    print(f"  AUC-ROC  : {roc_auc_score(y_test, y_proba):.4f}")
    print("-----------------------------------------------------\n")

    os.makedirs("artifacts", exist_ok=True)
    joblib.dump(pipeline, MODEL_OUT)
    print(f"[Train] Modelo guardado en {MODEL_OUT}")

    with open(COLS_OUT, "w", encoding="utf-8") as f:
        json.dump(FEATURE_COLS, f, indent=2)
    print(f"[Train] Columnas guardadas en {COLS_OUT}")


if __name__ == "__main__":
    main()
