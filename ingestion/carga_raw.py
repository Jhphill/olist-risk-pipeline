"""
carga_raw.py — Pipeline Olist, UNIVALLE 2026
Carga los 9 CSVs de Olist en PostgreSQL (schema public, tablas raw_*)

Ejecutar desde la raíz del proyecto:
    python ingestion/carga_raw.py

Requisitos:
    pip install pandas sqlalchemy psycopg2-binary
"""

import os
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
DB_USER     = "olist_user"
DB_PASSWORD = "olist_pass"
DB_HOST     = "localhost"
DB_PORT     = "5433"
DB_NAME     = "olist_db"

DATA_FOLDER = "data"
CHUNK_SIZE  = 10_000   # Filas por lote (protege RAM en geolocation ~1M filas)

# ─────────────────────────────────────────────
# LOGGING — consola + archivo
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingestion/carga_raw.log", mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# ESQUEMA EXPLÍCITO DE TIPOS POR TABLA
# Evita que PostgreSQL infiera fechas como varchar
# ─────────────────────────────────────────────
from sqlalchemy import (
    BigInteger, Float, Integer, Text, DateTime, Numeric
)

DTYPES: dict[str, dict] = {
    "orders": {
        "order_purchase_timestamp":    DateTime,
        "order_approved_at":           DateTime,
        "order_delivered_carrier_date": DateTime,
        "order_delivered_customer_date": DateTime,
        "order_estimated_delivery_date": DateTime,
    },
    "order_reviews": {
        "review_creation_date":  DateTime,
        "review_answer_timestamp": DateTime,
    },
    "order_payments": {
        "payment_value": Numeric(12, 2),
    },
    "order_items": {
        "price":         Numeric(12, 2),
        "freight_value": Numeric(12, 2),
        "shipping_limit_date": DateTime,
    },
}

# ─────────────────────────────────────────────
# ARCHIVOS A CARGAR
# ─────────────────────────────────────────────
CSV_FILES: dict[str, str] = {
    "customers":                  "olist_customers_dataset.csv",
    "geolocation":                "olist_geolocation_dataset.csv",
    "order_items":                "olist_order_items_dataset.csv",
    "order_payments":             "olist_order_payments_dataset.csv",
    "order_reviews":              "olist_order_reviews_dataset.csv",
    "orders":                     "olist_orders_dataset.csv",
    "products":                   "olist_products_dataset.csv",
    "sellers":                    "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────
def cargar_csv_a_postgres(engine) -> None:
    ingestion_ts = datetime.now()
    resumen = []

    for table_key, filename in CSV_FILES.items():
        file_path = os.path.join(DATA_FOLDER, filename)
        db_table  = f"raw_{table_key}"

        if not os.path.exists(file_path):
            log.error("Archivo no encontrado: %s", file_path)
            resumen.append((db_table, "ERROR — archivo no encontrado", 0))
            continue

        log.info("▶ Cargando %s → %s", filename, db_table)
        total_filas = 0
        primer_chunk = True

        try:
            # Parseo de fechas automático solo para tablas con columnas DateTime
            parse_dates = (
                list(DTYPES[table_key].keys())
                if table_key in DTYPES else False
            )

            for chunk in pd.read_csv(
                file_path,
                encoding="utf-8",
                low_memory=False,
                chunksize=CHUNK_SIZE,
                parse_dates=parse_dates if parse_dates else None,
            ):
                # Agregar timestamp de ingesta
                chunk["ingestion_timestamp"] = ingestion_ts

                # Primer chunk: crear tabla y definir esquema
                # Chunks siguientes: solo append
                chunk.to_sql(
                    name=db_table,
                    con=engine,
                    if_exists="replace" if primer_chunk else "append",
                    index=False,
                    dtype=DTYPES.get(table_key),   # None si no está en el dict
                    method="multi",    # Insert multi-row: más rápido que fila a fila
                )
                total_filas += len(chunk)
                primer_chunk = False

            log.info("  ✔ %s — %d filas cargadas", db_table, total_filas)
            resumen.append((db_table, "OK", total_filas))

        except Exception as exc:
            log.exception("  ✘ Error al cargar %s: %s", db_table, exc)
            resumen.append((db_table, f"ERROR — {exc}", total_filas))

    # ── Resumen final ──────────────────────────────────────────────────────
    log.info("")
    log.info("═" * 55)
    log.info("RESUMEN DE CARGA")
    log.info("═" * 55)
    for tabla, estado, filas in resumen:
        log.info("  %-38s  %7d filas  %s", tabla, filas, estado)
    log.info("═" * 55)


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Iniciando carga de datos a PostgreSQL…")
    log.info("Conexión: %s@%s:%s/%s", DB_USER, DB_HOST, DB_PORT, DB_NAME)

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True,   # Detecta conexiones caídas antes de usarlas
    )

    try:
        # Verificar conexión antes de empezar
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Conexión a PostgreSQL exitosa.")

        cargar_csv_a_postgres(engine)

    except Exception as exc:
        log.exception("No se pudo conectar a PostgreSQL: %s", exc)
    finally:
        engine.dispose()
        log.info("Conexión cerrada. Proceso terminado.")