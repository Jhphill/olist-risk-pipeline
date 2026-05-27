"""
kafka_producer.py
Lee los CSVs de Olist y publica eventos en Kafka respetando
el orden cronológico original de las transacciones.

Ejecutar: python streaming/kafka_producer.py
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# ── Configuración ─────────────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC_ORDERS  = "olist.orders"
TOPIC_PAYMENTS = "olist.payments"
TOPIC_REVIEWS  = "olist.reviews"
DATA_FOLDER   = "data"

# Solo 10% de los registros para no saturar RAM (restricción Mes 2)
SAMPLE_FRACTION = 0.10

# ── Serializer ────────────────────────────────────────────────────────────────
def json_serializer(data):
    return json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")

# ── Productor ─────────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=json_serializer,
    acks="all",               # Confirmación de escritura
    retries=3,
    batch_size=16384,
    linger_ms=10,             # Agrupa mensajes por 10ms para eficiencia
)

def publicar_orders():
    print("Cargando olist_orders_dataset.csv...")
    df = pd.read_csv(
        f"{DATA_FOLDER}/olist_orders_dataset.csv",
        parse_dates=["order_purchase_timestamp"]
    )
    # Muestra del 10% ordenada cronológicamente
    df = df.sample(frac=SAMPLE_FRACTION, random_state=42)
    df = df.sort_values("order_purchase_timestamp")
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} pedidos en topic '{TOPIC_ORDERS}'...")
    for _, row in df.iterrows():
        producer.send(TOPIC_ORDERS, value=row.to_dict())

    producer.flush()
    print(f"  ✔ {len(df):,} pedidos publicados.")
    return df["order_id"].tolist()

def publicar_payments(order_ids):
    print("Cargando olist_order_payments_dataset.csv...")
    df = pd.read_csv(f"{DATA_FOLDER}/olist_order_payments_dataset.csv")
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} pagos en topic '{TOPIC_PAYMENTS}'...")
    for _, row in df.iterrows():
        producer.send(TOPIC_PAYMENTS, value=row.to_dict())

    producer.flush()
    print(f"  ✔ {len(df):,} pagos publicados.")

def publicar_reviews(order_ids):
    print("Cargando olist_order_reviews_dataset.csv...")
    df = pd.read_csv(f"{DATA_FOLDER}/olist_order_reviews_dataset.csv")
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} reseñas en topic '{TOPIC_REVIEWS}'...")
    for _, row in df.iterrows():
        producer.send(TOPIC_REVIEWS, value=row.to_dict())

    producer.flush()
    print(f"  ✔ {len(df):,} reseñas publicadas.")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("PRODUCTOR KAFKA — Pipeline Olist Streaming")
    print("=" * 55)
    print(f"Broker: {KAFKA_BROKER}")
    print(f"Muestra: {int(SAMPLE_FRACTION*100)}% de los datos")
    print("=" * 55)

    try:
        order_ids = publicar_orders()
        publicar_payments(order_ids)
        publicar_reviews(order_ids)

        print("\n" + "=" * 55)
        print("✔ Todos los eventos publicados exitosamente.")
        print("=" * 55)

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        producer.close()