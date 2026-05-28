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
KAFKA_BROKER   = "localhost:9092"
TOPIC_ORDERS   = "olist.orders"
TOPIC_PAYMENTS = "olist.payments"
TOPIC_REVIEWS  = "olist.reviews"
TOPIC_ITEMS    = "olist.items"
DATA_FOLDER    = "data"

SAMPLE_FRACTION = 0.10
SLEEP_BETWEEN_MSGS = 0.005  # 5ms entre mensajes

# ── Serializer ────────────────────────────────────────────────────────────────
def json_serializer(data):
    return json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")

# ── Productor ─────────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=json_serializer,
    acks="all",
    retries=3,
    batch_size=16384,
    linger_ms=10,
)

def publicar_orders():
    print("Cargando olist_orders_dataset.csv...")
    df = pd.read_csv(
        f"{DATA_FOLDER}/olist_orders_dataset.csv",
        parse_dates=["order_purchase_timestamp"]
    )
    df = df.sample(frac=SAMPLE_FRACTION, random_state=42)
    df = df.sort_values("order_purchase_timestamp")
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} pedidos en topic '{TOPIC_ORDERS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_ORDERS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,} pedidos publicados...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {len(df):,} pedidos publicados.")
    return df["order_id"].tolist()

def publicar_items(order_ids):
    print("Cargando olist_order_items_dataset.csv...")
    df = pd.read_csv(
        f"{DATA_FOLDER}/olist_order_items_dataset.csv",
        parse_dates=["shipping_limit_date"]
    )
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} items en topic '{TOPIC_ITEMS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_ITEMS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,} items publicados...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {len(df):,} items publicados.")

def publicar_payments(order_ids):
    print("Cargando olist_order_payments_dataset.csv...")
    df = pd.read_csv(f"{DATA_FOLDER}/olist_order_payments_dataset.csv")
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} pagos en topic '{TOPIC_PAYMENTS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_PAYMENTS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,} pagos publicados...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {len(df):,} pagos publicados.")

def publicar_reviews(order_ids):
    print("Cargando olist_order_reviews_dataset.csv...")
    df = pd.read_csv(f"{DATA_FOLDER}/olist_order_reviews_dataset.csv")
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    print(f"  Publicando {len(df):,} reseñas en topic '{TOPIC_REVIEWS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_REVIEWS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,} reseñas publicadas...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {len(df):,} reseñas publicadas.")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("PRODUCTOR KAFKA — Pipeline Olist Streaming")
    print("=" * 55)
    print(f"Broker:  {KAFKA_BROKER}")
    print(f"Muestra: {int(SAMPLE_FRACTION*100)}% de los datos")
    print(f"Topics:  {TOPIC_ORDERS}, {TOPIC_PAYMENTS}, {TOPIC_REVIEWS}, {TOPIC_ITEMS}")
    print("=" * 55)

    try:
        order_ids = publicar_orders()
        publicar_items(order_ids)
        publicar_payments(order_ids)
        publicar_reviews(order_ids)

        print("\n" + "=" * 55)
        print("✔ Todos los eventos publicados exitosamente.")
        print("=" * 55)

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        producer.close()