"""
kafka_producer.py
Lee los CSVs de Olist y publica eventos en Kafka respetando
el orden cronológico original de las transacciones.

Ejecutar: python streaming/kafka_producer.py
"""

import json
import time
import os
import pandas as pd
from datetime import datetime

# ── Configuración ─────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC_ORDERS   = "olist.orders"
TOPIC_PAYMENTS = "olist.payments"
TOPIC_REVIEWS  = "olist.reviews"
TOPIC_ITEMS    = "olist.items"

# Ruta absoluta a /data — funciona sin importar desde dónde se ejecuta el script
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

SAMPLE_FRACTION    = 0.90   # 90% → ~90,500 pedidos
SLEEP_BETWEEN_MSGS = 0.005  # 5ms entre mensajes

# ── Serializer ────────────────────────────────────────────────────────────────
def json_serializer(data):
    return json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")

# ── Funciones de publicación ──────────────────────────────────────────────────
def publicar_orders(producer):
    print("Cargando olist_orders_dataset.csv...")
    df = pd.read_csv(
        os.path.join(DATA_FOLDER, "olist_orders_dataset.csv"),
        parse_dates=["order_purchase_timestamp"]
    )
    df = df.sample(frac=SAMPLE_FRACTION, random_state=42)
    df = df.sort_values("order_purchase_timestamp")
    df["ingestion_timestamp"] = datetime.now().isoformat()

    n = len(df)
    print(f"  Publicando {n:,} pedidos ({int(SAMPLE_FRACTION*100)}% de muestra) en topic '{TOPIC_ORDERS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_ORDERS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,}/{n:,} pedidos publicados...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {n:,} pedidos publicados.")
    return df["order_id"].tolist()


def publicar_items(producer, order_ids):
    print("Cargando olist_order_items_dataset.csv...")
    df = pd.read_csv(
        os.path.join(DATA_FOLDER, "olist_order_items_dataset.csv"),
        parse_dates=["shipping_limit_date"]
    )
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    n = len(df)
    print(f"  Publicando {n:,} items en topic '{TOPIC_ITEMS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_ITEMS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,}/{n:,} items publicados...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {n:,} items publicados.")


def publicar_payments(producer, order_ids):
    print("Cargando olist_order_payments_dataset.csv...")
    df = pd.read_csv(os.path.join(DATA_FOLDER, "olist_order_payments_dataset.csv"))
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    n = len(df)
    print(f"  Publicando {n:,} pagos en topic '{TOPIC_PAYMENTS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_PAYMENTS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,}/{n:,} pagos publicados...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {n:,} pagos publicados.")


def publicar_reviews(producer, order_ids):
    print("Cargando olist_order_reviews_dataset.csv...")
    df = pd.read_csv(os.path.join(DATA_FOLDER, "olist_order_reviews_dataset.csv"))
    df = df[df["order_id"].isin(order_ids)]
    df["ingestion_timestamp"] = datetime.now().isoformat()

    n = len(df)
    print(f"  Publicando {n:,} reseñas en topic '{TOPIC_REVIEWS}'...")
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(TOPIC_REVIEWS, value=row.to_dict())
        if (i + 1) % 1000 == 0:
            print(f"    → {i+1:,}/{n:,} reseñas publicadas...")
        time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"  ✔ {n:,} reseñas publicadas.")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from kafka import KafkaProducer  # import aquí para evitar error si Kafka no está corriendo

    print("=" * 57)
    print("PRODUCTOR KAFKA — Pipeline Olist Streaming")
    print("=" * 57)
    print(f"Broker:    {KAFKA_BROKER}")
    print(f"Muestra:   {int(SAMPLE_FRACTION*100)}% de los datos (~{int(99441*SAMPLE_FRACTION):,} pedidos)")
    print(f"Data dir:  {os.path.abspath(DATA_FOLDER)}")
    print(f"Topics:    {TOPIC_ORDERS}, {TOPIC_PAYMENTS}, {TOPIC_REVIEWS}, {TOPIC_ITEMS}")
    print("=" * 57)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=json_serializer,
        acks="all",
        retries=3,
        batch_size=16384,
        linger_ms=10,
    )

    try:
        order_ids = publicar_orders(producer)
        publicar_items(producer, order_ids)
        publicar_payments(producer, order_ids)
        publicar_reviews(producer, order_ids)

        print("\n" + "=" * 57)
        print("✔ Todos los eventos publicados exitosamente.")
        print("=" * 57)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        raise
    finally:
        producer.close()