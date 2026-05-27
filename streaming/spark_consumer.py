"""
spark_consumer.py
Consume eventos de Kafka y los escribe en PostgreSQL (schema bronze)
Ejecutar: python streaming/spark_consumer.py
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# ── Configuración ─────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
POSTGRES_URL   = "jdbc:postgresql://localhost:5433/olist_db"
POSTGRES_PROPS = {
    "user":     "olist_user",
    "password": "olist_pass",
    "driver":   "org.postgresql.Driver"
}
CHECKPOINT_DIR = "/tmp/olist_checkpoints"

# ── Schemas de los eventos ────────────────────────────────────────────────────
schema_orders = StructType([
    StructField("order_id",                      StringType(),  True),
    StructField("customer_id",                   StringType(),  True),
    StructField("order_status",                  StringType(),  True),
    StructField("order_purchase_timestamp",      StringType(),  True),
    StructField("order_approved_at",             StringType(),  True),
    StructField("order_delivered_carrier_date",  StringType(),  True),
    StructField("order_delivered_customer_date", StringType(),  True),
    StructField("order_estimated_delivery_date", StringType(),  True),
    StructField("ingestion_timestamp",           StringType(),  True),
])

schema_payments = StructType([
    StructField("order_id",              StringType(),  True),
    StructField("payment_sequential",    IntegerType(), True),
    StructField("payment_type",          StringType(),  True),
    StructField("payment_installments",  IntegerType(), True),
    StructField("payment_value",         DoubleType(),  True),
    StructField("ingestion_timestamp",   StringType(),  True),
])

schema_reviews = StructType([
    StructField("review_id",               StringType(),  True),
    StructField("order_id",                StringType(),  True),
    StructField("review_score",            IntegerType(), True),
    StructField("review_comment_title",    StringType(),  True),
    StructField("review_comment_message",  StringType(),  True),
    StructField("review_creation_date",    StringType(),  True),
    StructField("review_answer_timestamp", StringType(),  True),
    StructField("ingestion_timestamp",     StringType(),  True),
])

# ── Spark Session ─────────────────────────────────────────────────────────────
print("Iniciando Spark Session...")
spark = SparkSession.builder \
    .appName("OlistStreamingConsumer") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✔ Spark Session iniciada.")

# ── Función de escritura a PostgreSQL ─────────────────────────────────────────
def write_to_postgres(df, epoch_id, table_name):
    count = df.count()
    if count > 0:
        df.write.jdbc(
            url=POSTGRES_URL,
            table=table_name,
            mode="append",
            properties=POSTGRES_PROPS
        )
        print(f"  ✔ [{table_name}] {count:,} filas escritas (batch {epoch_id})")

# ── Leer de Kafka ─────────────────────────────────────────────────────────────
def read_topic(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

# ── Stream 1: Orders ──────────────────────────────────────────────────────────
print("Iniciando stream: olist.orders → bronze.orders...")
df_orders_raw = read_topic("olist.orders")
df_orders = df_orders_raw \
    .select(from_json(col("value").cast("string"), schema_orders).alias("data")) \
    .select("data.*") \
    .withColumn("bronze_timestamp", current_timestamp())

query_orders = df_orders.writeStream \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "bronze.orders")) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/orders") \
    .trigger(processingTime="10 seconds") \
    .start()

# ── Stream 2: Payments ────────────────────────────────────────────────────────
print("Iniciando stream: olist.payments → bronze.payments...")
df_payments_raw = read_topic("olist.payments")
df_payments = df_payments_raw \
    .select(from_json(col("value").cast("string"), schema_payments).alias("data")) \
    .select("data.*") \
    .withColumn("bronze_timestamp", current_timestamp())

query_payments = df_payments.writeStream \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "bronze.payments")) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/payments") \
    .trigger(processingTime="10 seconds") \
    .start()

# ── Stream 3: Reviews ─────────────────────────────────────────────────────────
print("Iniciando stream: olist.reviews → bronze.reviews...")
df_reviews_raw = read_topic("olist.reviews")
df_reviews = df_reviews_raw \
    .select(from_json(col("value").cast("string"), schema_reviews).alias("data")) \
    .select("data.*") \
    .withColumn("bronze_timestamp", current_timestamp())

query_reviews = df_reviews.writeStream \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "bronze.reviews")) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/reviews") \
    .trigger(processingTime="10 seconds") \
    .start()

# ── Esperar y monitorear ──────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("Streams activos — procesando eventos de Kafka...")
print("Presiona Ctrl+C para detener.")
print("=" * 55)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nDeteniendo streams...")
    query_orders.stop()
    query_payments.stop()
    query_reviews.stop()
    spark.stop()
    print("✔ Streams detenidos.")