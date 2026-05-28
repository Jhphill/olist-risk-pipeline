"""
olist_pipeline_dag.py
DAG principal del pipeline Olist — Mes 2
10 tareas: Producer → Sensor → Spark → dbt Silver → GE → dbt Gold → Stop → Notify
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import logging
import subprocess
import sys
import os

# ── Callback de fallo ─────────────────────────────────────────────────────────
def on_failure_callback(context):
    logging.error(
        f"[FALLO] Tarea: {context['task_instance'].task_id} | "
        f"DAG: {context['dag'].dag_id} | "
        f"Fecha: {context['execution_date']}"
    )

default_args = {
    "owner":               "olist_team",
    "retries":             2,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="olist_pipeline_mes2",
    description="Pipeline Olist Mes 2: Kafka → Spark Bronze → dbt Silver/Gold",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["olist", "streaming", "produccion"],
) as dag:

    # ── Tarea 1: Kafka Producer ───────────────────────────────────────────────
    def kafka_producer():
        env = os.environ.copy()
        env["JAVA_HOME"]         = r"C:\olist-risk-pipeline\java17\jdk-17.0.11+9"
        env["HADOOP_HOME"]       = r"C:\olist-risk-pipeline"
        env["PATH"]              = env["JAVA_HOME"] + r"\bin;" + env["PATH"]
        env["JAVA_TOOL_OPTIONS"] = ""

        result = subprocess.run(
            [sys.executable, r"C:\olist-risk-pipeline\streaming\kafka_producer.py"],
            capture_output=True, text=True, env=env
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t1_kafka_producer = PythonOperator(
        task_id="01_kafka_producer",
        python_callable=kafka_producer,
    )

    # ── Tarea 2: Sensor — espera ≥100 mensajes en Kafka ──────────────────────
    class KafkaReadySensor(BaseSensorOperator):
        def poke(self, context):
            try:
                from kafka import KafkaConsumer
                from kafka.structs import TopicPartition

                consumer = KafkaConsumer(
                    bootstrap_servers="localhost:9092",
                    group_id=None,
                )
                topics = ["olist.orders", "olist.payments",
                          "olist.reviews", "olist.items"]
                total = 0
                for topic in topics:
                    partitions = consumer.partitions_for_topic(topic) or set()
                    for p in partitions:
                        tp = TopicPartition(topic, p)
                        end = consumer.end_offsets([tp]).get(tp, 0)
                        total += end

                consumer.close()
                logging.info(f"Mensajes en Kafka: {total:,}")
                return total >= 100

            except Exception as e:
                logging.warning(f"Sensor error: {e}")
                return False

    t2_kafka_sensor = KafkaReadySensor(
        task_id="02_wait_kafka_ready",
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    # ── Tarea 3: Spark Consumer ───────────────────────────────────────────────
    def spark_consumer():
        env = os.environ.copy()
        env["JAVA_HOME"]         = r"C:\olist-risk-pipeline\java17\jdk-17.0.11+9"
        env["HADOOP_HOME"]       = r"C:\olist-risk-pipeline"
        env["PATH"]              = env["JAVA_HOME"] + r"\bin;" + env["PATH"] + r";C:\olist-risk-pipeline\bin"
        env["JAVA_TOOL_OPTIONS"] = ""

        result = subprocess.run(
            [sys.executable, r"C:\olist-risk-pipeline\streaming\spark_consumer.py"],
            capture_output=True, text=True, env=env,
            timeout=300
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t3_spark_consumer = PythonOperator(
        task_id="03_spark_bronze",
        python_callable=spark_consumer,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Tarea 4: dbt run silver ───────────────────────────────────────────────
    t4_dbt_silver = BashOperator(
        task_id="04_dbt_run_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 5: dbt test silver ──────────────────────────────────────────────
    t5_dbt_test_silver = BashOperator(
        task_id="05_dbt_test_silver",
        bash_command="cd /opt/airflow/dbt && dbt test --select silver --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 6: Great Expectations ───────────────────────────────────────────
    def ge_validate():
        result = subprocess.run(
            [sys.executable, r"C:\olist-risk-pipeline\great_expectations\validate_silver.py"],
            capture_output=True, text=True
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t6_ge_validate = PythonOperator(
        task_id="06_ge_validate",
        python_callable=ge_validate,
    )

    # ── Tarea 7: dbt run gold ─────────────────────────────────────────────────
    t7_dbt_gold = BashOperator(
        task_id="07_dbt_run_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --select gold --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 8: dbt test gold ────────────────────────────────────────────────
    t8_dbt_test_gold = BashOperator(
        task_id="08_dbt_test_gold",
        bash_command="cd /opt/airflow/dbt && dbt test --select gold --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 9: Stop Kafka Producer (limpieza) ───────────────────────────────
    def stop_kafka_producer():
        """
        Verifica que el productor terminó correctamente
        y que todos los topics tienen mensajes.
        """
        try:
            from kafka import KafkaConsumer
            from kafka.structs import TopicPartition

            consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
            topics = ["olist.orders", "olist.payments",
                      "olist.reviews", "olist.items"]

            resumen = {}
            for topic in topics:
                partitions = consumer.partitions_for_topic(topic) or set()
                total = 0
                for p in partitions:
                    tp = TopicPartition(topic, p)
                    total += consumer.end_offsets([tp]).get(tp, 0)
                resumen[topic] = total

            consumer.close()

            for topic, count in resumen.items():
                logging.info(f"  {topic}: {count:,} mensajes")

            total_msgs = sum(resumen.values())
            logging.info(f"Total mensajes en Kafka: {total_msgs:,}")

            if total_msgs == 0:
                raise Exception("Kafka no tiene mensajes — el productor falló.")

        except Exception as e:
            raise Exception(f"Error verificando Kafka: {e}")

    t9_stop_producer = PythonOperator(
        task_id="09_stop_kafka_producer",
        python_callable=stop_kafka_producer,
    )

    # ── Tarea 10: Notificación de éxito ───────────────────────────────────────
    def notify_success(**context):
        from sqlalchemy import create_engine, text
        engine = create_engine(
            "postgresql://olist_user:olist_pass@postgres:5432/olist_db"
        )
        with engine.connect() as conn:
            bronze = conn.execute(
                text("SELECT COUNT(*) FROM bronze.orders")
            ).fetchone()
            gold = conn.execute(
                text("SELECT COUNT(*) FROM gold.fct_orders")
            ).fetchone()
            riesgo = conn.execute(
                text("SELECT COUNT(*) FROM gold.fct_orders WHERE flag_riesgo = 1")
            ).fetchone()

        logging.info(
            f"[ÉXITO] Pipeline Mes 2 completado | "
            f"Fecha: {context['execution_date']} | "
            f"Bronze orders: {bronze[0]:,} | "
            f"Gold fct_orders: {gold[0]:,} | "
            f"Pedidos en riesgo: {riesgo[0]:,}"
        )
        engine.dispose()

    t10_notify = PythonOperator(
        task_id="10_notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ── Dependencias — orden crítico ──────────────────────────────────────────
    t1_kafka_producer >> t2_kafka_sensor >> t3_spark_consumer >> t4_dbt_silver >> t5_dbt_test_silver >> t6_ge_validate >> t7_dbt_gold >> t8_dbt_test_gold >> t9_stop_producer >> t10_notify