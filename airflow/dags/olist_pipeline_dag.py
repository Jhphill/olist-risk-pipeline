"""
olist_pipeline_dag.py
DAG principal del pipeline Olist — Mes 2
Flujo: Kafka Producer → Spark Consumer → dbt Silver → GE → dbt Gold → notify
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
        f"Fecha: {context['execution_date']} | "
        f"Error: {context.get('exception', 'desconocido')}"
    )

# ── Argumentos por defecto ────────────────────────────────────────────────────
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
        env["JAVA_HOME"]    = r"C:\olist-risk-pipeline\java17\jdk-17.0.11+9"
        env["HADOOP_HOME"]  = r"C:\olist-risk-pipeline"
        env["PATH"]         = env["JAVA_HOME"] + r"\bin;" + env["PATH"]
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

    # ── Tarea 2: Spark Consumer ───────────────────────────────────────────────
    def spark_consumer():
        env = os.environ.copy()
        env["JAVA_HOME"]    = r"C:\olist-risk-pipeline\java17\jdk-17.0.11+9"
        env["HADOOP_HOME"]  = r"C:\olist-risk-pipeline"
        env["PATH"]         = env["JAVA_HOME"] + r"\bin;" + env["PATH"] + r";C:\olist-risk-pipeline\bin"
        env["JAVA_TOOL_OPTIONS"] = ""

        result = subprocess.run(
            [sys.executable, r"C:\olist-risk-pipeline\streaming\spark_consumer.py"],
            capture_output=True, text=True, env=env,
            timeout=300  # 5 minutos máximo
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t2_spark_consumer = PythonOperator(
        task_id="02_spark_consumer",
        python_callable=spark_consumer,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Tarea 3: dbt run silver ───────────────────────────────────────────────
    t3_dbt_silver = BashOperator(
        task_id="03_dbt_run_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 4: dbt test silver ──────────────────────────────────────────────
    t4_dbt_test_silver = BashOperator(
        task_id="04_dbt_test_silver",
        bash_command="cd /opt/airflow/dbt && dbt test --select silver --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 5: Great Expectations ───────────────────────────────────────────
    def ge_validate():
        result = subprocess.run(
            [sys.executable, r"C:\olist-risk-pipeline\great_expectations\validate_silver.py"],
            capture_output=True, text=True
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t5_ge_validate = PythonOperator(
        task_id="05_ge_validate",
        python_callable=ge_validate,
    )

    # ── Tarea 6: dbt run gold ─────────────────────────────────────────────────
    t6_dbt_gold = BashOperator(
        task_id="06_dbt_run_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --select gold --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 7: dbt test gold ────────────────────────────────────────────────
    t7_dbt_test_gold = BashOperator(
        task_id="07_dbt_test_gold",
        bash_command="cd /opt/airflow/dbt && dbt test --select gold --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 8: Notificación de éxito ────────────────────────────────────────
    def notify_success(**context):
        from sqlalchemy import create_engine, text
        engine = create_engine(
            "postgresql://olist_user:olist_pass@postgres:5432/olist_db"
        )
        with engine.connect() as conn:
            bronze = conn.execute(text("SELECT COUNT(*) FROM bronze.orders")).fetchone()
            gold   = conn.execute(text("SELECT COUNT(*) FROM gold.fct_orders")).fetchone()
        logging.info(
            f"[ÉXITO] Pipeline Mes 2 completado | "
            f"Fecha: {context['execution_date']} | "
            f"Bronze orders: {bronze[0]:,} | "
            f"Gold fct_orders: {gold[0]:,}"
        )
        engine.dispose()

    t8_notify = PythonOperator(
        task_id="08_notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ── Dependencias ──────────────────────────────────────────────────────────
    t1_kafka_producer >> t2_spark_consumer >> t3_dbt_silver >> t4_dbt_test_silver >> t5_ge_validate >> t6_dbt_gold >> t7_dbt_test_gold >> t8_notify