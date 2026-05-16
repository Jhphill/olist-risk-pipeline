"""
olist_pipeline_dag.py
DAG principal del pipeline Olist — ejecuta diariamente
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

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
    "owner":            "olist_team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="olist_pipeline",
    description="Pipeline batch Olist: raw → silver → gold → validación",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["olist", "batch", "produccion"],
) as dag:

    # ── Tarea 1: Carga raw ────────────────────────────────────────────────────
    def carga_raw():
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "/opt/airflow/ingestion/carga_raw.py"],
            capture_output=True, text=True
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t1_carga_raw = PythonOperator(
        task_id="01_carga_raw",
        python_callable=carga_raw,
    )

    # ── Tarea 2: dbt run silver ───────────────────────────────────────────────
    t2_dbt_silver = BashOperator(
        task_id="02_dbt_run_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 3: dbt test silver ──────────────────────────────────────────────
    t3_dbt_test_silver = BashOperator(
        task_id="03_dbt_test_silver",
        bash_command="cd /opt/airflow/dbt && dbt test --select silver --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 4: Great Expectations ───────────────────────────────────────────
    def ge_validate():
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "/opt/airflow/great_expectations/validate_silver.py"],
            capture_output=True, text=True
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    t4_ge_validate = PythonOperator(
        task_id="04_ge_validate",
        python_callable=ge_validate,
    )

    # ── Tarea 5: dbt run gold ─────────────────────────────────────────────────
    t5_dbt_gold = BashOperator(
        task_id="05_dbt_run_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --select gold --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 6: dbt test gold ────────────────────────────────────────────────
    t6_dbt_test_gold = BashOperator(
        task_id="06_dbt_test_gold",
        bash_command="cd /opt/airflow/dbt && dbt test --select gold --profiles-dir /opt/airflow/dbt",
    )

    # ── Tarea 7: Notificación de éxito ───────────────────────────────────────
    def notify_success(**context):
        from sqlalchemy import create_engine, text
        engine = create_engine(
            "postgresql://olist_user:olist_pass@postgres:5432/olist_db"
        )
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM gold.fct_orders")
            ).fetchone()
        logging.info(
            f"[ÉXITO] Pipeline completado | "
            f"Fecha: {context['execution_date']} | "
            f"Filas en fct_orders: {result[0]:,}"
        )
        engine.dispose()

    t7_notify = PythonOperator(
        task_id="07_notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ── Dependencias (orden crítico) ──────────────────────────────────────────
    t1_carga_raw >> t2_dbt_silver >> t3_dbt_test_silver >> t4_ge_validate >> t5_dbt_gold >> t6_dbt_test_gold >> t7_notify