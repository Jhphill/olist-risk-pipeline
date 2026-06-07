# Pipeline de Datos en Tiempo Real para Análisis de Riesgo de Impago — Olist

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black?logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange?logo=apachespark)
![dbt](https://img.shields.io/badge/dbt-1.8.0-red?logo=dbt)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-teal?logo=apacheairflow)
![Flask](https://img.shields.io/badge/Flask-3.1-black?logo=flask)

**Universidad Privada del Valle (UNIVALLE) · Ingeniería de Datos · 2026**

Juan Felipe Caballero Flores · Luciana Sofía Coca Terrazas

Docente: M.Sc. Ing. Oscar Contreras Carrasco

</div>

---

## Tabla de Contenidos

1. [Descripción del Proyecto](#1-descripción-del-proyecto)
2. [Arquitectura](#2-arquitectura)
3. [Stack Tecnológico](#3-stack-tecnológico)
4. [Estructura del Repositorio](#4-estructura-del-repositorio)
5. [Instalación y Configuración](#5-instalación-y-configuración)
6. [Ejecución del Pipeline](#6-ejecución-del-pipeline)
7. [Dashboard en Tiempo Real](#7-dashboard-en-tiempo-real)
8. [Modelo de Datos](#8-modelo-de-datos)
9. [Lecciones Aprendidas](#9-lecciones-aprendidas)

---

## 1. Descripción del Proyecto

Este proyecto implementa un pipeline de datos completo para el análisis de riesgo de impago en e-commerce, utilizando el dataset público de Olist (Brazilian E-Commerce). El sistema detecta en tiempo real patrones de clientes con alta probabilidad de cancelar pedidos, no pagar o dejar reseñas negativas.

### Objetivo de Negocio

Proveer al equipo de análisis de riesgo de Olist un dashboard actualizado en tiempo real que permita identificar:

- Pedidos con alto riesgo de impago o cancelación
- Vendedores con tasas de riesgo elevadas
- Patrones geográficos y temporales de riesgo
- Correlación entre método de pago, cuotas y satisfacción del cliente

### Dataset

**Brazilian E-Commerce Public Dataset by Olist** — [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Archivo | Filas | Descripción |
|---------|-------|-------------|
| olist_orders_dataset.csv | 99,441 | Pedidos principales |
| olist_customers_dataset.csv | 99,441 | Clientes únicos |
| olist_order_items_dataset.csv | 112,650 | Items por pedido |
| olist_order_payments_dataset.csv | 103,886 | Pagos por pedido |
| olist_order_reviews_dataset.csv | 99,224 | Reseñas de clientes |
| olist_products_dataset.csv | 32,951 | Catálogo de productos |
| olist_sellers_dataset.csv | 3,095 | Vendedores |
| olist_geolocation_dataset.csv | 1,000,163 | Coordenadas por ZIP |

---

## 2. Arquitectura

```
CSVs Olist (data/)
      │
      ├─── Mes 1: Batch Pipeline ──────────────────────────────────────────┐
      │    carga_raw.py (SQLAlchemy)                                        │
      │    └── PostgreSQL: schema public (raw_*)                            │
      │                                                                     │
      └─── Mes 2: Streaming Pipeline ──────────────────────────────────────┤
           kafka_producer.py (10% muestra)                                  │
           └── Apache Kafka (4 topics)                                      │
               └── spark_consumer.py (Spark Structured Streaming)           │
                   └── PostgreSQL: schema bronze (orders/payments/reviews)  │
                                                                            │
dbt run (cada 30s) ─────────────────────────────────────────────────────────┤
  bronze.* / public.raw_* → silver.stg_* → gold.fct_orders + dims          │
                                                                            │
Apache Airflow DAG (orquestación) ──────────────────────────────────────────┤
                                                                            │
Flask Dashboard (localhost:5001) ←── gold.* ────────────────────────────────┘
  Auto-refresh cada 30 segundos
```

### Flag de Riesgo

```sql
flag_riesgo = 1 CUANDO:
  order_status IN ('canceled', 'unavailable')
  OR review_score <= 2
  OR dias_atraso > 30
  OR flag_pago_cero = 1
```

---

## 3. Stack Tecnológico

| Herramienta | Versión | Rol |
|-------------|---------|-----|
| Python | 3.11 | Scripts de ingesta y producción |
| Apache Kafka | 3.x | Broker de mensajería streaming |
| Spark Structured Streaming | 3.5.1 | Consumidor Kafka → Bronze |
| dbt-postgres | 1.8.0 | Transformaciones Silver → Gold |
| PostgreSQL | 15 | Base de datos (raw/bronze/silver/gold) |
| Apache Airflow | 2.8.1 | Orquestación del pipeline |
| Flask | 3.1 | Dashboard web en tiempo real |
| Docker + Compose | Latest | Contenedores de servicios |
| Great Expectations | Latest | Validación de calidad de datos |
| Java 17 (Temurin) | 17.0.11 | Runtime para Spark en Windows |

---

## 4. Estructura del Repositorio

```
olist-risk-pipeline/
├── data/                          # CSVs del dataset Olist (no versionados)
├── streaming/
│   ├── kafka_producer.py          # Publica eventos a Kafka (10% muestra)
│   └── spark_consumer.py          # Consume Kafka → bronze.* en PostgreSQL
├── dbt/
│   ├── profiles.yml               # Conexión a PostgreSQL
│   └── models/
│       ├── sources.yml            # Fuentes: public.raw_* y bronze.*
│       ├── silver/
│       │   ├── stg_orders.sql     # Lee bronze.orders → limpia NaN/timestamps
│       │   ├── stg_customers.sql  # Lee public.raw_customers
│       │   ├── stg_payments.sql   # Lee bronze.payments → agrega por order_id
│       │   ├── stg_reviews.sql    # Lee bronze.reviews → limpia NaN
│       │   ├── stg_sellers.sql    # Lee public.raw_sellers
│       │   ├── stg_products.sql   # Lee public.raw_products
│       │   └── scd_sellers.sql    # SCD Tipo 2 para vendedores
│       └── gold/
│           ├── fct_orders.sql     # Tabla de hechos principal
│           ├── dim_customer.sql   # Dimensión cliente con región Brasil
│           ├── dim_seller.sql     # Dimensión vendedor con total ventas
│           ├── dim_date.sql       # Dimensión fecha
│           └── dim_payment_method.sql
├── airflow/
│   └── dags/
│       └── olist_pipeline_dag.py  # DAG con 7 tareas, retries=2
├── ingestion/
│   └── carga_raw.py               # Carga batch Mes 1 (9 CSVs → raw_*)
├── dashboard_live/
│   └── app.py                     # Flask: 7 endpoints + HTML embebido
├── docs/
│   ├── architecture.png
│   └── screenshots/
├── dashboard.html                 # Dashboard estático Mes 1 (referencia)
├── run_streaming.ps1              # Script PowerShell para levantar el pipeline
├── java17/                        # JDK 17 Temurin para Spark en Windows
├── bin/                           # winutils.exe + hadoop.dll para Hadoop
├── .gitignore
└── README.md
```

---

## 5. Instalación y Configuración

### Prerrequisitos

- Windows 10/11 con Docker Desktop (backend WSL2)
- Python 3.11
- Java 17 (Temurin) en `java17/jdk-17.0.11+9/`
- winutils.exe en `bin/` ([descargar aquí](https://github.com/cdarlint/winutils))

### Setup inicial

```powershell
# 1. Clonar el repositorio
git clone https://github.com/Jhphill/olist-risk-pipeline.git
cd olist-risk-pipeline

# 2. Crear entorno virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Instalar dependencias
pip install kafka-python pandas pyspark==3.5.1 flask flask-cors psycopg2-binary dbt-core==1.8.0 dbt-postgres==1.8.0

# 4. Levantar servicios Docker
docker compose up -d

# 5. Cargar datos batch (Mes 1)
python ingestion/carga_raw.py

# 6. Correr dbt por primera vez
dbt run --project-dir dbt
```

---

## 6. Ejecución del Pipeline

### Terminal 1 — Kafka + Spark Streaming

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
$env:JAVA_HOME = "C:\olist-risk-pipeline\java17\jdk-17.0.11+9"
$env:HADOOP_HOME = "C:\olist-risk-pipeline"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH;C:\olist-risk-pipeline\bin"

docker exec -it olist_postgres psql -U olist_user -d olist_db -c "TRUNCATE bronze.orders; TRUNCATE bronze.payments; TRUNCATE bronze.reviews;"

python streaming/kafka_producer.py   # Publica ~9,944 eventos a Kafka
python streaming/spark_consumer.py   # Escribe a bronze.* en tiempo real
```

### Terminal 2 — Loop dbt (Bronze → Silver → Gold cada 30s)

```powershell
.\.venv\Scripts\Activate.ps1
while ($true) {
    dbt run --project-dir C:\olist-risk-pipeline\dbt
    Start-Sleep -Seconds 30
}
```

### Terminal 3 — Dashboard Flask

```powershell
.\.venv\Scripts\Activate.ps1
python dashboard_live/app.py
# Abrir: http://localhost:5001
```

---

## 7. Dashboard en Tiempo Real

El dashboard Flask consulta directamente `gold.*` en PostgreSQL y se actualiza automáticamente cada 30 segundos.

### KPIs disponibles

| KPI | Descripción |
|-----|-------------|
| Total Pedidos | Filas en gold.fct_orders |
| Pedidos en Riesgo | SUM(flag_riesgo) |
| Tasa de Riesgo % | % del total con flag_riesgo=1 |
| Avg Review Score | Promedio de review_score (1-5) |
| Ticket Promedio | AVG(total_value) en BRL |
| Días Atraso Prom. | Promedio de pedidos atrasados |

### Visualizaciones

| VIZ | Descripción |
|-----|-------------|
| Pipeline Status | Conteo de filas por capa (Bronze/Silver/Gold) en tiempo real |
| VIZ 2 | Evolución mensual de la tasa de riesgo (2016-2018) |
| VIZ 3 | Riesgo por estado de Brasil (top 15) |
| VIZ 4 | Top 10 vendedores con mayor tasa de riesgo |
| VIZ 5 | Método de pago vs tasa de riesgo y avg review |
| VIZ 6 | Cuotas vs satisfacción del cliente |

### APIs disponibles

| Endpoint | Descripción |
|----------|-------------|
| `GET /api/kpis` | KPIs globales |
| `GET /api/riesgo_mensual` | Evolución mensual |
| `GET /api/riesgo_estado` | Riesgo por estado |
| `GET /api/top_vendedores` | Top 10 vendedores |
| `GET /api/pago_vs_riesgo` | Método de pago vs riesgo |
| `GET /api/cuotas_satisfaccion` | Cuotas vs review |
| `GET /api/pipeline_status` | Estado de todas las capas |

---

## 8. Modelo de Datos

### Esquema Estrella (Gold Layer)

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  date_sk PK │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────┐    ┌─────────────────────┐
│ dim_customer │    │  fct_orders │    │ dim_payment_method  │
│ customer_sk  │◄───│  order_id   │───►│ payment_method_sk   │
│ city, state  │    │  flag_riesgo│    │ payment_type        │
│ region       │    │  total_value│    │ es_credito          │
└──────────────┘    │  dias_atraso│    └─────────────────────┘
                    │  review_score    
                    └──────┬──────┘
                           │
                    ┌──────┴──────┐
                    │  dim_seller │
                    │  seller_sk  │
                    │  city, state│
                    └─────────────┘
```

### Capas del Pipeline

| Schema | Tablas | Fuente | Descripción |
|--------|--------|--------|-------------|
| `public` | raw_* (9 tablas) | CSVs Olist | Carga batch Mes 1 |
| `bronze` | orders, payments, reviews | Kafka → Spark | Streaming Mes 2 |
| `silver` | stg_* (7 modelos) | bronze.* / raw_* | Limpieza + surrogate keys |
| `gold` | fct_orders + 4 dims | silver.* | Esquema estrella |

---

## 9. Lecciones Aprendidas

### Técnicas

**Compatibilidad Java/Spark en Windows** — PySpark 4.x usa Scala 2.13 y es incompatible con el conector Kafka `spark-sql-kafka-0-10_2.12`. La solución fue fijar `pyspark==3.5.1` y usar Java 17 Temurin. Java 25 (instalado globalmente) causaba errores de `BlockManagerId` que se resolvieron apuntando `JAVA_HOME` al JDK 17 local.

**Valores NaN de Spark en PostgreSQL** — El serializer JSON de Spark convierte valores nulos de pandas a la cadena `"NaN"` (con comillas dobles literales). Los modelos dbt Silver deben usar `TRIM(BOTH '"' FROM columna)` antes de comparar o castear a timestamp. Materializar `stg_orders` y `stg_reviews` como `table` en lugar de `view` evita que PostgreSQL reordene la evaluación del plan y aplique casts antes del filtro.

**dbt Fusion vs dbt Legacy** — `dbt-core 2.0.0-alpha.1` (dbt Fusion) no soporta el adaptador PostgreSQL. Fue necesario hacer downgrade a `dbt-core==1.8.0` + `dbt-postgres==1.8.0`.

**Hadoop winutils en Windows** — Spark en Windows requiere `winutils.exe` y `hadoop.dll` en el directorio `HADOOP_HOME/bin`. Sin ellos, `SparkContext` falla al inicializar con `FileNotFoundException`.

**ROUND con double precision en PostgreSQL** — PostgreSQL no tiene la función `ROUND(double precision, integer)`. Es necesario castear explícitamente: `ROUND(AVG(columna)::numeric, 2)`. La sintaxis `FILTER` debe ir antes del cast: `AVG(col) FILTER(WHERE ...) ::numeric`.

### De Arquitectura

**Metabase reemplazado por Flask** — Metabase consumía más de 1GB de RAM y lanzaba `OutOfMemoryError` constantemente en un equipo con 8GB. Flask con Chart.js es 100x más liviano, ofrece el mismo resultado visual y permite control total del auto-refresh.

**Muestra del 10% para streaming** — Publicar el dataset completo (99,441 pedidos) a Kafka y procesarlo con Spark en modo local con 8GB RAM es inviable. El 10% (~9,944 pedidos) es suficiente para demostrar el pipeline end-to-end y está documentado como restricción de hardware de desarrollo.

**dbt como puente batch/streaming** — Usar los mismos modelos dbt para ambas capas (batch `raw_*` y streaming `bronze.*`) simplificó enormemente el código. Solo fue necesario cambiar la fuente en `sources.yml` y los 3 modelos Silver que leen datos transaccionales.

### De Proceso

**Checkpoints de Spark** — Al reiniciar el consumidor Spark sin limpiar `/tmp/olist_checkpoints`, Spark cree que ya procesó todos los mensajes y no escribe nada nuevo a bronze. Siempre truncar bronze y limpiar checkpoints antes de una nueva corrida de demostración.

**Orden de arranque importa** — El pipeline debe iniciarse en el orden correcto: Docker → Kafka producer → Spark consumer → dbt loop → Flask dashboard. Invertir el orden causa errores de conexión o datos vacíos en el dashboard.

---

## Repositorio

🔗 **GitHub:** [https://github.com/Jhphill/olist-risk-pipeline](https://github.com/Jhphill/olist-risk-pipeline)

---

*Pipeline de Datos en Tiempo Real — Análisis de Riesgo de Impago E-Commerce Olist*
*UNIVALLE · Ingeniería de Datos · Gestión 2026*
