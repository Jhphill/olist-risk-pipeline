# 🛒 Pipeline de Datos para Análisis de Riesgo de Impago — Olist

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.10-orange?logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-darkgreen?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

## Descripción

Pipeline de datos batch que ingesta, transforma y visualiza los datos del e-commerce brasileño **Olist**, permitiendo detectar patrones de riesgo de impago, cancelación de pedidos y baja satisfacción del cliente. El sistema implementa una arquitectura por capas (Raw → Silver → Gold) con validación de calidad de datos y orquestación automatizada.

## Arquitectura

```
CSVs Olist (9 archivos)
    ↓ carga_raw.py (Python + pandas + SQLAlchemy)
PostgreSQL: schema public (tablas raw_*)
    ↓ dbt models/silver/ (6 modelos — vistas)
PostgreSQL: schema silver (datos limpios + surrogate keys)
    ↓ Great Expectations (4 expectativas de calidad)
    ↓ dbt models/gold/ (5 modelos — tablas)
PostgreSQL: schema gold (esquema estrella)
    ↓ Dashboard HTML (4 visualizaciones)
    ↑ Todo orquestado por Apache Airflow DAG (7 tareas)
```

## Stack Tecnológico

| Herramienta | Versión | Rol |
|---|---|---|
| Python | 3.11 | Scripts de ingesta y validación |
| PostgreSQL | 15 | Base de datos: schemas raw, silver, gold |
| dbt-postgres | 1.10 | Transformación Silver → Gold |
| Great Expectations | 1.17 | Validación de calidad de datos |
| Apache Airflow | 2.8.1 | Orquestación del pipeline |
| Docker + Compose | Latest | Contenedores de todos los servicios |
| pandas + SQLAlchemy | Latest | Ingesta de CSVs a PostgreSQL |

## Modelo Dimensional (Gold)

```
gold.fct_orders (99,441 filas)
    ├── gold.dim_customer   (96,096 clientes únicos)
    ├── gold.dim_seller     (3,095 vendedores)
    ├── gold.dim_date       (634 fechas)
    └── gold.dim_payment_method (5 métodos de pago)
```

**flag_riesgo = 1** cuando:
- `order_status IN ('canceled', 'unavailable')`, o
- `review_score <= 2`, o
- `dias_atraso > 30`, o
- `payment_value = 0` con estado `delivered`

## Cómo Ejecutar

### 1. Requisitos
```bash
# Clonar el repositorio
git clone https://github.com/Jhphill/olist-risk-pipeline.git
cd olist-risk-pipeline

# Crear entorno virtual
python -m venv .venv
.venv\Scripts\Activate.ps1

# Instalar dependencias
pip install pandas sqlalchemy psycopg2-binary dbt-postgres great-expectations matplotlib
```

### 2. Levantar servicios Docker
```bash
docker compose up -d
# Esperar 30 segundos para que PostgreSQL inicialice
```

### 3. Cargar datos raw
```bash
python ingestion/carga_raw.py
# Output: 9 tablas raw_* cargadas (1,651,922 filas totales)
```

### 4. Ejecutar transformaciones dbt
```bash
cd dbt
dbt run        # Silver + Gold (11 modelos)
dbt test       # 25 tests — PASS=25
```

### 5. Validar calidad con Great Expectations
```bash
cd ..
python great_expectations/validate_silver.py
# Output: 4/4 PASS — Éxito global: True
```

### 6. Generar dashboard
```bash
python notebooks/dashboard.py
# Abre docs/dashboard.html en el navegador
```

## Estructura del Repositorio

```
olist-risk-pipeline/
├── docker-compose.yml          # Servicios: PostgreSQL + Airflow + Metabase
├── ingestion/
│   └── carga_raw.py            # Carga 9 CSVs → tablas raw_* en PostgreSQL
├── dbt/
│   ├── dbt_project.yml         # Configuración del proyecto dbt
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── silver/             # 6 modelos de limpieza y normalización
│       │   ├── sources.yml
│       │   ├── stg_orders.sql
│       │   ├── stg_customers.sql
│       │   ├── stg_payments.sql
│       │   ├── stg_reviews.sql
│       │   ├── stg_sellers.sql
│       │   └── stg_products.sql
│       ├── gold/               # 5 modelos del esquema estrella
│       │   ├── fct_orders.sql
│       │   ├── dim_customer.sql
│       │   ├── dim_seller.sql
│       │   ├── dim_date.sql
│       │   └── dim_payment_method.sql
│       └── schema.yml          # 25 tests de calidad
├── airflow/
│   └── dags/
│       └── olist_pipeline_dag.py  # DAG con 7 tareas
├── great_expectations/
│   └── validate_silver.py      # 4 expectativas sobre silver.stg_orders
├── notebooks/
│   ├── exploracion_olist.ipynb # Análisis exploratorio completo
│   └── dashboard.py            # Genera docs/dashboard.html
└── docs/
    ├── dashboard.html           # Dashboard con 4 visualizaciones
    └── screenshots/            # Capturas para el informe
```

## Dashboard

El dashboard incluye 4 visualizaciones sobre las tablas Gold:

1. **% Pedidos con flag_riesgo por estado** — identifica qué estados del pedido concentran mayor riesgo
2. **Review score promedio por región de Brasil** — análisis geográfico de satisfacción
3. **Cuotas vs satisfacción del cliente** — relación entre financiamiento y experiencia
4. **Top 10 estados con mayor % de riesgo** — ranking geográfico de riesgo

**KPIs principales:** 99,441 pedidos · 15% en riesgo · 0.6% cancelados

## Lecciones Aprendidas

1. **Puerto 5432 en conflicto** — PostgreSQL 18 local bloqueaba el contenedor Docker. Solución: mapear al puerto 5433 en docker-compose.
2. **dbt concatena schemas** — al definir `schema` en el `profiles.yml` y en el modelo, dbt los concatena. Solución: macro `generate_schema_name` que sobreescribe el comportamiento.
3. **Duplicados en fct_orders** — un pedido puede tener múltiples reviews y múltiples sellers. Solución: CTEs con `DISTINCT ON` para garantizar unicidad.
4. **RAM limitada con Docker** — 8GB insuficientes para PostgreSQL + Airflow + Metabase simultáneamente. Solución: dashboard HTML estático generado con matplotlib.
5. **Great Expectations v1.x** — API completamente diferente a v0.x. Se usó modo `ephemeral` sin archivos de configuración para simplificar la integración.

## Próximos Pasos — Mes 2

- Reemplazar `carga_raw.py` por un **productor Kafka** que simule streaming
- Implementar **Spark Structured Streaming** como consumidor → Delta Lake (Bronze)
- Mantener dbt, Airflow y dashboard exactamente igual

---

**Universidad Privada del Valle (UNIVALLE)** · Facultad de Ciencias Empresariales · Ingeniería de Datos · 2026  
Juan Felipe Caballero Flores · Luciana Sofía Coca Terrazas  
Docente: M.Sc. Ing. Oscar Contreras Carrasco