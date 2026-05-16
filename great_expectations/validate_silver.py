"""
validate_silver.py
Valida la tabla silver.stg_orders con Great Expectations v1.x
Ejecutar desde la raíz: python great_expectations/validate_silver.py
"""

import great_expectations as gx
from sqlalchemy import create_engine
import pandas as pd

# ── Conexión ──────────────────────────────────────────────────────────────────
DB_URL = "postgresql://olist_user:olist_pass@localhost:5433/olist_db"
engine = create_engine(DB_URL)

print("Cargando silver.stg_orders...")
df = pd.read_sql("SELECT * FROM silver.stg_orders", engine)
print(f"  {len(df):,} filas cargadas.")

# ── Contexto GE ───────────────────────────────────────────────────────────────
context = gx.get_context(mode="ephemeral")

# ── Data Source ───────────────────────────────────────────────────────────────
data_source = context.data_sources.add_pandas("olist_pandas")
data_asset  = data_source.add_dataframe_asset("stg_orders")
batch_def   = data_asset.add_batch_definition_whole_dataframe("full_batch")
batch       = batch_def.get_batch(batch_parameters={"dataframe": df})

# ── Suite de expectativas ─────────────────────────────────────────────────────
suite = context.suites.add(
    gx.ExpectationSuite(name="suite_stg_orders")
)

# Expectativa 1: order_id no debe tener nulos
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
)

# Expectativa 2: dias_atraso debe estar entre -365 y 365
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="dias_atraso",
        min_value=-365,
        max_value=365,
        mostly=0.99   # permite hasta 1% de nulos (pedidos sin fecha de entrega)
    )
)

# Expectativa 3: order_id debe ser casi único (>99% únicos)
suite.add_expectation(
    gx.expectations.ExpectColumnProportionOfUniqueValuesToBeBetween(
        column="order_id",
        min_value=0.99,
        max_value=1.0
    )
)

# Expectativa 4: conteo de filas entre 90,000 y 110,000
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=90_000,
        max_value=110_000
    )
)

# ── Validación ────────────────────────────────────────────────────────────────
validation_def = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="validacion_stg_orders",
        data=batch_def,
        suite=suite,
    )
)

result = validation_def.run(batch_parameters={"dataframe": df})

# ── Reporte ───────────────────────────────────────────────────────────────────
print("\n" + "═" * 55)
print("RESULTADO DE VALIDACIÓN — silver.stg_orders")
print("═" * 55)

exitos  = 0
fallos  = 0

for res in result.results:
    nombre     = res.expectation_config.type
    columna    = res.expectation_config.kwargs.get("column", "tabla")
    ok         = res.success
    estado     = "✔ PASS" if ok else "✘ FAIL"
    if ok:
        exitos += 1
    else:
        fallos += 1
    print(f"  {estado}  {nombre} ({columna})")

print("═" * 55)
print(f"  Resultado: {exitos} passed, {fallos} failed")
print(f"  Éxito global: {result.success}")
print("═" * 55)

engine.dispose()