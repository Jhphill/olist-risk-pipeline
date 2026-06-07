"""
dashboard.py
Genera un dashboard HTML estático con 4 visualizaciones desde gold.*
Ejecutar: python notebooks/dashboard.py
Output:  docs/dashboard.html
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib
matplotlib.use('Agg')
from sqlalchemy import create_engine
import os

DB_URL = "postgresql://olist_user:olist_pass@localhost:5433/olist_db"
engine = create_engine(DB_URL)
os.makedirs("docs/screenshots", exist_ok=True)

print("Cargando datos desde gold...")

# ── Consulta 1: Tasa de cancelación ──────────────────────────────────────────
df_status = pd.read_sql("""
    SELECT order_status,
           SUM(flag_riesgo) AS en_riesgo,
           COUNT(*)         AS total,
           ROUND(SUM(flag_riesgo) * 100.0 / COUNT(*), 1) AS pct_riesgo
    FROM gold.fct_orders
    GROUP BY order_status
    ORDER BY pct_riesgo DESC
""", engine)

# ── Consulta 2: Review score por región ──────────────────────────────────────
df_region = pd.read_sql("""
    SELECT c.region_brasil,
           ROUND(AVG(f.review_score)::numeric, 2) AS avg_score,
           COUNT(*) AS total
    FROM gold.fct_orders f
    JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE f.review_score IS NOT NULL
    GROUP BY c.region_brasil
    ORDER BY avg_score DESC
""", engine)

# ── Consulta 3: Cuotas vs review score ───────────────────────────────────────
df_cuotas = pd.read_sql("""
    SELECT installments,
           ROUND(AVG(review_score)::numeric, 2) AS avg_score,
           COUNT(*) AS total
    FROM gold.fct_orders
    WHERE review_score IS NOT NULL
      AND installments BETWEEN 1 AND 12
    GROUP BY installments
    ORDER BY installments
""", engine)

# ── Consulta 4: Top 10 estados por flag_riesgo ───────────────────────────────
df_estados = pd.read_sql("""
    SELECT c.state,
           SUM(f.flag_riesgo) AS en_riesgo,
           COUNT(*) AS total,
           ROUND(SUM(f.flag_riesgo) * 100.0 / COUNT(*), 1) AS pct_riesgo
    FROM gold.fct_orders f
    JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
    GROUP BY c.state
    ORDER BY pct_riesgo DESC
    LIMIT 10
""", engine)

engine.dispose()
print("Datos cargados. Generando visualizaciones...")

# ── KPI global ────────────────────────────────────────────────────────────────
total_pedidos  = df_status['total'].sum()
total_riesgo   = df_status['en_riesgo'].sum()
pct_riesgo_global = round(total_riesgo / total_pedidos * 100, 1)
cancelados     = df_status[df_status['order_status']=='canceled']['total'].sum()
pct_cancelados = round(cancelados / total_pedidos * 100, 1)

# ── Fig 1: Pedidos en riesgo por status ──────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 4))
colores = ['#E74C3C' if s in ['canceled','unavailable'] else '#3498DB'
           for s in df_status['order_status']]
bars = ax.bar(df_status['order_status'], df_status['pct_riesgo'],
              color=colores, edgecolor='white')
ax.bar_label(bars, fmt='%.1f%%', padding=3, fontsize=9)
ax.set_title('% Pedidos con Flag de Riesgo por Estado', fontweight='bold', pad=12)
ax.set_xlabel('Estado del pedido')
ax.set_ylabel('% con flag_riesgo = 1')
ax.set_ylim(0, 110)
plt.xticks(rotation=20, ha='right')
plt.tight_layout()
plt.savefig('docs/screenshots/viz1_riesgo_status.png', dpi=120)
plt.close()

# ── Fig 2: Review score por región ───────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7, 4))
colores2 = ['#2ECC71','#3498DB','#F39C12','#9B59B6','#E74C3C']
bars2 = ax.barh(df_region['region_brasil'], df_region['avg_score'],
                color=colores2[:len(df_region)], edgecolor='white')
ax.bar_label(bars2, fmt='%.2f', padding=3, fontsize=10)
ax.set_title('Review Score Promedio por Región de Brasil', fontweight='bold', pad=12)
ax.set_xlabel('Score promedio (1-5)')
ax.set_xlim(0, 5.5)
ax.axvline(x=4.0, color='gray', linestyle='--', alpha=0.5, label='Score 4.0')
ax.legend(fontsize=8)
plt.tight_layout()
plt.savefig('docs/screenshots/viz2_score_region.png', dpi=120)
plt.close()

# ── Fig 3: Cuotas vs review score ────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(8, 4))
ax.plot(df_cuotas['installments'], df_cuotas['avg_score'],
        marker='o', color='#3498DB', linewidth=2, markersize=7)
ax.fill_between(df_cuotas['installments'], df_cuotas['avg_score'],
                alpha=0.15, color='#3498DB')
ax.set_title('Review Score Promedio según Número de Cuotas', fontweight='bold', pad=12)
ax.set_xlabel('Número de cuotas')
ax.set_ylabel('Score promedio')
ax.set_ylim(3.5, 5.0)
ax.set_xticks(df_cuotas['installments'])
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('docs/screenshots/viz3_cuotas_score.png', dpi=120)
plt.close()

# ── Fig 4: Top 10 estados por riesgo ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(8, 5))
colores4 = ['#E74C3C' if p > 30 else '#E67E22' if p > 20 else '#F1C40F'
            for p in df_estados['pct_riesgo']]
bars4 = ax.barh(df_estados['state'], df_estados['pct_riesgo'],
                color=colores4, edgecolor='white')
ax.bar_label(bars4, fmt='%.1f%%', padding=3, fontsize=9)
ax.set_title('Top 10 Estados con Mayor % de Pedidos en Riesgo', fontweight='bold', pad=12)
ax.set_xlabel('% de pedidos con flag_riesgo = 1')
ax.invert_yaxis()
plt.tight_layout()
plt.savefig('docs/screenshots/viz4_riesgo_estados.png', dpi=120)
plt.close()

print("Figuras guardadas. Generando HTML...")

# ── HTML Dashboard ────────────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Dashboard — Análisis de Riesgo Olist</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; }}
  header {{ background: linear-gradient(135deg, #1a1f2e, #2d3561);
            padding: 2rem; text-align: center; border-bottom: 2px solid #3498DB; }}
  header h1 {{ font-size: 1.8rem; color: #fff; margin-bottom: 0.3rem; }}
  header p  {{ color: #94a3b8; font-size: 0.9rem; }}
  .kpis {{ display: flex; gap: 1rem; padding: 1.5rem 2rem; flex-wrap: wrap; }}
  .kpi {{ background: #1e2433; border-radius: 10px; padding: 1.2rem 1.8rem;
          flex: 1; min-width: 160px; border-left: 4px solid #3498DB; }}
  .kpi .val {{ font-size: 2rem; font-weight: bold; color: #3498DB; }}
  .kpi .lab {{ font-size: 0.8rem; color: #94a3b8; margin-top: 0.3rem; }}
  .kpi.red {{ border-left-color: #E74C3C; }}
  .kpi.red .val {{ color: #E74C3C; }}
  .kpi.green {{ border-left-color: #2ECC71; }}
  .kpi.green .val {{ color: #2ECC71; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr;
           gap: 1.5rem; padding: 0 2rem 2rem; }}
  .card {{ background: #1e2433; border-radius: 12px; padding: 1.2rem;
           border: 1px solid #2d3561; }}
  .card h3 {{ font-size: 0.9rem; color: #94a3b8; margin-bottom: 1rem;
              text-transform: uppercase; letter-spacing: 0.05em; }}
  .card img {{ width: 100%; border-radius: 6px; }}
  footer {{ text-align: center; padding: 1.5rem; color: #4a5568; font-size: 0.8rem; }}
  @media (max-width: 768px) {{ .grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<header>
  <h1>📊 Pipeline Olist — Análisis de Riesgo de Impago</h1>
  <p>UNIVALLE · Ingeniería de Datos · 2026 · Juan Felipe Caballero & Luciana Coca</p>
</header>

<div class="kpis">
  <div class="kpi"><div class="val">{total_pedidos:,}</div>
    <div class="lab">Total de pedidos</div></div>
  <div class="kpi red"><div class="val">{pct_riesgo_global}%</div>
    <div class="lab">Pedidos con flag_riesgo</div></div>
  <div class="kpi red"><div class="val">{pct_cancelados}%</div>
    <div class="lab">Tasa de cancelación</div></div>
  <div class="kpi green"><div class="val">{float(df_region['avg_score'].mean()):.2f}</div>
    <div class="lab">Review score promedio</div></div>
  <div class="kpi"><div class="val">99,441</div>
    <div class="lab">Pedidos únicos validados</div></div>
</div>

<div class="grid">
  <div class="card">
    <h3>Riesgo por estado del pedido</h3>
    <img src="screenshots/viz1_riesgo_status.png">
  </div>
  <div class="card">
    <h3>Review score por región</h3>
    <img src="screenshots/viz2_score_region.png">
  </div>
  <div class="card">
    <h3>Cuotas vs satisfacción del cliente</h3>
    <img src="screenshots/viz3_cuotas_score.png">
  </div>
  <div class="card">
    <h3>Top 10 estados con mayor riesgo</h3>
    <img src="screenshots/viz4_riesgo_estados.png">
  </div>
</div>

<footer>
  Pipeline de Datos · Dataset Olist · dbt + PostgreSQL + Great Expectations
</footer>
</body>
</html>"""

with open("docs/dashboard.html", "w", encoding="utf-8") as f:
    f.write(html)

print("✔ Dashboard generado en docs/dashboard.html")
print(f"  KPIs: {total_pedidos:,} pedidos | {pct_riesgo_global}% en riesgo | {pct_cancelados}% cancelados")