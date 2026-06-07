"""
dashboard_live.py — Olist Risk Pipeline  (v3 — bugs corregidos)
Flask + Chart.js — consulta gold.* en PostgreSQL
Auto-refresh cada 30s — muestra cambios en tiempo real conforme fluye el pipeline

Correcciones v3:
  - VIZ 2: query directa a fct_orders sin JOIN dim_date (usa purchase_ts)
  - VIZ 3: fallback sin JOIN cuando dim_customer está vacío
  - Gráficas: altura fija con chart-wrap, maintainAspectRatio:false correcto
  - Bronze→Gold: el dashboard SOLO muestra datos; dbt/Airflow promueve las capas
"""

from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
import psycopg2, psycopg2.extras, os
from datetime import datetime

app  = Flask(__name__)
CORS(app)

DB = dict(
    host     = os.getenv("DB_HOST",     "localhost"),
    port     = int(os.getenv("DB_PORT", "5433")),
    dbname   = os.getenv("DB_NAME",     "olist_db"),
    user     = os.getenv("DB_USER",     "olist_user"),
    password = os.getenv("DB_PASSWORD", "olist_pass"),
)

def q(sql, p=None):
    with psycopg2.connect(**DB) as cn:
        with cn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, p)
            return cur.fetchall()

def safe_q(sql):
    try:    return [dict(r) for r in q(sql)]
    except: return []

def safe_one(sql):
    try:
        rows = q(sql)
        return dict(rows[0]) if rows else {}
    except: return {}

def count(schema, table):
    try:    return int(q(f"SELECT COUNT(*) AS n FROM {schema}.{table}")[0]["n"])
    except: return None

# ── KPIs ─────────────────────────────────────────────────────────────────────
@app.route("/api/kpis")
def kpis():
    return jsonify(safe_one("""
        SELECT
            COUNT(*)                                                AS total_pedidos,
            SUM(flag_riesgo)                                        AS pedidos_riesgo,
            ROUND(SUM(flag_riesgo)*100.0/NULLIF(COUNT(*),0), 2)    AS tasa_riesgo_pct,
            ROUND(AVG(review_score)::numeric,2)                              AS avg_review_score,
            ROUND(AVG(total_value)::numeric,2)                               AS avg_ticket,
            ROUND(AVG(dias_atraso) FILTER(WHERE dias_atraso>0)::numeric,1)  AS avg_dias_atraso
        FROM gold.fct_orders
    """))

# ── VIZ 2: evolución mensual — usa purchase_ts directo, sin JOIN dim_date ──
@app.route("/api/riesgo_mensual")
def riesgo_mensual():
    # Intenta primero con dim_date; si falla o devuelve 0 filas, usa fct_orders solo
    rows = safe_q("""
        SELECT
            d.year,
            d.month,
            ROUND(AVG(f.flag_riesgo::numeric)*100, 2) AS tasa_riesgo_pct,
            COUNT(*)                                    AS total_pedidos
        FROM gold.fct_orders f
        JOIN gold.dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """)
    if not rows:
        # fallback: extraer año/mes directo de fct_orders si tiene columna de fecha
        rows = safe_q("""
            SELECT
                EXTRACT(YEAR  FROM purchase_ts)::int  AS year,
                EXTRACT(MONTH FROM purchase_ts)::int  AS month,
                ROUND(AVG(flag_riesgo::numeric)*100,2)  AS tasa_riesgo_pct,
                COUNT(*)                                 AS total_pedidos
            FROM gold.fct_orders
            WHERE purchase_ts IS NOT NULL
            GROUP BY 1,2
            ORDER BY 1,2
        """)
    return jsonify(rows)

# ── VIZ 3: riesgo por estado — con fallback sin JOIN ─────────────────────────
@app.route("/api/riesgo_estado")
def riesgo_estado():
    rows = safe_q("""
        SELECT
            c.state,
            COUNT(*)                                    AS total_pedidos,
            SUM(f.flag_riesgo)                          AS pedidos_riesgo,
            ROUND(AVG(f.flag_riesgo::numeric)*100,2)   AS tasa_riesgo_pct,
            ROUND(AVG(f.review_score)::numeric,2)                AS avg_review
        FROM gold.fct_orders f
        JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
        WHERE c.state IS NOT NULL
        GROUP BY c.state
        HAVING COUNT(*) >= 5
        ORDER BY tasa_riesgo_pct DESC
        LIMIT 15
    """)
    return jsonify(rows)

# ── VIZ 4: top vendedores ────────────────────────────────────────────────────
@app.route("/api/top_vendedores")
def top_vendedores():
    rows = safe_q("""
        SELECT
            s.seller_id, s.city, s.state,
            COUNT(*)                                    AS total_pedidos,
            SUM(f.flag_riesgo)                          AS pedidos_riesgo,
            ROUND(AVG(f.flag_riesgo::numeric)*100,2)   AS tasa_riesgo_pct
        FROM gold.fct_orders f
        JOIN gold.dim_seller s ON f.seller_sk = s.seller_sk
        GROUP BY s.seller_id, s.city, s.state
        HAVING COUNT(*) >= 5
        ORDER BY tasa_riesgo_pct DESC
        LIMIT 10
    """)
    return jsonify(rows)

# ── VIZ 5: método de pago ────────────────────────────────────────────────────
@app.route("/api/pago_vs_riesgo")
def pago_vs_riesgo():
    rows = safe_q("""
        SELECT
            pm.payment_type,
            COUNT(*)                                    AS total_pedidos,
            ROUND(AVG(f.flag_riesgo::numeric)*100,2)   AS tasa_riesgo_pct,
            ROUND(AVG(f.review_score)::numeric,2)                AS avg_review
        FROM gold.fct_orders f
        JOIN gold.dim_payment_method pm ON f.payment_method_sk = pm.payment_method_sk
        GROUP BY pm.payment_type
        ORDER BY tasa_riesgo_pct DESC
    """)
    return jsonify(rows)

# ── VIZ 6: cuotas vs satisfacción ───────────────────────────────────────────
@app.route("/api/cuotas_satisfaccion")
def cuotas_satisfaccion():
    rows = safe_q("""
        SELECT
            installments                              AS num_cuotas,
            COUNT(*)                                  AS total_pedidos,
            ROUND(AVG(review_score),3)                AS avg_review,
            ROUND(AVG(flag_riesgo::numeric)*100,2)   AS tasa_riesgo_pct
        FROM gold.fct_orders
        WHERE installments BETWEEN 1 AND 12
        GROUP BY installments
        ORDER BY installments
    """)
    return jsonify(rows)

# ── Estado del pipeline ──────────────────────────────────────────────────────
@app.route("/api/pipeline_status")
def pipeline_status():
    return jsonify({
        "layers": {
            "bronze_orders":     count("bronze","orders"),
            "bronze_payments":   count("bronze","payments"),
            "bronze_reviews":    count("bronze","reviews"),
            "silver_stg_orders": count("silver","stg_orders"),
            "gold_fct_orders":   count("gold","fct_orders"),
            "gold_dim_customer": count("gold","dim_customer"),
            "gold_dim_seller":   count("gold","dim_seller"),
        },
        "last_updated": datetime.now().isoformat()
    })

# ── Index ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

# ═══════════════════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Olist Risk — Live Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#08101e; --panel:#0f1929; --border:#1a2840;
  --accent:#00c8f0; --accent2:#ff6530; --accent3:#1fc98a;
  --danger:#ff3f50; --warn:#f59e0b;
  --text:#dde6f0; --muted:#4e6280;
  --mono:'Space Mono',monospace; --sans:'DM Sans',sans-serif;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;overflow-x:hidden}

/* scanlines */
body::after{content:'';position:fixed;inset:0;pointer-events:none;z-index:9998;
  background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(0,200,240,.012) 3px,rgba(0,200,240,.012) 4px)}

/* HEADER */
header{
  display:flex;align-items:center;justify-content:space-between;
  padding:14px 28px;border-bottom:1px solid var(--border);
  background:rgba(15,25,41,.97);backdrop-filter:blur(8px);
  position:sticky;top:0;z-index:100;
}
.logo{display:flex;align-items:center;gap:10px}
.ldot{width:8px;height:8px;background:var(--accent);border-radius:50%;
  box-shadow:0 0 10px var(--accent);animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.ltitle{font-family:var(--mono);font-size:12px;color:var(--accent);letter-spacing:.07em;font-weight:700}
.lsub{font-size:10px;color:var(--muted);font-family:var(--mono)}
.hright{display:flex;align-items:center;gap:12px}
.live{display:flex;align-items:center;gap:5px;background:rgba(0,200,240,.07);
  border:1px solid rgba(0,200,240,.2);border-radius:16px;padding:3px 10px;
  font-family:var(--mono);font-size:10px;color:var(--accent)}
.ldot2{width:5px;height:5px;background:var(--accent3);border-radius:50%;animation:blink 1.4s infinite}
#ts{font-family:var(--mono);font-size:10px;color:var(--muted)}
#rbtn{background:rgba(0,200,240,.08);border:1px solid rgba(0,200,240,.25);color:var(--accent);
  font-family:var(--mono);font-size:10px;padding:5px 12px;border-radius:5px;cursor:pointer;transition:all .2s}
#rbtn:hover{background:rgba(0,200,240,.18)}

/* MAIN */
main{padding:20px 28px;max-width:1560px;margin:0 auto}
.stitle{font-family:var(--mono);font-size:10px;color:var(--muted);letter-spacing:.1em;
  text-transform:uppercase;margin:24px 0 12px;padding-left:10px;border-left:2px solid var(--accent)}

/* KPI GRID */
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px}
.kpi{background:var(--panel);border:1px solid var(--border);border-radius:10px;
  padding:16px 18px;position:relative;overflow:hidden;transition:border-color .3s,transform .2s}
.kpi:hover{border-color:rgba(0,200,240,.25);transform:translateY(-2px)}
.kpi::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--accent),transparent);opacity:0;transition:opacity .3s}
.kpi:hover::before{opacity:1}
.kl{font-family:var(--mono);font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}
.kv{font-family:var(--mono);font-size:28px;font-weight:700;line-height:1;transition:color .4s}
.kv.ok{color:var(--accent3)} .kv.warn{color:var(--warn)} .kv.bad{color:var(--danger)}
.kd{margin-top:6px;font-size:10px;color:var(--muted);font-family:var(--mono)}

/* PIPELINE */
.pbar{display:flex;align-items:center;background:var(--panel);border:1px solid var(--border);
  border-radius:10px;padding:14px 18px;overflow-x:auto;gap:0}
.pstage{display:flex;flex-direction:column;align-items:center;min-width:100px;flex-shrink:0}
.pname{font-family:var(--mono);font-size:8.5px;color:var(--muted);text-transform:uppercase;
  letter-spacing:.06em;margin-bottom:4px;text-align:center;line-height:1.3}
.pn{font-family:var(--mono);font-size:17px;font-weight:700;color:var(--accent)}
.pn.br{color:#f97316} .pn.si{color:#a78bfa} .pn.go{color:#fbbf24} .pn.null{color:var(--muted);font-size:13px}
.parr{flex:1;height:1px;background:var(--border);position:relative;min-width:16px}
.parr::after{content:'▶';position:absolute;right:-5px;top:-6px;color:var(--muted);font-size:8px}

/* CHART GRID */
.cgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:960px){.cgrid{grid-template-columns:1fr}}
.ccard{background:var(--panel);border:1px solid var(--border);border-radius:10px;
  padding:16px;position:relative;display:flex;flex-direction:column;transition:border-color .3s}
.ccard:hover{border-color:rgba(0,200,240,.18)}
.chead{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;flex-shrink:0}
.ctitle{font-family:var(--mono);font-size:10px;color:var(--accent);font-weight:700;letter-spacing:.05em}
.csub{font-size:9px;color:var(--muted);margin-top:2px}
.cbadge{font-family:var(--mono);font-size:9px;background:rgba(0,200,240,.06);
  border:1px solid rgba(0,200,240,.15);border-radius:3px;padding:2px 6px;color:var(--muted);flex-shrink:0}

/* ── CHART WRAP — altura fija, el canvas se estira dentro ── */
.cwrap{position:relative;width:100%;height:240px;flex-shrink:0}
.cwrap.tall{height:270px}
.cwrap canvas{position:absolute!important;inset:0;width:100%!important;height:100%!important}

/* TABLE */
.twrap{overflow-y:auto;max-height:270px;flex:1}
table{width:100%;border-collapse:collapse;font-size:11px}
th{font-family:var(--mono);font-size:8.5px;color:var(--muted);text-transform:uppercase;
  letter-spacing:.06em;padding:6px 9px;text-align:left;border-bottom:1px solid var(--border);
  position:sticky;top:0;background:var(--panel);z-index:1}
td{padding:7px 9px;border-bottom:1px solid rgba(26,40,64,.6)}
tr:hover td{background:rgba(0,200,240,.03)}
.rbar-bg{flex:1;height:3px;background:var(--border);border-radius:2px;min-width:40px}
.rbar-fill{height:100%;border-radius:2px;background:linear-gradient(90deg,var(--accent3),var(--danger))}
.rbar-wrap{display:flex;align-items:center;gap:5px}

/* LOADING */
.lov{position:absolute;inset:0;background:rgba(8,16,30,.7);display:flex;align-items:center;
  justify-content:center;border-radius:10px;z-index:10;opacity:0;pointer-events:none;transition:opacity .2s}
.lov.on{opacity:1;pointer-events:auto}
.spin{width:18px;height:18px;border:2px solid var(--border);border-top-color:var(--accent);
  border-radius:50%;animation:sp .65s linear infinite}
@keyframes sp{to{transform:rotate(360deg)}}

/* TOAST */
#toast{position:fixed;bottom:18px;right:18px;background:rgba(255,63,80,.12);
  border:1px solid rgba(255,63,80,.35);color:var(--danger);font-family:var(--mono);
  font-size:10px;padding:7px 14px;border-radius:7px;
  transform:translateY(50px);opacity:0;transition:all .3s;z-index:9999}
#toast.on{transform:translateY(0);opacity:1}

footer{margin-top:32px;padding:12px 28px;border-top:1px solid var(--border);
  text-align:center;font-family:var(--mono);font-size:9px;color:var(--muted)}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="ldot"></div>
    <div>
      <div class="ltitle">OLIST RISK PIPELINE</div>
      <div class="lsub">Dashboard Live · dbt · Kafka · Spark · UNIVALLE 2026</div>
    </div>
  </div>
  <div class="hright">
    <div class="live"><div class="ldot2"></div>LIVE</div>
    <span id="ts">—</span>
    <button id="rbtn" onclick="refreshAll()">↻ Actualizar</button>
  </div>
</header>

<main>

<!-- KPIs -->
<div class="stitle">▸ KPIs Globales — gold.fct_orders</div>
<div class="kpi-grid">
  <div class="kpi"><div class="kl">Total Pedidos</div><div class="kv ok" id="kpi-total">—</div><div class="kd">en gold.fct_orders</div></div>
  <div class="kpi"><div class="kl">Pedidos en Riesgo</div><div class="kv warn" id="kpi-riesgo">—</div><div class="kd">flag_riesgo = 1</div></div>
  <div class="kpi"><div class="kl">Tasa de Riesgo</div><div class="kv bad" id="kpi-tasa">—</div><div class="kd">% del total</div></div>
  <div class="kpi"><div class="kl">Avg Review Score</div><div class="kv" id="kpi-review">—</div><div class="kd">escala 1–5</div></div>
  <div class="kpi"><div class="kl">Ticket Promedio</div><div class="kv ok" id="kpi-ticket">—</div><div class="kd">BRL</div></div>
  <div class="kpi"><div class="kl">Días Atraso Prom.</div><div class="kv warn" id="kpi-atraso">—</div><div class="kd">pedidos atrasados</div></div>
</div>

<!-- PIPELINE -->
<div class="stitle">▸ Estado del Pipeline — filas por capa (se actualiza cada 30s)</div>
<div class="pbar">
  <div class="pstage"><div class="pname">BRONZE<br>Orders</div><div class="pn br" id="p-bronze-orders">—</div></div>
  <div class="parr"></div>
  <div class="pstage"><div class="pname">BRONZE<br>Payments</div><div class="pn br" id="p-bronze-payments">—</div></div>
  <div class="parr"></div>
  <div class="pstage"><div class="pname">BRONZE<br>Reviews</div><div class="pn br" id="p-bronze-reviews">—</div></div>
  <div class="parr"></div>
  <div class="pstage"><div class="pname">SILVER<br>stg_orders</div><div class="pn si" id="p-silver-stg_orders">—</div></div>
  <div class="parr"></div>
  <div class="pstage"><div class="pname">GOLD<br>fct_orders</div><div class="pn go" id="p-gold-fct_orders">—</div></div>
  <div class="parr"></div>
  <div class="pstage"><div class="pname">GOLD<br>dim_customer</div><div class="pn go" id="p-gold-dim_customer">—</div></div>
  <div class="parr"></div>
  <div class="pstage"><div class="pname">GOLD<br>dim_seller</div><div class="pn go" id="p-gold-dim_seller">—</div></div>
</div>

<!-- ROW 1 -->
<div class="stitle">▸ Análisis Temporal y Geográfico</div>
<div class="cgrid">
  <div class="ccard" id="cc-linea">
    <div class="lov"><div class="spin"></div></div>
    <div class="chead"><div><div class="ctitle">EVOLUCIÓN MENSUAL DEL RIESGO</div><div class="csub">Tasa de riesgo % · mes a mes · 2016–2018</div></div><div class="cbadge">VIZ 2</div></div>
    <div class="cwrap"><canvas id="ch-linea"></canvas></div>
  </div>
  <div class="ccard" id="cc-estados">
    <div class="lov"><div class="spin"></div></div>
    <div class="chead"><div><div class="ctitle">RIESGO POR ESTADO DE BRASIL</div><div class="csub">Top 15 estados · tasa de riesgo %</div></div><div class="cbadge">VIZ 3</div></div>
    <div class="cwrap"><canvas id="ch-estados"></canvas></div>
  </div>
</div>

<!-- ROW 2 -->
<div class="stitle">▸ Vendedores y Métodos de Pago</div>
<div class="cgrid">
  <div class="ccard" id="cc-vend">
    <div class="lov"><div class="spin"></div></div>
    <div class="chead"><div><div class="ctitle">TOP 10 VENDEDORES · MAYOR RIESGO</div><div class="csub">Solo vendedores con ≥ 5 pedidos</div></div><div class="cbadge">VIZ 4</div></div>
    <div class="cwrap tall"><canvas id="ch-vend"></canvas></div>
  </div>
  <div class="ccard" id="cc-pagos">
    <div class="lov"><div class="spin"></div></div>
    <div class="chead"><div><div class="ctitle">MÉTODO DE PAGO VS RIESGO</div><div class="csub">% riesgo y avg review por tipo de pago</div></div><div class="cbadge">VIZ 5</div></div>
    <div class="cwrap tall"><canvas id="ch-pagos"></canvas></div>
  </div>
</div>

<!-- ROW 3 -->
<div class="stitle">▸ Cuotas vs Satisfacción</div>
<div class="cgrid">
  <div class="ccard" id="cc-cuotas">
    <div class="lov"><div class="spin"></div></div>
    <div class="chead"><div><div class="ctitle">CUOTAS VS SATISFACCIÓN DEL CLIENTE</div><div class="csub">Avg review y tasa de riesgo por nº de cuotas</div></div><div class="cbadge">VIZ 6</div></div>
    <div class="cwrap"><canvas id="ch-cuotas"></canvas></div>
  </div>
  <div class="ccard" id="cc-tabla">
    <div class="lov"><div class="spin"></div></div>
    <div class="chead"><div><div class="ctitle">TABLA — TOP VENDEDORES EN RIESGO</div><div class="csub">Detalle completo del top 10</div></div><div class="cbadge">DETALLE</div></div>
    <div class="twrap">
      <table id="t-vend">
        <thead><tr><th>#</th><th>Seller ID</th><th>Ciudad</th><th>Estado</th><th>Pedidos</th><th>Riesgo %</th><th>Barra</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>

</main>

<footer>
  Olist Risk Pipeline · Juan Felipe Caballero Flores &amp; Luciana Sofía Coca Terrazas
  · UNIVALLE · Ing. de Datos 2026 · Docente: M.Sc. Ing. Oscar Contreras Carrasco
</footer>
<div id="toast"></div>

<script>
// ── Chart.js defaults ────────────────────────────────────────────────────────
Chart.defaults.color         = '#4e6280';
Chart.defaults.borderColor   = '#1a2840';
Chart.defaults.font.family   = "'Space Mono', monospace";
Chart.defaults.font.size     = 10;

const C = { accent:'#00c8f0', accent2:'#ff6530', accent3:'#1fc98a', danger:'#ff3f50', warn:'#f59e0b' };
const charts = {};

// helpers
const $  = id => document.getElementById(id);
const ctx = id => $(id).getContext('2d');
const fmt = (n,d=0) => n==null ? '—' : Number(n).toLocaleString('es-BO',{minimumFractionDigits:d,maximumFractionDigits:d});
const showL = id => { const el = document.querySelector(`#${id} .lov`); if(el) el.classList.add('on'); };
const hideL = id => { const el = document.querySelector(`#${id} .lov`); if(el) el.classList.remove('on'); };
const kill  = k  => { if(charts[k]){ charts[k].destroy(); delete charts[k]; } };
function toast(msg){ const t=$('toast'); t.textContent='⚠ '+msg; t.classList.add('on'); setTimeout(()=>t.classList.remove('on'),4000); }

async function get(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error('HTTP '+r.status);
  return r.json();
}

// ── 1. KPIs ──────────────────────────────────────────────────────────────────
async function loadKpis(){
  try{
    const d = await get('/api/kpis');
    $('kpi-total').textContent  = fmt(d.total_pedidos);
    $('kpi-riesgo').textContent = fmt(d.pedidos_riesgo);
    $('kpi-tasa').textContent   = fmt(d.tasa_riesgo_pct,1)+'%';
    $('kpi-review').textContent = fmt(d.avg_review_score,2);
    $('kpi-ticket').textContent = 'R$'+fmt(d.avg_ticket,0);
    $('kpi-atraso').textContent = fmt(d.avg_dias_atraso,1)+'d';
  }catch(e){ toast('KPIs: '+e.message); }
}

// ── 2. Pipeline status ───────────────────────────────────────────────────────
async function loadPipeline(){
  try{
    const d = await get('/api/pipeline_status');
    const L = d.layers;
    [['bronze_orders','p-bronze-orders'],
     ['bronze_payments','p-bronze-payments'],
     ['bronze_reviews','p-bronze-reviews'],
     ['silver_stg_orders','p-silver-stg_orders'],
     ['gold_fct_orders','p-gold-fct_orders'],
     ['gold_dim_customer','p-gold-dim_customer'],
     ['gold_dim_seller','p-gold-dim_seller']
    ].forEach(([k,id])=>{
      const el=$(id); if(!el) return;
      const v=L[k];
      el.textContent = v!=null ? fmt(v) : '—';
    });
    $('ts').textContent = '↻ '+new Date(d.last_updated).toLocaleTimeString('es-BO');
  }catch(e){ toast('Pipeline: '+e.message); }
}

// ── 3. Evolución mensual ─────────────────────────────────────────────────────
async function loadLinea(){
  showL('cc-linea');
  try{
    const data = await get('/api/riesgo_mensual');
    if(!data.length){ hideL('cc-linea'); return; }
    const labels = data.map(r=>`${r.year}-${String(r.month).padStart(2,'0')}`);
    const tasa   = data.map(r=>parseFloat(r.tasa_riesgo_pct)||0);
    const total  = data.map(r=>parseInt(r.total_pedidos)||0);
    kill('linea');
    charts['linea'] = new Chart(ctx('ch-linea'),{
      type:'line',
      data:{ labels, datasets:[
        { label:'Tasa Riesgo %', data:tasa, yAxisID:'y',
          borderColor:C.danger, backgroundColor:'rgba(255,63,80,.07)',
          borderWidth:2, pointRadius:2.5, pointHoverRadius:5, tension:.35, fill:true },
        { label:'Total Pedidos', data:total, yAxisID:'y2',
          borderColor:C.accent, borderWidth:1.5, pointRadius:1.5,
          borderDash:[4,3], tension:.3, fill:false }
      ]},
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{mode:'index',intersect:false},
        plugins:{ legend:{labels:{usePointStyle:true,padding:10,boxWidth:8}} },
        scales:{
          x:{ grid:{color:'#1a2840'}, ticks:{maxRotation:45,font:{size:8}} },
          y:{ grid:{color:'#1a2840'}, min:0,
              title:{display:true,text:'Riesgo %',color:C.danger,font:{size:9}} },
          y2:{ position:'right', grid:{drawOnChartArea:false},
               title:{display:true,text:'Pedidos',color:C.accent,font:{size:9}} }
        }
      }
    });
  }catch(e){ toast('VIZ2: '+e.message); }
  hideL('cc-linea');
}

// ── 4. Riesgo por estado ─────────────────────────────────────────────────────
async function loadEstados(){
  showL('cc-estados');
  try{
    const data = await get('/api/riesgo_estado');
    if(!data.length){ hideL('cc-estados'); return; }
    const labels = data.map(r=>String(r.state||'?'));
    const tasa   = data.map(r=>parseFloat(r.tasa_riesgo_pct)||0);
    const mx     = Math.max(...tasa)||1;
    // color ramp: verde→rojo según ratio
    const colors = tasa.map(v=>{
      const t=v/mx;
      return `rgba(${Math.round(31+t*224)},${Math.round(201-t*138)},${Math.round(138-t*88)},.72)`;
    });
    kill('estados');
    charts['estados'] = new Chart(ctx('ch-estados'),{
      type:'bar',
      data:{ labels, datasets:[{
        label:'Tasa Riesgo %', data:tasa,
        backgroundColor:colors, borderWidth:0, borderRadius:3
      }]},
      options:{
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{display:false},
          tooltip:{ callbacks:{ label: ctx=>`${ctx.parsed.x.toFixed(1)}%` }}},
        scales:{
          x:{ grid:{color:'#1a2840'}, min:0,
              ticks:{ callback:v=>v+'%', font:{size:8} }},
          y:{ grid:{display:false}, ticks:{font:{size:9}} }
        }
      }
    });
  }catch(e){ toast('VIZ3: '+e.message); }
  hideL('cc-estados');
}

// ── 5. Top vendedores ────────────────────────────────────────────────────────
async function loadVendedores(){
  showL('cc-vend'); showL('cc-tabla');
  try{
    const data = await get('/api/top_vendedores');
    if(data.length){
      const labels = data.map(r=>r.seller_id.substring(0,8)+'…');
      const tasa   = data.map(r=>parseFloat(r.tasa_riesgo_pct)||0);
      const peds   = data.map(r=>parseInt(r.total_pedidos)||0);
      kill('vend');
      charts['vend'] = new Chart(ctx('ch-vend'),{
        type:'bar',
        data:{ labels, datasets:[
          { label:'Tasa Riesgo %', data:tasa, backgroundColor:'rgba(255,101,48,.7)',
            borderWidth:0, borderRadius:3, yAxisID:'y' },
          { label:'Total Pedidos', data:peds, type:'line',
            borderColor:C.accent, borderWidth:1.5, pointRadius:3,
            backgroundColor:'transparent', yAxisID:'y2' }
        ]},
        options:{
          indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{labels:{usePointStyle:true,padding:10,boxWidth:8}} },
          scales:{
            x:{ grid:{color:'#1a2840'}, min:0 },
            y:{ grid:{display:false}, ticks:{font:{size:8}} },
            y2:{ position:'right', grid:{drawOnChartArea:false} }
          }
        }
      });
      // tabla
      document.querySelector('#t-vend tbody').innerHTML = data.map((r,i)=>{
        const pct=parseFloat(r.tasa_riesgo_pct)||0;
        return `<tr>
          <td style="color:var(--muted);font-family:var(--mono)">${i+1}</td>
          <td style="font-family:var(--mono);font-size:9px;color:var(--accent)">${r.seller_id.substring(0,14)}…</td>
          <td>${r.city||'—'}</td><td>${r.state||'—'}</td>
          <td style="font-family:var(--mono)">${fmt(r.total_pedidos)}</td>
          <td style="font-family:var(--mono);color:var(--danger)">${fmt(pct,1)}%</td>
          <td><div class="rbar-wrap"><div class="rbar-bg"><div class="rbar-fill" style="width:${Math.min(pct,100)}%"></div></div></div></td>
        </tr>`;
      }).join('');
    }
  }catch(e){ toast('VIZ4: '+e.message); }
  hideL('cc-vend'); hideL('cc-tabla');
}

// ── 6. Método de pago ────────────────────────────────────────────────────────
async function loadPagos(){
  showL('cc-pagos');
  try{
    const data = await get('/api/pago_vs_riesgo');
    if(!data.length){ hideL('cc-pagos'); return; }
    const labels = data.map(r=>r.payment_type.replace(/_/g,' ').toUpperCase());
    const tasa   = data.map(r=>parseFloat(r.tasa_riesgo_pct)||0);
    const rev    = data.map(r=>parseFloat(r.avg_review)||0);
    kill('pagos');
    charts['pagos'] = new Chart(ctx('ch-pagos'),{
      type:'bar',
      data:{ labels, datasets:[
        { label:'Tasa Riesgo %', data:tasa, backgroundColor:'rgba(255,63,80,.65)', borderRadius:4, yAxisID:'y' },
        { label:'Avg Review',    data:rev,  backgroundColor:'rgba(31,201,138,.65)', borderRadius:4, yAxisID:'y2' }
      ]},
      options:{
        responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{labels:{usePointStyle:true,padding:10,boxWidth:8}} },
        scales:{
          x:{ grid:{display:false} },
          y:{ grid:{color:'#1a2840'}, min:0,
              title:{display:true,text:'Riesgo %',color:C.danger,font:{size:9}} },
          y2:{ position:'right', grid:{drawOnChartArea:false}, min:1, max:5,
               title:{display:true,text:'Avg Review',color:C.accent3,font:{size:9}} }
        }
      }
    });
  }catch(e){ toast('VIZ5: '+e.message); }
  hideL('cc-pagos');
}

// ── 7. Cuotas vs satisfacción ────────────────────────────────────────────────
async function loadCuotas(){
  showL('cc-cuotas');
  try{
    const data = await get('/api/cuotas_satisfaccion');
    if(!data.length){ hideL('cc-cuotas'); return; }
    const labels = data.map(r=>`${r.num_cuotas}x`);
    const rev    = data.map(r=>parseFloat(r.avg_review)||0);
    const tasa   = data.map(r=>parseFloat(r.tasa_riesgo_pct)||0);
    kill('cuotas');
    charts['cuotas'] = new Chart(ctx('ch-cuotas'),{
      type:'line',
      data:{ labels, datasets:[
        { label:'Avg Review', data:rev, yAxisID:'y',
          borderColor:C.accent3, backgroundColor:'rgba(31,201,138,.08)',
          borderWidth:2.5, pointRadius:4, tension:.3, fill:true },
        { label:'Tasa Riesgo %', data:tasa, yAxisID:'y2',
          borderColor:C.danger, borderWidth:2, pointRadius:3.5,
          borderDash:[5,3], tension:.3, fill:false }
      ]},
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{mode:'index',intersect:false},
        plugins:{ legend:{labels:{usePointStyle:true,padding:10,boxWidth:8}} },
        scales:{
          x:{ grid:{color:'#1a2840'} },
          y:{ min:1, max:5, grid:{color:'#1a2840'},
              title:{display:true,text:'Avg Review',color:C.accent3,font:{size:9}} },
          y2:{ position:'right', grid:{drawOnChartArea:false}, min:0,
               title:{display:true,text:'Riesgo %',color:C.danger,font:{size:9}} }
        }
      }
    });
  }catch(e){ toast('VIZ6: '+e.message); }
  hideL('cc-cuotas');
}

// ── Refresh all ───────────────────────────────────────────────────────────────
async function refreshAll(){
  $('rbtn').textContent='↻ Cargando…';
  await Promise.all([loadKpis(),loadPipeline(),loadLinea(),loadEstados(),loadVendedores(),loadPagos(),loadCuotas()]);
  $('rbtn').textContent='↻ Actualizar';
}

// ── Init + auto-refresh 30s ───────────────────────────────────────────────────
refreshAll();
setInterval(refreshAll, 30000);

let cd=30;
setInterval(()=>{
  cd--; if(cd<=0) cd=30;
  if(cd>3) $('rbtn').textContent=`↻ Actualizar (${cd}s)`;
},1000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)