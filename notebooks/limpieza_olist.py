# =============================================================
#  PROCESAMIENTO INICIAL DEL DATASET OLIST
#  Proyecto: Pipeline de Datos en Tiempo Real – Ingeniería de Datos
#  Integrantes: Juan Felipe Caballero Flores / Luciana Sofía Coca Terrazas
#  UNIVALLE – 2026
# =============================================================
#
#  INSTRUCCIONES:
#  1. Cambia la variable BASE_PATH a la carpeta donde tienes los CSVs.
#  2. Las imágenes se guardarán en una subcarpeta "capturas/" dentro de BASE_PATH.
#  3. Ejecuta: python limpieza_olist.py
#  4. Usa las imágenes generadas en la carpeta capturas/ para el informe.
# =============================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os
import unicodedata
import warnings

warnings.filterwarnings("ignore")

# ------------------------------------------------------------------
# CONFIGURACIÓN – cambia BASE_PATH a tu ruta local
# ------------------------------------------------------------------
BASE_PATH = r"C:\Users\jhphi\Documents\proyecto_Ingenieria de datos"  # ← cambia esta ruta si tus CSVs están en otra carpeta
OUT_PATH  = os.path.join(BASE_PATH, "capturas")
os.makedirs(OUT_PATH, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 150,
    "font.family": "DejaVu Sans",
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})


def save(fig, nombre):
    path = os.path.join(OUT_PATH, nombre)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  [✓] Guardado: capturas/{nombre}")


# ------------------------------------------------------------------
# 1. CARGA DE DATOS
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 1: Carga de archivos CSV")
print("="*60)

orders      = pd.read_csv(os.path.join(BASE_PATH, "olist_orders_dataset.csv"))
customers   = pd.read_csv(os.path.join(BASE_PATH, "olist_customers_dataset.csv"))
items       = pd.read_csv(os.path.join(BASE_PATH, "olist_order_items_dataset.csv"))
payments    = pd.read_csv(os.path.join(BASE_PATH, "olist_order_payments_dataset.csv"))
reviews     = pd.read_csv(os.path.join(BASE_PATH, "olist_order_reviews_dataset.csv"))
products    = pd.read_csv(os.path.join(BASE_PATH, "olist_products_dataset.csv"))
sellers     = pd.read_csv(os.path.join(BASE_PATH, "olist_sellers_dataset.csv"))
geo         = pd.read_csv(os.path.join(BASE_PATH, "olist_geolocation_dataset.csv"))
translation = pd.read_csv(os.path.join(BASE_PATH, "product_category_name_translation.csv"))

datasets = {
    "orders": orders, "customers": customers, "items": items,
    "payments": payments, "reviews": reviews, "products": products,
    "sellers": sellers, "geo": geo, "translation": translation,
}

for name, df in datasets.items():
    print(f"  {name:12s}: {df.shape[0]:>7,} filas  x  {df.shape[1]} columnas")


# ------------------------------------------------------------------
# 2. ANÁLISIS DE VALORES NULOS  →  FIGURA 1
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 2: Análisis de valores nulos")
print("="*60)

null_summary = []
for name, df in datasets.items():
    for col in df.columns:
        n = df[col].isnull().sum()
        if n > 0:
            pct = n / len(df) * 100
            null_summary.append({"Dataset": name, "Columna": col,
                                  "Nulos": n, "Porcentaje (%)": round(pct, 2)})
            print(f"  {name}.{col}: {n:,} nulos ({pct:.1f}%)")

null_df = pd.DataFrame(null_summary)

# --- Figura 1: Heatmap de nulos ---
fig, ax = plt.subplots(figsize=(10, 4))
null_df["Label"] = null_df["Dataset"] + "\n" + null_df["Columna"]
bars = ax.barh(null_df["Label"], null_df["Porcentaje (%)"],
               color="#E07B54", edgecolor="white")
ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=9)
ax.set_xlabel("Porcentaje de valores nulos (%)")
ax.set_title("Figura 1 – Valores Nulos por Columna y Dataset")
ax.axvline(5, color="red", linestyle="--", linewidth=1, label="Umbral 5%")
ax.legend(fontsize=9)
ax.set_xlim(0, 100)
plt.tight_layout()
save(fig, "fig1_valores_nulos.png")


# ------------------------------------------------------------------
# 3. DUPLICADOS  →  FIGURA 2
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 3: Análisis de duplicados")
print("="*60)

dup_data = {name: df.duplicated().sum() for name, df in datasets.items()}
for name, n in dup_data.items():
    print(f"  {name:12s}: {n:,} duplicados")

fig, ax = plt.subplots(figsize=(9, 4))
colores = ["#E07B54" if v > 0 else "#5BA4CF" for v in dup_data.values()]
bars = ax.bar(dup_data.keys(), dup_data.values(), color=colores, edgecolor="white")
ax.bar_label(ax.containers[0], fmt=lambda x: f'{x:,.0f}', padding=3, fontsize=10)
ax.set_ylabel("Cantidad de filas duplicadas")
ax.set_title("Figura 2 – Duplicados por Dataset\n(rojo = tiene duplicados)")
ax.set_xticklabels(dup_data.keys(), rotation=20, ha="right")
plt.tight_layout()
save(fig, "fig2_duplicados.png")


# ------------------------------------------------------------------
# 4. INCONSISTENCIAS TEMPORALES EN ORDERS  →  FIGURA 3
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 4: Inconsistencias temporales en orders")
print("="*60)

date_cols = ["order_purchase_timestamp", "order_approved_at",
             "order_delivered_carrier_date", "order_delivered_customer_date",
             "order_estimated_delivery_date"]
for c in date_cols:
    orders[c] = pd.to_datetime(orders[c], errors="coerce")

# Entrega antes de compra
mask_entrega_antes = (
    orders["order_delivered_customer_date"].notna() &
    (orders["order_delivered_customer_date"] < orders["order_purchase_timestamp"])
)
n_inc_fecha = mask_entrega_antes.sum()
print(f"  Pedidos con entrega anterior a compra: {n_inc_fecha}")

# Entrega real vs estimada (días de atraso)
orders["dias_atraso"] = (
    orders["order_delivered_customer_date"] - orders["order_estimated_delivery_date"]
).dt.days
n_atrasados = (orders["dias_atraso"] > 0).sum()
n_adelantados = (orders["dias_atraso"] < 0).sum()
print(f"  Pedidos entregados con atraso:   {n_atrasados:,}")
print(f"  Pedidos entregados antes de lo estimado: {n_adelantados:,}")

fig, axes = plt.subplots(1, 2, figsize=(12, 4))

# Subplot A: estados del pedido
status_counts = orders["order_status"].value_counts()
axes[0].bar(status_counts.index, status_counts.values,
            color="#5BA4CF", edgecolor="white")
ax.bar_label(ax.containers[0], fmt=lambda x: f'{x:,.0f}', padding=3, fontsize=10)
axes[0].set_title("Distribución de estados de pedido")
axes[0].set_xticklabels(status_counts.index, rotation=30, ha="right")
axes[0].set_ylabel("Cantidad de pedidos")

# Subplot B: distribución de días de atraso (solo entregados)
delivered = orders[orders["order_status"] == "delivered"]["dias_atraso"].dropna()
delivered_clip = delivered.clip(-30, 60)
axes[1].hist(delivered_clip, bins=40, color="#F4A261", edgecolor="white")
axes[1].axvline(0, color="red", linestyle="--", linewidth=1.5, label="Fecha estimada")
axes[1].set_title("Días de atraso en entrega (pedidos delivered)")
axes[1].set_xlabel("Días (negativo = llegó antes)")
axes[1].set_ylabel("Frecuencia")
axes[1].legend()

fig.suptitle("Figura 3 – Análisis Temporal de Pedidos", fontsize=13, fontweight="bold")
plt.tight_layout()
save(fig, "fig3_inconsistencias_temporales.png")


# ------------------------------------------------------------------
# 5. OUTLIERS EN PAYMENTS  →  FIGURA 4
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 5: Outliers en payments")
print("="*60)

q1  = payments["payment_value"].quantile(0.25)
q3  = payments["payment_value"].quantile(0.75)
iqr = q3 - q1
lim_sup = q3 + 1.5 * iqr
outliers_pago = (payments["payment_value"] > lim_sup).sum()
print(f"  IQR: {iqr:.2f}  |  Límite superior: {lim_sup:.2f}")
print(f"  Outliers en payment_value (método IQR): {outliers_pago:,}")

max_inst = payments["payment_installments"].max()
out_inst = (payments["payment_installments"] > 24).sum()
print(f"  Máximo de cuotas registrado: {max_inst}")
print(f"  Registros con > 24 cuotas (inconsistentes): {out_inst}")

fig, axes = plt.subplots(1, 2, figsize=(12, 4))

# Subplot A: boxplot payment_value
clipped = payments["payment_value"].clip(0, 2000)
axes[0].boxplot(clipped, vert=True, patch_artist=True,
                boxprops=dict(facecolor="#AED6F1"),
                medianprops=dict(color="red", linewidth=2))
axes[0].set_title("Distribución de payment_value\n(recortado en 2000 BRL para visualización)")
axes[0].set_ylabel("Valor del pago (BRL)")
axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

# Subplot B: cuotas
inst_counts = payments["payment_installments"].value_counts().sort_index()
colores_inst = ["#E07B54" if i > 24 else "#5BA4CF" for i in inst_counts.index]
axes[1].bar(inst_counts.index.astype(str), inst_counts.values,
            color=colores_inst, edgecolor="white")
axes[1].set_title("Distribución de cuotas de pago\n(rojo = > 24 cuotas, inconsistente)")
axes[1].set_xlabel("Número de cuotas")
axes[1].set_ylabel("Frecuencia")
axes[1].tick_params(axis="x", rotation=45)

fig.suptitle("Figura 4 – Outliers en Pagos", fontsize=13, fontweight="bold")
plt.tight_layout()
save(fig, "fig4_outliers_pagos.png")


# ------------------------------------------------------------------
# 6. OUTLIERS EN ITEMS (price y freight_value)  →  FIGURA 5
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 6: Outliers en items")
print("="*60)

for col in ["price", "freight_value"]:
    q1_ = items[col].quantile(0.25)
    q3_ = items[col].quantile(0.75)
    iqr_ = q3_ - q1_
    lim_ = q3_ + 1.5 * iqr_
    n_out = (items[col] > lim_).sum()
    n_zero = (items[col] == 0).sum()
    print(f"  {col}: outliers = {n_out:,}  |  ceros = {n_zero:,}  |  límite IQR = {lim_:.2f}")

fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for ax, col, color, lim in zip(
    axes,
    ["price", "freight_value"],
    ["#5BA4CF", "#F4A261"],
    [1500, 150]
):
    data = items[col].clip(0, lim)
    ax.hist(data, bins=50, color=color, edgecolor="white")
    ax.set_title(f"Distribución de {col}\n(recortado en {lim} BRL)")
    ax.set_xlabel(f"{col} (BRL)")
    ax.set_ylabel("Frecuencia")

fig.suptitle("Figura 5 – Outliers en Ítems de Pedido", fontsize=13, fontweight="bold")
plt.tight_layout()
save(fig, "fig5_outliers_items.png")


# ------------------------------------------------------------------
# 7. DISTRIBUCIÓN DE review_score  →  FIGURA 6
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 7: Análisis de reseñas")
print("="*60)

score_counts = reviews["review_score"].value_counts().sort_index()
print(f"  Distribución de scores:\n{score_counts.to_string()}")
print(f"  Scores fuera de rango [1,5]: {((reviews['review_score'] < 1) | (reviews['review_score'] > 5)).sum()}")
print(f"  Nulos en review_comment_title:   {reviews['review_comment_title'].isnull().sum():,}  "
      f"({reviews['review_comment_title'].isnull().mean()*100:.1f}%)")
print(f"  Nulos en review_comment_message: {reviews['review_comment_message'].isnull().sum():,}  "
      f"({reviews['review_comment_message'].isnull().mean()*100:.1f}%)")

fig, axes = plt.subplots(1, 2, figsize=(12, 4))

colores_score = ["#E07B54" if s <= 2 else "#F4A261" if s == 3 else "#5BA4CF" for s in score_counts.index]
axes[0].bar(score_counts.index.astype(str), score_counts.values,
            color=colores_score, edgecolor="white")
axes[0].bar_label(axes[0].containers[0], fmt=lambda x: f'{x:,.0f}', padding=3, fontsize=10)
axes[0].set_title("Distribución de review_score")
axes[0].set_xlabel("Puntuación (1=muy malo, 5=excelente)")
axes[0].set_ylabel("Cantidad de reseñas")

nulos_reviews = {
    "review_comment_title\n(opcional)": reviews["review_comment_title"].isnull().mean() * 100,
    "review_comment_message\n(opcional)": reviews["review_comment_message"].isnull().mean() * 100,
}
axes[1].bar(nulos_reviews.keys(), nulos_reviews.values(),
            color=["#AED6F1", "#F9E79F"], edgecolor="gray")
axes[1].bar_label(axes[1].containers[0], fmt=lambda x: f'{x:.1f}%', padding=3, fontsize=10)
axes[1].set_ylabel("% de valores nulos")
axes[1].set_title("% Nulos en columnas opcionales\nde reseñas")
axes[1].set_ylim(0, 100)

fig.suptitle("Figura 6 – Análisis de Reseñas", fontsize=13, fontweight="bold")
plt.tight_layout()
save(fig, "fig6_resenas.png")


# ------------------------------------------------------------------
# 8. LIMPIEZA FINAL Y DATASETS LIMPIOS
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  PASO 8: Aplicando limpieza")
print("="*60)

def normalizar_ciudad(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto

# --- ORDERS ---
orders_clean = orders.copy()
antes = len(orders_clean)
mask_error_fecha = (
    orders_clean["order_delivered_customer_date"].notna() &
    (orders_clean["order_delivered_customer_date"] < orders_clean["order_purchase_timestamp"])
)
orders_clean = orders_clean[~mask_error_fecha]
print(f"  orders: eliminadas {antes - len(orders_clean)} filas con fecha de entrega < fecha de compra")
orders_clean["ingestion_timestamp"] = pd.Timestamp.now()

# --- PRODUCTS ---
products_clean = products.copy()
n_antes = products_clean["product_category_name"].isnull().sum()
products_clean["product_category_name"] = products_clean["product_category_name"].fillna("sin_categoria")
products_clean = products_clean.merge(translation, on="product_category_name", how="left")
print(f"  products: imputados {n_antes} nulos en product_category_name con 'sin_categoria'")

# --- CUSTOMERS ---
customers_clean = customers.copy()
customers_clean["customer_city"] = customers_clean["customer_city"].apply(normalizar_ciudad)
print(f"  customers: ciudades normalizadas a minúsculas sin acentos")

# --- SELLERS ---
sellers_clean = sellers.copy()
sellers_clean["seller_city"] = sellers_clean["seller_city"].apply(normalizar_ciudad)
print(f"  sellers: ciudades normalizadas")

# --- GEO ---
geo_clean = geo.drop_duplicates()
geo_clean = (
    geo_clean.groupby("geolocation_zip_code_prefix", as_index=False)
    .agg({"geolocation_lat": "mean", "geolocation_lng": "mean",
          "geolocation_city": "first", "geolocation_state": "first"})
)
print(f"  geo: reducido de {len(geo):,} a {len(geo_clean):,} filas (centroide por ZIP)")

# --- PAYMENTS ---
payments_clean = payments.copy()
n_zero = (payments_clean["payment_value"] == 0).sum()
payments_clean["flag_pago_cero"] = (payments_clean["payment_value"] == 0).astype(int)
payments_clean["flag_cuotas_anomalas"] = (payments_clean["payment_installments"] > 24).astype(int)
print(f"  payments: {n_zero} registros con payment_value = 0 marcados con flag")

# --- REVIEWS ---
reviews_clean = reviews.copy()
reviews_clean["review_comment_title"]   = reviews_clean["review_comment_title"].fillna("")
reviews_clean["review_comment_message"] = reviews_clean["review_comment_message"].fillna("")
print(f"  reviews: nulos en comentarios reemplazados por cadena vacía")


# ------------------------------------------------------------------
# 9. FIGURA 7 – RESUMEN ANTES vs DESPUÉS
# ------------------------------------------------------------------
summary = {
    "orders\n(filas eliminadas)":      [len(orders),   len(orders_clean)],
    "geo\n(filas deduplicadas)":       [len(geo),      len(geo_clean)],
    "products\nnulos imputados":       [products["product_category_name"].isnull().sum(), 0],
}

fig, axes = plt.subplots(1, 3, figsize=(13, 4))
for ax, (titulo, (antes_, despues_)) in zip(axes, summary.items()):
    ax.bar(["Antes", "Después"], [antes_, despues_],
           color=["#E07B54", "#5BA4CF"], edgecolor="white", width=0.5)
    ax.bar_label(ax.containers[0], fmt=lambda x: f'{x:,.0f}', padding=3, fontsize=10)
    ax.set_title(titulo)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

fig.suptitle("Figura 7 – Impacto de la Limpieza: Antes vs Después",
             fontsize=13, fontweight="bold")
plt.tight_layout()
save(fig, "fig7_antes_despues.png")


# ------------------------------------------------------------------
# 10. TABLA RESUMEN FINAL (impresa en consola)
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  RESUMEN FINAL DE LIMPIEZA")
print("="*60)
print(f"  {'Dataset':<12} {'Filas originales':>18} {'Filas limpias':>15} {'Diferencia':>12}")
print(f"  {'-'*60}")
pares = [
    ("orders",   orders,   orders_clean),
    ("customers",customers, customers_clean),
    ("products", products,  products_clean),
    ("payments", payments,  payments_clean),
    ("reviews",  reviews,   reviews_clean),
    ("sellers",  sellers,   sellers_clean),
    ("geo",      geo,       geo_clean),
]
for name, orig, clean in pares:
    diff = len(orig) - len(clean)
    print(f"  {name:<12} {len(orig):>18,} {len(clean):>15,} {diff:>12,}")

print("\n" + "="*60)
print("  PROCESO COMPLETADO")
print(f"  Las capturas se guardaron en: {os.path.abspath(OUT_PATH)}")
print("="*60 + "\n")
