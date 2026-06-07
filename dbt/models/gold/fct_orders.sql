{{ config(materialized='table') }}

WITH seller_por_orden AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        seller_id
    FROM {{ source('public', 'raw_order_items') }}
    ORDER BY order_id, seller_id
),

review_por_orden AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        review_score,
        flag_review_negativa
    FROM {{ ref('stg_reviews') }}
    ORDER BY order_id, review_score ASC
),

cliente_por_orden AS (
    SELECT DISTINCT ON (o.order_id)
        o.order_id,
        c.customer_sk
    FROM {{ source('public', 'raw_orders') }} o
    JOIN {{ source('public', 'raw_customers') }} rc
      ON o.customer_id = rc.customer_id
    JOIN {{ ref('stg_customers') }} c
      ON rc.customer_unique_id = c.customer_unique_id
    ORDER BY o.order_id
)

SELECT
    o.order_id,
    cl.customer_sk,
    md5(s.seller_id)                                AS seller_sk,
    TO_CHAR(o.purchase_ts, 'YYYYMMDD')::integer     AS date_sk,
    md5(p.payment_type)                             AS payment_method_sk,
    COALESCE(p.total_value, 0)                      AS total_value,
    COALESCE(p.max_installments, 0)                 AS installments,
    o.dias_atraso,
    r.review_score,
    COALESCE(r.flag_review_negativa, 0)             AS flag_review_negativa,
    COALESCE(p.flag_pago_cero, 0)                   AS flag_pago_cero,
    o.order_status,
    CASE WHEN
        o.order_status IN ('canceled','unavailable')
        OR COALESCE(r.review_score, 5) <= 2
        OR COALESCE(o.dias_atraso, 0) > 30
        OR COALESCE(p.flag_pago_cero, 0) = 1
    THEN 1 ELSE 0 END                               AS flag_riesgo,
    o.purchase_ts,
    o.ingestion_timestamp

FROM {{ ref('stg_orders') }} o
LEFT JOIN cliente_por_orden cl
       ON o.order_id = cl.order_id
LEFT JOIN seller_por_orden s
       ON o.order_id = s.order_id
LEFT JOIN {{ ref('stg_payments') }} p
       ON o.order_id = p.order_id
LEFT JOIN review_por_orden r
       ON o.order_id = r.order_id