{{ config(materialized='table') }}

SELECT
    s.seller_sk,
    s.seller_id,
    s.city,
    s.state,
    COUNT(DISTINCT oi.order_id)  AS total_ventas
FROM {{ ref('stg_sellers') }} s
LEFT JOIN {{ source('public', 'raw_order_items') }} oi
       ON s.seller_id = oi.seller_id
GROUP BY s.seller_sk, s.seller_id, s.city, s.state