{{ config(materialized='table') }}

SELECT DISTINCT
    TO_CHAR(purchase_ts, 'YYYYMMDD')::integer   AS date_sk,
    purchase_ts::date                            AS full_date,
    EXTRACT(YEAR    FROM purchase_ts)::integer   AS year,
    EXTRACT(MONTH   FROM purchase_ts)::integer   AS month,
    EXTRACT(QUARTER FROM purchase_ts)::integer   AS quarter,
    EXTRACT(DOW     FROM purchase_ts)::integer   AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM purchase_ts) IN (0,6) THEN 1 ELSE 0 END AS is_weekend
FROM {{ ref('stg_orders') }}
WHERE purchase_ts IS NOT NULL