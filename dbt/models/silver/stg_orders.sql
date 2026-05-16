-- models/silver/stg_orders.sql
{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp                    AS purchase_ts,
    order_approved_at::timestamp                          AS approved_ts,
    order_delivered_carrier_date::timestamp               AS carrier_ts,
    order_delivered_customer_date::timestamp              AS delivered_ts,
    order_estimated_delivery_date::timestamp              AS estimated_delivery_ts,

    -- Días de atraso: positivo = atrasado, negativo = adelantado
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
        THEN EXTRACT(DAY FROM (
            order_delivered_customer_date::timestamp -
            order_estimated_delivery_date::timestamp
        ))::integer
        ELSE NULL
    END AS dias_atraso,

    ingestion_timestamp

FROM {{ source('public', 'raw_orders') }}
WHERE order_id IS NOT NULL