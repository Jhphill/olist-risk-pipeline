-- models/silver/stg_orders.sql
-- Lee de bronze.orders (Kafka → Spark Structured Streaming)
-- Materializado como TABLE para evitar evaluación incorrecta de casts
{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(order_purchase_timestamp,''))
              IN ('NaN','nan','','null','None')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM order_purchase_timestamp)::timestamp
    END                                                   AS purchase_ts,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(order_approved_at,''))
              IN ('NaN','nan','','null','None')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM order_approved_at)::timestamp
    END                                                   AS approved_ts,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(order_delivered_carrier_date,''))
              IN ('NaN','nan','','null','None')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM order_delivered_carrier_date)::timestamp
    END                                                   AS carrier_ts,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(order_delivered_customer_date,''))
              IN ('NaN','nan','','null','None')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM order_delivered_customer_date)::timestamp
    END                                                   AS delivered_ts,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(order_estimated_delivery_date,''))
              IN ('NaN','nan','','null','None')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM order_estimated_delivery_date)::timestamp
    END                                                   AS estimated_delivery_ts,

    CASE
        WHEN TRIM(BOTH '"' FROM COALESCE(order_delivered_customer_date,''))
             NOT IN ('NaN','nan','','null','None')
         AND TRIM(BOTH '"' FROM COALESCE(order_estimated_delivery_date,''))
             NOT IN ('NaN','nan','','null','None')
        THEN EXTRACT(DAY FROM (
            TRIM(BOTH '"' FROM order_delivered_customer_date)::timestamp -
            TRIM(BOTH '"' FROM order_estimated_delivery_date)::timestamp
        ))::integer
        ELSE NULL
    END                                                   AS dias_atraso,

    ingestion_timestamp

FROM {{ source('bronze', 'orders') }}
WHERE order_id IS NOT NULL
  AND TRIM(BOTH '"' FROM COALESCE(order_id,''))
      NOT IN ('NaN','nan','','null','None')