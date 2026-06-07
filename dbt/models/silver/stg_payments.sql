-- models/silver/stg_payments.sql
{{ config(materialized='view') }}

-- Agrupamos pagos múltiples por order_id
SELECT
    order_id,
    SUM(payment_value)                                   AS total_value,
    MAX(payment_installments)                            AS max_installments,
    -- Tipo de pago dominante (el de mayor valor)
    (ARRAY_AGG(payment_type ORDER BY payment_value DESC))[1] AS payment_type,
    COUNT(*)                                             AS num_payment_methods,

    -- Flag: pago cero en pedido (anomalía)
    CASE WHEN SUM(payment_value) = 0 THEN 1 ELSE 0 END  AS flag_pago_cero,

    MAX(ingestion_timestamp)                             AS ingestion_timestamp

FROM {{ source('public', 'raw_order_payments') }}
WHERE order_id IS NOT NULL
GROUP BY order_id