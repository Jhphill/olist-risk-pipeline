{{ config(materialized='table') }}

SELECT DISTINCT
    md5(payment_type)           AS payment_method_sk,
    payment_type,
    CASE WHEN payment_type = 'credit_card' THEN 1 ELSE 0 END  AS es_credito,
    CASE WHEN payment_type IN ('boleto','voucher') THEN 1 ELSE 0 END AS es_efectivo
FROM {{ ref('stg_payments') }}
WHERE payment_type IS NOT NULL