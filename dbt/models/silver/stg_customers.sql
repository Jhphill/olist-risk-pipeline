-- models/silver/stg_customers.sql
{{ config(materialized='view') }}

SELECT
    -- Surrogate key con md5
    md5(customer_unique_id)                              AS customer_sk,
    customer_id,
    customer_unique_id,
    -- Normalizar ciudad: minúsculas y sin espacios extra
    LOWER(TRIM(customer_city))                           AS city,
    UPPER(TRIM(customer_state))                          AS state,
    customer_zip_code_prefix                             AS zip_code,
    ingestion_timestamp

FROM {{ source('public', 'raw_customers') }}
WHERE customer_id IS NOT NULL