-- models/silver/scd_sellers.sql
-- SCD Tipo 2 para vendedores
-- Simula cambios históricos usando variaciones en los datos raw

{{ config(materialized='table') }}

WITH sellers_raw AS (
    SELECT
        seller_id,
        LOWER(TRIM(seller_city))             AS city,
        UPPER(TRIM(seller_state))            AS state,
        seller_zip_code_prefix               AS zip_code,
        ingestion_timestamp
    FROM {{ source('public', 'raw_sellers') }}
),

-- Simulamos versiones históricas asignando
-- valid_from basado en ingestion_timestamp
sellers_con_version AS (
    SELECT
        seller_id,
        city,
        state,
        zip_code,
        ingestion_timestamp                  AS valid_from,
        -- valid_to = siguiente ingestion o fecha infinita si es el último
        LEAD(ingestion_timestamp) OVER (
            PARTITION BY seller_id
            ORDER BY ingestion_timestamp
        )                                    AS valid_to_raw
    FROM sellers_raw
),

sellers_scd AS (
    SELECT
        -- Surrogate key única por versión
        md5(seller_id || valid_from::text)   AS seller_sk,
        seller_id,
        city,
        state,
        zip_code,
        valid_from,
        -- Si no hay versión posterior, la fila es vigente hasta el "infinito"
        COALESCE(
            valid_to_raw,
            '9999-12-31'::timestamp
        )                                    AS valid_to,
        -- Flag de registro vigente
        CASE
            WHEN valid_to_raw IS NULL THEN true
            ELSE false
        END                                  AS is_current
    FROM sellers_con_version
)

SELECT * FROM sellers_scd