-- models/silver/stg_sellers.sql
{{ config(materialized='view') }}

SELECT
    md5(seller_id)                                       AS seller_sk,
    seller_id,
    LOWER(TRIM(seller_city))                             AS city,
    UPPER(TRIM(seller_state))                            AS state,
    seller_zip_code_prefix                               AS zip_code,
    ingestion_timestamp

FROM {{ source('public', 'raw_sellers') }}
WHERE seller_id IS NOT NULL