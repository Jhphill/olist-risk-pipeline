{{ config(materialized='table') }}

SELECT
    md5(customer_unique_id)     AS customer_sk,
    customer_unique_id,
    MAX(city)                   AS city,
    MAX(state)                  AS state,
    CASE MAX(state)
        WHEN 'SP' THEN 'Sudeste' WHEN 'RJ' THEN 'Sudeste'
        WHEN 'MG' THEN 'Sudeste' WHEN 'ES' THEN 'Sudeste'
        WHEN 'PR' THEN 'Sur'     WHEN 'SC' THEN 'Sur'
        WHEN 'RS' THEN 'Sur'
        WHEN 'BA' THEN 'Nordeste' WHEN 'CE' THEN 'Nordeste'
        WHEN 'PE' THEN 'Nordeste' WHEN 'MA' THEN 'Nordeste'
        WHEN 'PB' THEN 'Nordeste' WHEN 'RN' THEN 'Nordeste'
        WHEN 'AL' THEN 'Nordeste' WHEN 'SE' THEN 'Nordeste'
        WHEN 'PI' THEN 'Nordeste'
        WHEN 'AM' THEN 'Norte' WHEN 'PA' THEN 'Norte'
        WHEN 'RO' THEN 'Norte' WHEN 'AC' THEN 'Norte'
        WHEN 'AP' THEN 'Norte' WHEN 'RR' THEN 'Norte'
        WHEN 'TO' THEN 'Norte'
        WHEN 'MT' THEN 'Centro-Oeste' WHEN 'MS' THEN 'Centro-Oeste'
        WHEN 'GO' THEN 'Centro-Oeste' WHEN 'DF' THEN 'Centro-Oeste'
        ELSE 'Otro'
    END                         AS region_brasil
FROM {{ ref('stg_customers') }}
GROUP BY md5(customer_unique_id), customer_unique_id