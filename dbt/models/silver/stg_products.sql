-- models/silver/stg_products.sql
{{ config(materialized='view') }}

SELECT
    md5(p.product_id)                                    AS product_sk,
    p.product_id,
    -- Imputar categoría nula
    COALESCE(t.product_category_name_english,
             p.product_category_name,
             'sin_categoria')                            AS category_en,
    p.product_category_name                              AS category_pt,
    p.product_name_lenght                                AS name_length,
    p.product_description_lenght                         AS description_length,
    p.product_photos_qty                                 AS photos_qty,
    p.product_weight_g                                   AS weight_g,
    p.product_length_cm                                  AS length_cm,
    p.product_height_cm                                  AS height_cm,
    p.product_width_cm                                   AS width_cm,
    p.ingestion_timestamp

FROM {{ source('public', 'raw_products') }} p
LEFT JOIN {{ source('public', 'raw_product_category_translation') }} t
       ON p.product_category_name = t.product_category_name