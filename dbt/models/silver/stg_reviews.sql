-- models/silver/stg_reviews.sql
{{ config(materialized='view') }}

SELECT
    review_id,
    order_id,
    review_score,
    -- Flag review negativa
    CASE WHEN review_score <= 2 THEN 1 ELSE 0 END        AS flag_review_negativa,
    -- Imputar nulos en comentarios con cadena vacía
    COALESCE(review_comment_title, '')                   AS review_title,
    COALESCE(review_comment_message, '')                 AS review_message,
    -- Flag si tiene comentario escrito
    CASE
        WHEN review_comment_message IS NOT NULL
         AND TRIM(review_comment_message) != ''
        THEN 1 ELSE 0
    END                                                  AS has_comment,
    review_creation_date::timestamp                      AS created_ts,
    review_answer_timestamp::timestamp                   AS answered_ts,
    ingestion_timestamp

FROM {{ source('public', 'raw_order_reviews') }}
WHERE order_id IS NOT NULL