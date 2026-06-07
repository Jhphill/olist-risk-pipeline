-- models/silver/stg_reviews.sql
-- Lee de bronze.reviews (Kafka → Spark Structured Streaming)
-- Materializado como TABLE para evitar que PostgreSQL evalúe
-- los casts en orden incorrecto al hacer JOIN en fct_orders
{{ config(materialized='table') }}

SELECT
    review_id,
    order_id,
    review_score,
    CASE WHEN review_score <= 2 THEN 1 ELSE 0 END         AS flag_review_negativa,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(review_comment_title,''))
              IN ('NaN','nan','None','null','')
         THEN ''
         ELSE TRIM(BOTH '"' FROM review_comment_title)
    END                                                   AS review_title,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(review_comment_message,''))
              IN ('NaN','nan','None','null','')
         THEN ''
         ELSE TRIM(BOTH '"' FROM review_comment_message)
    END                                                   AS review_message,

    CASE
        WHEN review_comment_message IS NULL THEN 0
        WHEN TRIM(BOTH '"' FROM review_comment_message)
             IN ('NaN','nan','None','null','') THEN 0
        ELSE 1
    END                                                   AS has_comment,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(review_creation_date,''))
              IN ('NaN','nan','None','null','')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM review_creation_date)::timestamp
    END                                                   AS created_ts,

    CASE WHEN TRIM(BOTH '"' FROM COALESCE(review_answer_timestamp,''))
              IN ('NaN','nan','None','null','')
         THEN NULL
         ELSE TRIM(BOTH '"' FROM review_answer_timestamp)::timestamp
    END                                                   AS answered_ts,

    ingestion_timestamp

FROM {{ source('bronze', 'reviews') }}
WHERE order_id IS NOT NULL
  AND TRIM(BOTH '"' FROM COALESCE(order_id,''))
      NOT IN ('NaN','nan','','null','None')