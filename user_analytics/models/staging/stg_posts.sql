{{ config(materialized='view') }}
WITH source_data AS (
    SELECT
        raw_json
    FROM {{ source('raw_data', 'posts') }}
),
parsed_posts AS (
    SELECT
        CAST(raw_json:userId AS INT)    AS user_id,
        
        CAST(raw_json:id AS INT)        AS post_id,
        
        raw_json:title::VARCHAR         AS title,
        
        raw_json:body::VARCHAR          AS body,
        
        CURRENT_TIMESTAMP()             AS loaded_at_utc
    FROM source_data
)

SELECT * FROM parsed_posts
