{{ config(materialized='view') }}

-- Select data from the raw source table (RAW_DATA.COMMENTS)
WITH source_data AS (
    SELECT
        raw_json
    FROM {{ source('raw_data', 'comments') }}
),

parsed_comments AS (
    SELECT
        -- Rename postId to post_id and cast to INT
        CAST(raw_json:postId AS INT)    AS post_id,
        
        -- Rename id to comment_id and cast to INT (Primary Key for the comment)
        CAST(raw_json:id AS INT)        AS comment_id,
        
        -- Extract and cast string fields
        raw_json:name::VARCHAR          AS name,
        raw_json:email::VARCHAR         AS email,
        raw_json:body::VARCHAR          AS body,
        
        -- Add a timestamp for audit/lineage purposes
        CURRENT_TIMESTAMP()             AS loaded_at_utc
    FROM source_data
)

SELECT * FROM parsed_comments
