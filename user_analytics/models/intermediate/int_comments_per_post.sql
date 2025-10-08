{{ config(materialized='table') }}

SELECT
    post_id,
    COUNT(comment_id) AS number_of_comments
FROM {{ ref('stg_comments') }} -- Reference the clean staging comments data
GROUP BY 1
