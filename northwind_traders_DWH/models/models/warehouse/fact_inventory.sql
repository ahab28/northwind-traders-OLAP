with source AS (
    SELECT 
            i.*,
            i.id AS inventory_id,
            CURRENT_TIMESTAMP() AS insertion_timestamp,
    FROM {{ ref('stg_inventory_transactions') }} i
),

deduplicated_source AS (
    SELECT *
            EXCEPT(id),
            CASE WHEN (ROW_NUMBER() OVER(PARTITION BY inventory_id)) = 1 THEN "false" ELSE "true" END as duplicate
    FROM source
)

SELECT *
EXCEPT 
    (duplicate),
FROM deduplicated_source
WHERE duplicate = "false"

