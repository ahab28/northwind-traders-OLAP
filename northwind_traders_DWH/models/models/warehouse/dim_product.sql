WITH source AS(
    SELECT
        p.id AS product_id,
        p.product_code,
        p.product_name,
        p.description,
        s.company AS supplier_company,
        p.standard_cost,
        p.list_price,
        p.reorder_level,
        p.target_level,
        p.quantity_per_unit,
        p.discontinued,
        p.minimum_reorder_quantity,
        p.category,
        p.attachments,
        current_timestamp() AS insertion_timestamp,
    FROM {{ ref('stg_products') }} p
    LEFT JOIN {{ ref('stg_suppliers') }} s
    ON s.id = p.supplier_id
),
deduplicated_source AS (
    SELECT *,

            CASE WHEN (ROW_NUMBER() OVER(PARTITION BY product_id)) = 1 THEN "false" ELSE "true" END as duplicate

    FROM source
)
SELECT *
EXCEPT 
    (duplicate),
FROM deduplicated_source
WHERE duplicate = "false"