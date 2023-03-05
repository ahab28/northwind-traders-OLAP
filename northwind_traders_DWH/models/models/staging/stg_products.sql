WITH source AS (
    SELECT p.*, cast(p.supplier_ids AS INTEGER) AS supplier_id FROM {{source('northwind', 'products')}} p
)

SELECT 
    * EXCEPT (supplier_ids),
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    
 FROM source
 WHERE supplier_ids NOT LIKE "%;%"