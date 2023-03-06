{{ config(
  materialized='view',
  schema='reports_northwind'
) }}


WITH source AS (
    SELECT 
        product_id, 
        product_name, 
        COUNT(DISTINCT order_id) AS order_count
    FROM 
        {{ ref('obt_sales_overview') }} 
    GROUP BY 
        product_id, 
        product_name 
    ORDER BY 
        order_count DESC
)

SELECT * FROM source