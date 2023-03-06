{{ config(
  materialized='view',
  schema='reports_northwind'
) }}


WITH source AS (
    SELECT 
        order_date,
        COUNT(DISTINCT order_id) AS order_count
    FROM 
        {{ ref('obt_sales_overview') }} 
    GROUP BY 
        order_date 
    ORDER BY 
        order_count DESC
)

SELECT * FROM source