{{ config(
  materialized='view',
  schema='reports_northwind'
) }}


WITH source AS (
    SELECT 
        customer_id,
        CONCAT(customer_first_name, ' ', customer_last_name) as customer_full_name,
        COUNT(DISTINCT order_id) AS order_count
    FROM 
        {{ ref('obt_sales_overview') }} 
    GROUP BY 
        customer_id,
        customer_full_name
    ORDER BY 
        order_count DESC
)

SELECT * FROM source