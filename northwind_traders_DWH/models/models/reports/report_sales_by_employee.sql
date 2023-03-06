{{ config(
  materialized='view',
  schema='reports_northwind'
) }}


WITH source AS (
    SELECT 
        employee_id, 
        CONCAT(employee_first_name, ' ', employee_last_name) as employee_full_name,
        COUNT(DISTINCT order_id) AS order_count
    FROM 
        {{ ref('obt_sales_overview') }} 
    GROUP BY 
        employee_id, 
        employee_full_name 
    ORDER BY 
        order_count DESC
)

SELECT * FROM source