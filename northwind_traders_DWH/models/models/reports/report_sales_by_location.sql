{{ config(
  materialized='view',
  schema='reports_northwind'
) }}


WITH source AS (
    SELECT 
        customer_country_region,
        customer_state_province,
        customer_city,
        customer_zip_postal_code,
        COUNT(DISTINCT order_id) AS order_count
    FROM 
        {{ ref('obt_sales_overview') }} 
    GROUP BY 
        customer_country_region,
        customer_state_province,
        customer_city,
        customer_zip_postal_code
    ORDER BY 
        order_count DESC
)

SELECT * FROM source