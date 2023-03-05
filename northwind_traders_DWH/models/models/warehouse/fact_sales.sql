WITH source AS(
    SELECT
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        date(o.order_date) AS order_date,
        o.shipped_date,
        o.paid_date,
        current_timestamp() AS insertion_timestamp,
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_order_details') }} od
    ON od.order_id = o.id
    WHERE od.order_id IS NOT NULL
),

deduplicated_source AS (
    SELECT *,

            CASE WHEN (ROW_NUMBER() OVER(PARTITION BY product_id)) = 1 THEN "false" ELSE "true" END as duplicate

    FROM source
)


SELECT *
EXCEPT 
    (duplicate)
FROM deduplicated_source
WHERE duplicate = "false"

