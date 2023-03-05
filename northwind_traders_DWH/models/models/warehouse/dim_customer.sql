WITH source AS (
    SELECT
        id AS customer_id,
        company,
        last_name,
        first_name,
        email_address,
        job_title,
        business_phone,
        home_phone,
        mobile_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        web_page,
        notes,
        attachments,
        CURRENT_TIMESTAMP() AS insertion_timestamp,
    FROM {{ ref('stg_customer') }}
),
deduplicated_source AS (
    SELECT *,
            CASE WHEN (ROW_NUMBER() OVER(PARTITION BY customer_id)) = 1 THEN "false" ELSE "true" END as duplicate
    FROM source
)
SELECT *
EXCEPT 
    (duplicate),
FROM deduplicated_source
WHERE duplicate = "false"