SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code,
    customer_city as city,
    customer_state as state
FROM 
    {{ source('ecommerce', 'customers') }}