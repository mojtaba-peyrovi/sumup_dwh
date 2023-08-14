with source as (
    select 
        id as store_id,
        name as store_name, 
        address as store_address,
        city as store_city,
        country as store_country,
        format_date("%Y-%m-%d %H:%M:%S", PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', created_at)) as created_at,
        typology as store_typology,
        customer_id
    from {{ source('store','store') }}
)

select * from source
