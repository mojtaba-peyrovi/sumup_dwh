with source as (
    select 
        id as transaction_id, 
        device_id, 
        product_name, 
        cast(product_sku as string) as product_sku,
        category_name as product_category_name, 
        amount, 
        status, 
        REPLACE(card_number, ' ', '')card_number, 
        cvv,
        format_date("%Y-%m-%d %H:%M:%S", PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', created_at)) as created_at,
        format_date("%Y-%m-%d %H:%M:%S", PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', happened_at)) as happened_at
    from {{ source('transaction','transaction') }}   
    
)

select * from source
where source.card_number not like '%+%' 
