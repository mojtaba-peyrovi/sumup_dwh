with stg_product as (
    select 
        case 
            when RIGHT(product_name,1) = '.' 
            then SUBSTR(product_name, 1, LENGTH(product_name) - 1)
            when RIGHT(product_name,1) = ',' 
            then SUBSTR(product_name, 1, LENGTH(product_name) - 1)
            else product_name
        end as product_name, 
        
        product_sku, 

        case 
            when RIGHT(product_category_name,1) = '.' 
            then SUBSTR(product_category_name, 1, LENGTH(product_category_name) - 1)
            when RIGHT(product_category_name,1) = ',' 
            then SUBSTR(product_category_name, 1, LENGTH(product_category_name) - 1)
            else product_category_name
        end as product_category_name,
 
        happened_at

    from {{ ref('stg_transaction__transaction') }}    
),

mark_duplicates as (
    select *, 
    
    lag(product_name) over(partition by product_name order by happened_at)
    as previous_product_name

    from stg_product
),

filtered as (
    select 
        product_name, 
        product_sku,
        product_category_name
    from mark_duplicates
    where previous_product_name is null
)


select 
    {{ dbt_utils.generate_surrogate_key(['filtered.product_name']) }} as product_key,
    product_name, 
    product_sku, 
    product_category_name
from filtered