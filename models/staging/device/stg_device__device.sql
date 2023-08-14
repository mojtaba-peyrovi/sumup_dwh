with source as (
    select 
        id as device_id, 
        type as device_type, 
        store_id
    from {{ source('device','device') }}     
)

select * from source
