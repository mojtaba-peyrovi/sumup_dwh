with stg_device as (
    select * from {{ ref('stg_device__device') }}
)

select * from stg_device