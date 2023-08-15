with stg_store as (
    select * from {{ ref('stg_store__store') }}
)

select 
{{ dbt_utils.generate_surrogate_key(['stg_store.store_id']) }} as store_key,
stg_store.*
from stg_store
