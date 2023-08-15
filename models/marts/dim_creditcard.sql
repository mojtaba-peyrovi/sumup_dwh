with stg_creditcard as (
    select
        card_number,
        cvv,
        happened_at
    from {{ ref('stg_transaction__transaction') }}
),

mark_duplicates as (
    select *, 
    coalesce(
    lag(card_number) over(partition by card_number order by happened_at)
    ) as previous_cardnumber


    from stg_creditcard
),

filtered as (
    select card_number, cvv from mark_duplicates
    where previous_cardnumber is null
)

select
{{ dbt_utils.generate_surrogate_key(['filtered.card_number']) }} as card_key,

filtered.card_number, 
filtered.cvv

from filtered