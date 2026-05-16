{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q15 — Most common transaction categories by volume and value.

  Grain: one row per (category, currency).
  
  Same currency-included rationale as mart_tx_by_channel. Categories
  filter to only completed transactions for value metrics; counts
  include all statuses to give a true picture of activity by category.

  Excludes rows where amount IS NULL or category IS NULL.
*/

with transactions_filtered as (
    select
        category,
        currency,
        status,
        amount
    from {{ ref('fct_transactions') }}
    where amount is not null
      and category is not null
)

select
    category,
    currency,
    
    count(*) as tx_count,
    count(*) filter (where status = 'completed') as completed_tx_count,
    
    sum(amount) filter (where status = 'completed') as total_value,
    avg(amount) filter (where status = 'completed') as avg_ticket

from transactions_filtered
group by category, currency
order by completed_tx_count desc, currency