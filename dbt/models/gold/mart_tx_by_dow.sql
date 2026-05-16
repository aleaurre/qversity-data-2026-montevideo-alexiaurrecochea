{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q16 — Transaction volume by day of week.

  Grain: one row per (day_of_week, currency).
  
  day_of_week_name added for direct labeling in Power BI without an extra
  measure. Postgres DOW convention: Sunday=0, Saturday=6 (preserved from
  silver). The numeric column is kept for correct sorting in visuals.

  Excludes rows where amount IS NULL.
*/

with transactions_filtered as (
    select
        day_of_week,
        currency,
        status,
        amount
    from {{ ref('fct_transactions') }}
    where amount is not null
)

select
    day_of_week,
    case day_of_week
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end as day_of_week_name,
    currency,
    
    count(*) as tx_count,
    count(*) filter (where status = 'completed') as completed_tx_count,
    
    sum(amount) filter (where status = 'completed') as total_value,
    avg(amount) filter (where status = 'completed') as avg_ticket

from transactions_filtered
group by day_of_week, currency
order by day_of_week, currency