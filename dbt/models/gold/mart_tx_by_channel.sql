{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q3 — Revenue breakdown by transaction channel.
  Q17 — Average transaction size by channel.
  Q18 — Failed transaction rate by channel.

  Grain: one row per (channel, currency).
  
  Currency is included because a single client can transact in multiple
  currencies (e.g., AR client with both ARS and USD accounts). Aggregating
  amounts across currencies without conversion is misleading; this grain
  keeps amounts comparable within each currency. Power BI users can filter
  by currency or country to drill down.

  Metrics:
    - tx_count: all transactions regardless of status (denominator for failed_rate)
    - completed_tx_count / failed_tx_count: explicit counts by status
    - total_value, avg_ticket: only over completed transactions (realized)
    - failed_rate: failed_tx_count / NULLIF(tx_count, 0)

  Excludes rows where amount IS NULL (~3% DQ documented in decisions.md day 6).
*/

with transactions_filtered as (
    select
        channel,
        currency,
        status,
        amount,
        is_failed
    from {{ ref('fct_transactions') }}
    where amount is not null
)

select
    channel,
    currency,
    
    count(*) as tx_count,
    count(*) filter (where status = 'completed') as completed_tx_count,
    count(*) filter (where is_failed = true)     as failed_tx_count,
    
    sum(amount)  filter (where status = 'completed') as total_value,
    avg(amount)  filter (where status = 'completed') as avg_ticket,
    
    count(*) filter (where is_failed = true)::numeric
        / nullif(count(*), 0) as failed_rate

from transactions_filtered
group by channel, currency
order by channel, currency