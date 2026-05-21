{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  mart_top_merchants
  ------------------
  Grain: 1 row per (merchant, currency).

  Answers: Q22 — Top merchants by transaction volume / value.

  Sources:
    - silver.fct_transactions : completed transactions only

  Currency handling: multi-currency, NO FX conversion at this layer.
  Each merchant × currency combination is a separate row, consistent
  with the wider Gold layer pattern (mart_tx_by_channel, mart_tx_by_category).
  Dashboard consumers filter by currency or aggregate within currency.
  See decisions.md §1 on currency strategy.

  Status filter: only `completed` transactions count toward merchant
  performance. Failed / reversed transactions are excluded — they
  represent attempted volume, not realized merchant revenue.

  Category enrichment: each merchant gets a `top_category` column
  representing the most frequent transaction category for that merchant.
  This adds qualitative context to the ranking without expanding the
  grain (e.g., "Uber Eats — restaurants — 1234 tx").

  Null safety: merchant IS NOT NULL guard. Transactions with NULL
  merchant (typically internal transfers, account fees, interest
  charges) are excluded from the merchant ranking by design.
*/

with completed_tx as (

    select
        merchant,
        currency,
        category,
        amount
    from {{ ref('fct_transactions') }}
    where status = 'completed'
      and merchant is not null

),

category_per_merchant as (

    -- Most frequent category per (merchant, currency).
    -- Tiebreaker: alphabetical category name, deterministic across runs.
    select
        merchant,
        currency,
        category as top_category,
        row_number() over (
            partition by merchant, currency
            order by count(*) desc, category asc
        ) as rn
    from completed_tx
    group by merchant, currency, category

),

aggregated as (

    select
        merchant,
        currency,
        count(*)                                    as tx_count,
        round(sum(amount)::numeric, 2)              as total_value,
        round(avg(amount)::numeric, 2)              as avg_ticket,
        round(min(amount)::numeric, 2)              as min_ticket,
        round(max(amount)::numeric, 2)              as max_ticket
    from completed_tx
    group by merchant, currency

),

merchant_with_category as (

    select
        a.merchant,
        a.currency,
        c.top_category,
        a.tx_count,
        a.total_value,
        a.avg_ticket,
        a.min_ticket,
        a.max_ticket
    from aggregated a
    left join category_per_merchant c
        on c.merchant = a.merchant
        and c.currency = a.currency
        and c.rn = 1

)

select
    merchant,
    currency,
    top_category,
    tx_count,
    total_value,
    avg_ticket,
    min_ticket,
    max_ticket,
    -- Rank within currency, so top 10 in USD isn't drowned by COP volumes
    row_number() over (
        partition by currency
        order by total_value desc
    ) as rank_by_value_within_currency,
    row_number() over (
        partition by currency
        order by tx_count desc
    ) as rank_by_count_within_currency
from merchant_with_category
order by currency, rank_by_value_within_currency