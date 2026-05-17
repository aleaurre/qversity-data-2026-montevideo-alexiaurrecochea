{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q19 — International transfer patterns.
  
  Grain: one row per (origin_country, tx_currency).
  
  Definition of "international transfer" (decisions.md Day 9 §8):
    is_international = (transaction_type = 'transfer'
                        AND tx_currency != account_currency
                        AND tx_currency != customer_country_currency)
  
  The strictest of three considered definitions: a transfer is international
  only if its currency differs from BOTH the originating account's currency
  AND the customer's home country currency. This minimizes false positives
  (e.g., a Uruguayan with a USD account sending USD to another USD account
  is correctly classified as domestic).
  
  All transfers are included (not only international ones) with the 
  is_international flag aggregated as counts. This allows Power BI to:
    - Filter to only international (Q19 primary use case)
    - Compute the international share within each corridor (origin_country,
      tx_currency)
    - Show domestic vs international comparison
  
  Amounts in native currency (no FX conversion). Power BI users can filter
  by tx_currency or country to obtain currency-comparable views.
  
  Excludes rows where amount IS NULL (~3% DQ documented in decisions.md
  Day 6) or where any join key is missing.
*/

with transfers as (
    select
        t.transaction_id,
        t.customer_id,
        t.account_id,
        t.currency       as tx_currency,
        t.amount,
        t.status
    from {{ ref('fct_transactions') }} t
    where t.transaction_type = 'transfer'
      and t.amount is not null
),

enriched as (
    select
        tr.transaction_id,
        tr.amount,
        tr.status,
        tr.tx_currency,
        a.currency       as account_currency,
        c.country        as origin_country,
        cc.currency_code as customer_country_currency,
        
        -- The classifier per decisions.md Day 9 §8
        case 
            when tr.tx_currency != a.currency
             and tr.tx_currency != cc.currency_code 
            then true 
            else false 
        end as is_international
    
    from transfers tr
    inner join {{ ref('dim_account') }} a
        on a.account_id = tr.account_id
    inner join {{ ref('dim_customer') }} c
        on c.customer_id = tr.customer_id
    left join {{ ref('country_currency') }} cc
        on cc.country_code = c.country
)

select
    origin_country,
    tx_currency,
    
    -- Counts
    count(*) as tx_count,
    count(*) filter (where is_international) as international_tx_count,
    count(*) filter (where not is_international) as domestic_tx_count,
    count(*) filter (where status = 'completed') as completed_tx_count,
    count(*) filter (where is_international and status = 'completed')
        as completed_international_tx_count,
    
    -- Share of international within this (country, currency) corridor
    count(*) filter (where is_international)::numeric
        / nullif(count(*), 0) as international_share,
    
    -- Value metrics (completed only, in native tx_currency)
    sum(amount) filter (where status = 'completed') as total_value,
    sum(amount) filter (where is_international and status = 'completed')
        as total_international_value,
    avg(amount) filter (where status = 'completed') as avg_ticket,
    avg(amount) filter (where is_international and status = 'completed')
        as avg_international_ticket

from enriched
group by origin_country, tx_currency
order by origin_country, tx_currency