{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q8 — Days past due distribution by loan type.
  
  Grain: one row per (loan_type, dpd_bucket, currency).
  
  Reads from int_loan_portfolio_metrics, which applies the DPD bucketing
  logic defined in decisions.md Day 9 §3.
  
  Currency is included in grain since loan amounts only make sense within
  their native currency; Power BI users can filter to USD for cross-country
  views or to any LATAM currency for in-country drill-down.
*/

with loans as (
    select *
    from {{ ref('int_loan_portfolio_metrics') }}
    where outstanding_balance is not null
)

select
    loan_type,
    dpd_bucket,
    currency,
    
    count(*) as loan_count,
    sum(outstanding_balance) as total_outstanding_balance,
    avg(outstanding_balance) as avg_outstanding_balance,
    avg(days_past_due)       as avg_days_past_due,
    
    -- Internal proportion: share of loans in this bucket vs total for the (type, currency)
    count(*)::numeric / sum(count(*)) over (partition by loan_type, currency) 
        as bucket_share_within_type_currency

from loans
group by loan_type, dpd_bucket, currency
order by loan_type, dpd_bucket, currency