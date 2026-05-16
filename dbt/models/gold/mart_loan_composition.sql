{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q23 — Loan portfolio composition: outstanding balance by type and status.
  
  Grain: one row per (loan_type, status, currency).
  
  Reads from int_loan_portfolio_metrics. Includes monthly_interest_accrued
  aggregated to allow Power BI to display interest income directly per
  (type, status, currency) combination (supports Q4 partially: interest 
  income by loan type).
*/

with loans as (
    select *
    from {{ ref('int_loan_portfolio_metrics') }}
)

select
    loan_type,
    status,
    currency,
    
    count(*) as loan_count,
    sum(principal)                as total_principal,
    sum(outstanding_balance)      as total_outstanding_balance,
    avg(outstanding_balance)      as avg_outstanding_balance,
    
    -- Interest metrics (uses decimal scale, Day 9 fix)
    avg(interest_rate_decimal)    as avg_interest_rate,
    sum(monthly_interest_accrued) as total_monthly_interest_accrued,
    
    -- Delinquency flags aggregated for cross-cutting visibility
    count(*) filter (where is_delinquent) as delinquent_count,
    count(*) filter (where is_default)    as default_count,
    
    -- Internal share of total outstanding for the type within currency
    sum(outstanding_balance) / 
        nullif(sum(sum(outstanding_balance)) over (partition by loan_type, currency), 0)
        as outstanding_share_within_type_currency

from loans
group by loan_type, status, currency
order by loan_type, status, currency