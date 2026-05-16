{{
  config(
    materialized='view'
  )
}}

/*
  int_loan_portfolio_metrics
  --------------------------
  Grain: one row per loan (preserves fct_loans grain, adds derived columns).
  
  Purpose: shared analytics-ready base for downstream loan portfolio marts.
  Centralizes the application of business definitions (is_delinquent, is_default,
  dpd_bucket, monthly accrued interest) so multiple marts can consume them
  without duplicating logic.
  
  Consumed by:
    - mart_loan_dpd          : aggregates by (loan_type, dpd_bucket, currency)
    - mart_loan_composition  : aggregates by (loan_type, status, currency)
  
  Business definitions applied (see decisions.md Day 9):
    - is_delinquent          = days_past_due >= 30 (collections threshold)
    - is_default             = days_past_due >= 90 OR status = 'default' (Basel/IFRS9)
    - dpd_bucket             = 6-bucket aging convention (0 / 1-29 / 30-59 / 60-89 / 90-179 / 180+)
    - monthly_interest_accrued = outstanding_balance * interest_rate_decimal / 12
                                 (only for active loans: 'current' or 'delinquent')
  
  Materialized as view because:
    - Source fct_loans is small (~7.6k rows)
    - Downstream marts apply further aggregation, no benefit from caching
    - Avoids storage duplication for purely derived columns
*/

with loans as (
    select
        loan_id,
        customer_id,
        loan_type,
        currency,
        status,
        days_past_due,
        principal,
        outstanding_balance,
        interest_rate,
        interest_rate_decimal,
        monthly_payment,
        start_date,
        end_date,
        term_months,
        collateral_type
    from {{ ref('fct_loans') }}
)

select
    -- Identity
    loan_id,
    customer_id,
    
    -- Dimensions
    loan_type,
    currency,
    status,
    collateral_type,
    
    -- Raw measures
    principal,
    outstanding_balance,
    interest_rate,
    interest_rate_decimal,
    monthly_payment,
    days_past_due,
    term_months,
    start_date,
    end_date,
    
    -- ---------- Derived: delinquency flags ----------
    -- DPD-based, status is informational only (see decisions.md Day 9 §2).
    case when days_past_due >= 30 then true else false end as is_delinquent,
    case when days_past_due >= 90 or status = 'default' then true else false end as is_default,
    
    -- ---------- Derived: DPD bucket (decisions.md Day 9 §3) ----------
    case
        when days_past_due = 0   then '00 - Current'
        when days_past_due <= 29 then '01 - Early (1-29)'
        when days_past_due <= 59 then '02 - 30-59 DPD'
        when days_past_due <= 89 then '03 - 60-89 DPD'
        when days_past_due <= 179 then '04 - 90-179 DPD'
        else '05 - 180+ DPD'
    end as dpd_bucket,
    
    -- ---------- Derived: monthly interest accrued ----------
    -- Only active loans accrue. paid_off and default loans contribute 0.
    -- Uses interest_rate_decimal (Day 9 fix on 100x scale).
    case
        when status in ('current', 'delinquent')
             and outstanding_balance is not null
             and interest_rate_decimal is not null
        then outstanding_balance * interest_rate_decimal / 12.0
        else 0
    end as monthly_interest_accrued

from loans