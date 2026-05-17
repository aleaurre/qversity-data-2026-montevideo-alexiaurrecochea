{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q9 — Risk score buckets (low/medium/high/critical) breakdown.
  
  Grain: one row per risk_bucket.
  
  Aggregates customers by their internal risk_bucket (mapped from
  risk_score 0-100 via get_risk_bucket macro). Includes delinquency
  rate per bucket as cross-validation: do internal risk scores actually
  predict delinquency? In real data, expect strong correlation.
*/

with customers as (
    select *
    from {{ ref('int_customer_risk_profile') }}
)

select
    risk_bucket,
    
    count(*) as customer_count,
    avg(risk_score) as avg_risk_score,
    
    -- Share of total customer base in this bucket
    count(*)::numeric / sum(count(*)) over () as bucket_share,
    
    -- Cross-validation: does the risk bucket correlate with actual delinquency?
    count(*) filter (where loans_count > 0) as customers_with_loans,
    count(*) filter (where is_delinquent_customer)::numeric
        / nullif(count(*) filter (where loans_count > 0), 0)
        as delinquency_rate,
    
    count(*) filter (where is_default_customer)::numeric
        / nullif(count(*) filter (where loans_count > 0), 0)
        as default_rate,
    
    -- Credit profile averages per bucket
    avg(credit_score)    as avg_credit_score,
    avg(utilization_pct) as avg_utilization_pct

from customers
group by risk_bucket
order by risk_bucket