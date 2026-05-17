{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q7 — Relationship between credit utilization and delinquency.
  
  Grain: one row per utilization_bucket.
  
  Each row reports the delinquency rate for customers in that utilization
  bucket (over customers with loans). The expected pattern is a monotonic
  increase: higher utilization correlates with higher delinquency rate.
  
  If the data is realistic, the curve will be ascending. A flat or
  non-monotonic curve would suggest synthetic-data artifacts (see Day 9
  findings on synthetic data patterns).
*/

with customers as (
    select *
    from {{ ref('int_customer_risk_profile') }}
)

select
    utilization_bucket,
    
    count(*) as customer_count,
    count(*) filter (where loans_count > 0) as customers_with_loans,
    count(*) filter (where is_delinquent_customer) as delinquent_customers,
    count(*) filter (where is_default_customer)    as default_customers,
    
    -- The key metric: delinquency rate for this utilization bucket
    count(*) filter (where is_delinquent_customer)::numeric
        / nullif(count(*) filter (where loans_count > 0), 0)
        as delinquency_rate,
    
    count(*) filter (where is_default_customer)::numeric
        / nullif(count(*) filter (where loans_count > 0), 0)
        as default_rate,
    
    -- Summary stats for context in the dashboard
    avg(utilization_pct) as avg_utilization_pct,
    avg(credit_score)    as avg_credit_score

from customers
group by utilization_bucket
order by utilization_bucket