{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q5 — Loan delinquency rate by customer segment.
  
  Grain: one row per customer_segment.
  
  Delinquency rate = customers_delinquent / customers_with_loans.
  Note: customers without loans are excluded from the denominator
  (no credit risk to evaluate). They are reported separately for context.
  
  See decisions.md Day 9 §2 and customer-level definition.
*/

with customers as (
    select *
    from {{ ref('int_customer_risk_profile') }}
)

select
    customer_segment,
    
    count(*) as customer_count,
    count(*) filter (where loans_count > 0) as customers_with_loans,
    count(*) filter (where loans_count = 0) as customers_without_loans,
    count(*) filter (where is_delinquent_customer) as delinquent_customers,
    count(*) filter (where is_default_customer)    as default_customers,
    
    -- Delinquency rate over the eligible population (customers with loans)
    count(*) filter (where is_delinquent_customer)::numeric
        / nullif(count(*) filter (where loans_count > 0), 0)
        as delinquency_rate,
    
    -- Default rate over the eligible population
    count(*) filter (where is_default_customer)::numeric
        / nullif(count(*) filter (where loans_count > 0), 0)
        as default_rate

from customers
group by customer_segment
order by delinquency_rate desc