{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q6 — Credit score distribution by country.
  
  Grain: one row per (country, credit_score_bucket).
  
  Uses FICO bucketing from decisions.md Day 9 §5, including '00 - Invalid'
  for out-of-range scores and '99 - Unknown' for NULL. The bucket_share
  column gives the within-country proportion for direct visualization.
*/

with customers as (
    select *
    from {{ ref('int_customer_risk_profile') }}
)

select
    country,
    credit_score_bucket,
    
    count(*) as customer_count,
    avg(credit_score) as avg_credit_score,
    
    -- Share within country: sums to 1.0 across all buckets for each country
    count(*)::numeric 
        / sum(count(*)) over (partition by country)
        as bucket_share_within_country

from customers
group by country, credit_score_bucket
order by country, credit_score_bucket