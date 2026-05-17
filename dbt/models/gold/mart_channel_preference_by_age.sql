{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q21 — Channel preference by age group.
  
  Grain: one row per (age_bucket, preferred_channel).
  
  Exposes all 5 channels separately: mobile, web, atm, branch, phone.
  Power BI groups digital (mobile + web) vs non-digital (atm + branch + phone)
  visually in the dashboard, which keeps the model flexible for future
  questions that may need different groupings (e.g., remote vs in-person).
  
  bucket_share_within_age sums to 1.0 for each age_bucket, suitable
  for direct percentage display in stacked bar charts.
*/

with engagement as (
    select
        c.customer_id,
        c.age_bucket,
        d.preferred_channel
    from {{ ref('dim_customer') }} c
    left join {{ ref('dim_digital_engagement') }} d
        on d.customer_id = c.customer_id
    where d.preferred_channel is not null
)

select
    age_bucket,
    preferred_channel,
    
    count(*) as customer_count,
    
    -- Share of this channel within the age bucket
    count(*)::numeric / sum(count(*)) over (partition by age_bucket)
        as bucket_share_within_age

from engagement
group by age_bucket, preferred_channel
order by age_bucket, preferred_channel