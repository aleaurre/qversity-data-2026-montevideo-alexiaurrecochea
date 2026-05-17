{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q20 — Mobile app adoption rate by customer segment.

  Grain: one row per customer_segment.
  
  Adoption rate denominator: total customers in the segment (not "with credit"
  or similar restriction). Mobile/web adoption is an universal product
  question — every customer is a potential mobile user.
  
  Additional engagement metrics included for the dashboard:
    - web_adoption_rate: complement of Q20 for the web channel
    - paperless_rate, push_rate: digital engagement proxies
    - avg_monthly_logins: depth of engagement, not just adoption
*/

with engagement as (
    select
        c.customer_id,
        c.customer_segment,
        d.mobile_app_registered,
        d.web_banking_registered,
        d.push_notifications,
        d.paperless_statements,
        d.avg_monthly_logins
    from {{ ref('dim_customer') }} c
    left join {{ ref('dim_digital_engagement') }} d
        on d.customer_id = c.customer_id
)

select
    customer_segment,
    
    count(*) as customer_count,
    
    -- Q20 primary metric
    count(*) filter (where mobile_app_registered = true)::numeric
        / nullif(count(*), 0) as mobile_adoption_rate,
    
    -- Complementary digital adoption metrics
    count(*) filter (where web_banking_registered = true)::numeric
        / nullif(count(*), 0) as web_adoption_rate,
    
    count(*) filter (where mobile_app_registered = true or web_banking_registered = true)::numeric
        / nullif(count(*), 0) as any_digital_adoption_rate,
    
    count(*) filter (where push_notifications = true)::numeric
        / nullif(count(*), 0) as push_notifications_rate,
    
    count(*) filter (where paperless_statements = true)::numeric
        / nullif(count(*), 0) as paperless_rate,
    
    -- Depth of engagement
    avg(avg_monthly_logins) as avg_monthly_logins

from engagement
group by customer_segment
order by mobile_adoption_rate desc