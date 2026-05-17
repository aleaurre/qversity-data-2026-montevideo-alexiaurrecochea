{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q1 — Average revenue per customer by segment (USD).

  Revenue computation is centralized in int_customer_monthly_revenue. This
  mart applies only the segment-grain aggregation on top. See that model's
  header for the full revenue definition and Day 9 corrections (interest_rate
  scale, temporal consistency).

  Customers with no fees and no active loans contribute 0 revenue (via
  COALESCE in the intermediate), so they correctly pull down the segment
  average rather than being excluded.

  Day 10 refactor: previous version duplicated the per-customer revenue
  computation that already lived (in slightly different form) in
  mart_customer_360. Both marts now consume int_customer_monthly_revenue,
  eliminating that duplication.
*/

with

revenue as (
    select
        customer_id,
        monthly_fee_revenue_usd,
        monthly_interest_revenue_usd,
        total_monthly_revenue_usd
    from {{ ref('int_customer_monthly_revenue') }}
),

revenue_per_customer as (
    select
        c.customer_id,
        c.customer_segment,
        rev.monthly_fee_revenue_usd      as monthly_fee_revenue_usd,
        rev.monthly_interest_revenue_usd as monthly_interest_revenue_usd,
        rev.total_monthly_revenue_usd    as total_revenue_usd
    from {{ ref('dim_customer') }} c
    left join revenue rev
        on rev.customer_id = c.customer_id
)

select
    customer_segment,
    count(*)                                   as customer_count,
    sum(total_revenue_usd)                     as total_revenue_usd,
    avg(total_revenue_usd)                     as avg_revenue_per_customer_usd,
    sum(monthly_fee_revenue_usd)               as total_fee_revenue_usd,
    sum(monthly_interest_revenue_usd)          as total_interest_revenue_usd,
    avg(monthly_fee_revenue_usd)               as avg_fee_revenue_per_customer_usd,
    avg(monthly_interest_revenue_usd)          as avg_interest_revenue_per_customer_usd,
    case
        when sum(total_revenue_usd) = 0 then null
        else sum(monthly_fee_revenue_usd) / nullif(sum(total_revenue_usd), 0)
    end as fee_revenue_share
from revenue_per_customer
group by customer_segment
order by avg_revenue_per_customer_usd desc