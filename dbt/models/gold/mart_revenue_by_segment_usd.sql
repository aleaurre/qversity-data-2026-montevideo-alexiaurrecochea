{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  Q1 — Average revenue per customer by segment (USD).

  Revenue definition (see decisions.md §1 + Day 9 + Day 11 corrections):
    revenue_monthly = monthly_fee_revenue + monthly_interest_income
      monthly_fee_revenue     = SUM(fees_lifetime) / GREATEST(tenure_months, 3)
                                where status = 'completed'
      monthly_interest_income = SUM(outstanding_balance * interest_rate_decimal / 12)
                                where loan status IN ('current', 'delinquent')

  All amounts converted to USD using silver.fx_rates (May 2026 snapshot).

  Customers with no fees and no active loans contribute 0 revenue, not NULL,
  so they correctly pull down the segment average. Customers with very
  short tenure (1-3 months) receive a clamped denominator to prevent
  the metric from blowing up (e.g., lifetime fees of $200M ÷ 1 month).

  Fixes documented in decisions.md:
    - Day 9: interest_rate scale (used decimal version)
    - Day 9: temporal consistency (fees normalized to monthly via tenure)
    - Day 11: GREATEST(tenure_months, 3) clamp prevents short-tenure blowups
*/

with

fx as (
    select currency, rate_to_usd
    from {{ ref('fx_rates') }}
),

fee_revenue_lifetime_per_customer as (
    -- Lifetime cumulative fees in USD. Will be normalized to monthly
    -- in revenue_per_customer using tenure_months for temporal consistency
    -- with interest_revenue. See decisions.md Day 9.
    select
        t.customer_id,
        sum(t.amount * fx.rate_to_usd) as fee_revenue_lifetime_usd
    from {{ ref('fct_transactions') }} t
    left join fx on fx.currency = t.currency
    where t.transaction_type = 'fee'
      and t.status = 'completed'
    group by t.customer_id
),

interest_revenue_per_customer as (
    -- Monthly interest income in USD. Uses interest_rate_decimal (0-1 scale).
    -- See decisions.md Day 9 on the scale correction.
    select
        l.customer_id,
        sum(
            l.outstanding_balance * l.interest_rate_decimal / 12.0 * fx.rate_to_usd
        ) as interest_revenue_usd
    from {{ ref('fct_loans') }} l
    left join fx on fx.currency = l.currency
    where l.status in ('current', 'delinquent')
    group by l.customer_id
),

revenue_per_customer as (
    select
        c.customer_id,
        c.customer_segment,
        -- Monthly fee revenue = lifetime fees / GREATEST(tenure_months, 3) in USD.
        -- COALESCE handles customers without fees (LEFT JOIN nulls).
        -- GREATEST clamp prevents short-tenure blowups. See decisions.md Day 11.
        coalesce(
            fee.fee_revenue_lifetime_usd / greatest(c.tenure_months, 3),
            0
        ) as monthly_fee_revenue_usd,
        coalesce(interest.interest_revenue_usd, 0) as monthly_interest_revenue_usd,
        coalesce(
            fee.fee_revenue_lifetime_usd / greatest(c.tenure_months, 3),
            0
        ) + coalesce(interest.interest_revenue_usd, 0) as total_revenue_usd
    from {{ ref('dim_customer') }} c
    left join fee_revenue_lifetime_per_customer fee
        on fee.customer_id = c.customer_id
    left join interest_revenue_per_customer interest
        on interest.customer_id = c.customer_id
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