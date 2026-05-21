{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  mart_revenue_monthly_by_segment_usd
  -----------------------------------
  Grain: 1 row per (year_month × customer_segment).

  Answers: Q2 — Monthly revenue trend by customer segment.
           Powers the line chart on the Revenue & Transactions
           dashboard page.

  Sources:
    - silver.fct_transactions  : completed fee transactions
    - silver.fct_loans         : active loans for interest accrual
    - silver.dim_customer      : customer_segment dimension
    - silver.fx_rates          : multi-currency conversion to USD

  Revenue definition (see decisions.md):
    monthly_fee_revenue:
      Fee transactions are dated. Fee revenue is attributed to the
      month the transaction occurred (not normalized via tenure as in
      mart_customer_360 — this mart preserves the time-series shape).
      Status filter: completed only.

    monthly_interest_income:
      Interest is NOT dated in fct_loans (no monthly schedule available).
      It is recurrent on active loans. We allocate the same monthly
      interest accrual to every month from loan start_date forward.
      This is a SIMPLIFICATION — in production we'd have a loan_schedule
      table. Documented in decisions.md Day 11.

      For the dashboard, the practical effect is a slight upward bias
      in older months (loans that may have been paid off historically
      still appear "active" because we don't have closure dates).
      Acceptable given dataset limitations.

  Currency: all values converted to USD via fx_rates snapshot.
  Cutoff: the current calendar month is excluded to avoid showing
  partial-month declines as if they were trend reversals.
  See decisions.md Day 11.

  Time range observed in source: Sept 2020 → May 2026 (current month).
  Output range after cutoff: Sept 2020 → previous calendar month.
*/

with fx as (
    select currency, rate_to_usd
    from {{ ref('fx_rates') }}
),

monthly_fee_revenue as (

    select
        date_trunc('month', t.transaction_date)::date  as revenue_month,
        c.customer_segment,
        sum(t.amount * fx.rate_to_usd)                 as fee_revenue_usd
    from {{ ref('fct_transactions') }} t
    inner join {{ ref('dim_customer') }} c
        on c.customer_id = t.customer_id
    left join fx
        on fx.currency = t.currency
    where t.transaction_type = 'fee'
      and t.status = 'completed'
      and t.transaction_date is not null
      and t.transaction_date < date_trunc('month', current_date)
    group by 1, 2

),

monthly_interest_income as (

    -- Cross join expansion: each active loan contributes its monthly
    -- interest to every month from start_date forward (capped at
    -- current month - 1). See header docstring for rationale.
    select
        m.revenue_month,
        c.customer_segment,
        sum(
            l.outstanding_balance * l.interest_rate_decimal / 12.0 * fx.rate_to_usd
        ) as interest_revenue_usd
    from {{ ref('fct_loans') }} l
    inner join {{ ref('dim_customer') }} c
        on c.customer_id = l.customer_id
    left join fx
        on fx.currency = l.currency
    cross join (
        select distinct date_trunc('month', transaction_date)::date as revenue_month
        from {{ ref('fct_transactions') }}
        where transaction_date is not null
          and transaction_date < date_trunc('month', current_date)
    ) m
    where l.status in ('current', 'delinquent')
      and l.outstanding_balance is not null
      and l.interest_rate_decimal is not null
      and m.revenue_month >= date_trunc('month', l.start_date)::date
    group by 1, 2

),

combined as (

    select
        coalesce(f.revenue_month, i.revenue_month)             as revenue_month,
        coalesce(f.customer_segment, i.customer_segment)       as customer_segment,
        coalesce(f.fee_revenue_usd, 0)                         as fee_revenue_usd,
        coalesce(i.interest_revenue_usd, 0)                    as interest_revenue_usd,
        coalesce(f.fee_revenue_usd, 0)
            + coalesce(i.interest_revenue_usd, 0)              as total_revenue_usd
    from monthly_fee_revenue f
    full outer join monthly_interest_income i
        on f.revenue_month = i.revenue_month
        and f.customer_segment = i.customer_segment

)

select
    revenue_month,
    customer_segment,
    round(fee_revenue_usd::numeric, 2)        as fee_revenue_usd,
    round(interest_revenue_usd::numeric, 2)   as interest_revenue_usd,
    round(total_revenue_usd::numeric, 2)      as total_revenue_usd
from combined
order by revenue_month, customer_segment