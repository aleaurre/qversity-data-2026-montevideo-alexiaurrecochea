{{
  config(
    materialized='view'
  )
}}

/*
  int_customer_monthly_revenue
  ----------------------------
  Grain: one row per customer (preserves dim_customer grain).

  Purpose: shared analytics-ready base for customer-grain revenue marts.
  Centralizes the revenue definition that previously lived (duplicated) in
  mart_customer_360 and mart_revenue_by_segment_usd. Both marts now consume
  this model and apply only their own aggregation / grain logic on top.

  Consumed by:
    - mart_customer_360         : customer-grain revenue columns (native ccy)
    - mart_revenue_by_segment_usd : segment-grain rollup (USD only)

  Business definitions applied (see decisions.md §1 and Day 9):
    - monthly_fee_revenue = SUM(amount) WHERE transaction_type='fee'
                            AND status='completed', divided by tenure_months
                            (temporal consistency fix, decisions.md Day 9).
                            COALESCE to 0 for customers with no fees AND for
                            zero-tenure customers (~0.9 %, newly registered).
    - monthly_interest_revenue = SUM(outstanding_balance * interest_rate_decimal / 12)
                                 over active loans (status in current/delinquent).
                                 Uses interest_rate_decimal (0-1 scale, Day 9 fix).
                                 Default and paid_off loans accrue no interest.
    - total_monthly_revenue = sum of the two components above.

  Fee attribution (decisions.md Day 10 §refactor):
    Fees are attributed to the customer who EXECUTED the transaction
    (t.customer_id), not the customer who OWNS the account (a.customer_id).
    Pre-refactor, mart_revenue_by_segment_usd used t.customer_id while
    mart_customer_360 used a.customer_id via JOIN to dim_account — the two
    marts produced inconsistent customer-level revenue. The refactor unifies
    on t.customer_id, matching the snapshot in business_questions.md and
    the validated Q1 output. See decisions.md Day 10 for the analysis of
    the divergence (proportion of fees with t.customer_id != a.customer_id)
    and the documented choice.

  Precision policy (decisions.md Day 10):
    - USD columns are NOT rounded here. They are consumed by
      mart_revenue_by_segment_usd via AVG over ~1,200 customers per segment.
      Rounding to 2 decimals at customer grain before AVG introduces a
      per-segment delta vs computing AVG over full-precision values and
      rounding the final result. The pre-refactor mart_revenue_by_segment_usd
      preserved precision until the segment-level rollup; this intermediate
      matches that behavior so the snapshot is reproducible exactly.
    - Native columns ARE rounded to 2 decimals. They are consumed by
      mart_customer_360 at customer grain (display, no further aggregation
      inside the mart).

  Materialized as view: ~5,000 rows, downstream marts apply further filtering
  and aggregation, no caching benefit. Aligns with int_loan_portfolio_metrics
  and int_customer_risk_profile pattern.
*/

with

fx as (
    select currency, rate_to_usd
    from {{ ref('fx_rates') }}
),

customers as (
    select customer_id, tenure_months
    from {{ ref('dim_customer') }}
),

-- -------------------------------------------------------------------------
-- Fee revenue: lifetime sum of completed fees, then normalized to monthly.
-- Attribution: t.customer_id (the customer who executed the transaction).
-- See header for the t.customer_id vs a.customer_id discussion.
-- -------------------------------------------------------------------------
fee_revenue_per_customer as (

    select
        t.customer_id,
        sum(t.amount)                       as fee_revenue_lifetime_native,
        sum(t.amount * fx.rate_to_usd)      as fee_revenue_lifetime_usd
    from {{ ref('fct_transactions') }} as t
    left join fx
        on fx.currency = t.currency
    where t.transaction_type = 'fee'
      and t.status = 'completed'
    group by t.customer_id

),

-- -------------------------------------------------------------------------
-- Interest revenue: monthly accrual on active loans.
-- -------------------------------------------------------------------------
interest_revenue_per_customer as (

    select
        l.customer_id,
        sum(l.outstanding_balance * l.interest_rate_decimal / 12.0)
            as monthly_interest_revenue_native,
        sum(l.outstanding_balance * l.interest_rate_decimal / 12.0 * fx.rate_to_usd)
            as monthly_interest_revenue_usd
    from {{ ref('fct_loans') }} as l
    left join fx
        on fx.currency = l.currency
    where l.status in ('current', 'delinquent')
      and l.outstanding_balance is not null
      and l.interest_rate_decimal is not null
    group by l.customer_id

),

final as (

    select
        c.customer_id,

        -- ---------- Lifetime fee revenue (kept for auditability) ----------
        -- Native: rounded (display-grain, single customer)
        coalesce(round(f.fee_revenue_lifetime_native, 2), 0)
            as total_fees_paid_lifetime_native,
        -- USD: full precision (downstream AVG over segment requires it)
        coalesce(f.fee_revenue_lifetime_usd, 0)
            as total_fees_paid_lifetime_usd,

        -- ---------- Monthly fee revenue (lifetime / tenure) ----------
        -- Native: rounded
        coalesce(
            round(
                (f.fee_revenue_lifetime_native / nullif(c.tenure_months, 0)),
                2
            ),
            0
        ) as monthly_fee_revenue_native,
        -- USD: full precision
        coalesce(
            f.fee_revenue_lifetime_usd / nullif(c.tenure_months, 0),
            0
        ) as monthly_fee_revenue_usd,

        -- ---------- Monthly interest revenue ----------
        -- Native: rounded
        coalesce(round(i.monthly_interest_revenue_native, 2), 0)
            as monthly_interest_revenue_native,
        -- USD: full precision
        coalesce(i.monthly_interest_revenue_usd, 0)
            as monthly_interest_revenue_usd,

        -- ---------- Total monthly revenue ----------
        -- Native: sum of rounded components
        coalesce(
            round(
                (f.fee_revenue_lifetime_native / nullif(c.tenure_months, 0)),
                2
            ),
            0
        ) + coalesce(round(i.monthly_interest_revenue_native, 2), 0)
            as total_monthly_revenue_native,
        -- USD: sum of full-precision components
        coalesce(
            f.fee_revenue_lifetime_usd / nullif(c.tenure_months, 0),
            0
        ) + coalesce(i.monthly_interest_revenue_usd, 0)
            as total_monthly_revenue_usd

    from customers as c
    left join fee_revenue_per_customer      as f on c.customer_id = f.customer_id
    left join interest_revenue_per_customer as i on c.customer_id = i.customer_id

)

select * from final
