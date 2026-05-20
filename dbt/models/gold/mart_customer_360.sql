{{
    config(
        materialized='table'
    )
}}

/*
    mart_customer_360
    -----------------
    Grain: 1 row per customer.
    Answers: Q1 (avg revenue per customer by segment - rollup),
             Q9 (risk score buckets), Q10 (count by country/city),
             Q11 (age by segment), Q13 (status breakdown),
             Q14 (KYC distribution), Q24 (avg products per segment).

    Sources (all LEFT JOIN from dim_customer so no customer is dropped):
      - silver.dim_customer            : identity, demographics, segment
      - silver.dim_credit_info         : credit_score, utilization, etc.
      - silver.dim_digital_engagement  : mobile/web registration, channel
      - silver.agg_customer_activity   : pre-aggregated product counts (Q24)
      - silver.fct_transactions        : fee revenue (completed only)
      - silver.fct_loans               : interest income (active loans)

    Revenue definition (per decisions.md, Day 8 + Day 9 + Day 11 corrections):
      - total_fees_paid_lifetime = SUM(amount) where transaction_type='fee'
        AND status='completed'. Other statuses (failed/pending/reversed)
        represent ~75% of fee rows in the synthetic dataset but are NOT
        realized revenue.
      - monthly_fee_revenue = total_fees_paid_lifetime / GREATEST(tenure_months, 3).
        Day 9 fix: temporal consistency (lifetime fees -> monthly).
        Day 11 fix: clamp denominator to >=3 months. Customers with very
        short tenure (1-3 months) produced unrealistic monthly rates
        (e.g., $206M/mo for a customer with tenure=1) because lifetime
        fees were divided by near-zero. The clamp treats <3-month tenures
        as if they had 3 months of accrual, stabilizing the metric.
      - monthly_interest_income = SUM(outstanding_balance * interest_rate_decimal / 12)
        over loans with status IN ('current', 'delinquent'). Default and
        paid_off loans accrue no interest. interest_rate_decimal is on 0-1
        scale (Day 9 fix). See decisions.md Day 9.
        Note: residual outliers exist due to extreme outstanding_balance
        values in source data (~452 loans > $100M). Source data issue,
        instrumented as warn-level test on silver.fct_loans. See decisions.md
        Day 11.
      - total_revenue_monthly = monthly_fee_revenue + monthly_interest_income.

    Bucket macros applied:
      - age_bucket, tenure_bucket: already materialized in dim_customer (Silver).
      - risk_bucket, credit_score_bucket, utilization_bucket: applied here
        via get_* macros. NULL inputs -> NULL bucket (filtered downstream).

    Null safety:
      - WHERE c.risk_score IS NOT NULL guard kept defensively (current data
        has no NULLs but protects future loads).
      - Customers without credit_info / digital_engagement / loans / transactions
        get NULL metrics, NOT excluded. LEFT JOIN preserves the full population
        for Q10/Q13/Q14 even when credit/digital data is missing.
      - 38 customers (~0.8%) have tenure_months=0. Their monthly_fee_revenue
        is computed as fees / 3 via the Day 11 clamp, attributing a stabilized
        rate rather than dividing by zero.
*/

with base_customer as (

    select
        customer_id,
        country,
        city,
        gender,
        age,
        age_bucket,
        customer_segment,
        tenure_months,
        tenure_bucket,
        kyc_status,
        status,
        risk_score
    from {{ ref('dim_customer') }}
    where risk_score is not null

),

credit as (

    select
        customer_id,
        credit_score,
        utilization_pct,
        total_limit,
        total_used,
        late_payments_12m,
        bankruptcy_flag
    from {{ ref('dim_credit_info') }}

),

digital as (

    select
        customer_id,
        mobile_app_registered,
        web_banking_registered
    from {{ ref('dim_digital_engagement') }}

),

activity as (

    select
        customer_id,
        accounts_count,
        loans_count,
        transactions_count,
        total_products
    from {{ ref('agg_customer_activity') }}

),

fees_by_customer as (

    -- Lifetime cumulative fee revenue: only completed fees count.
    -- Will be normalized to monthly in final CTE using tenure_months.
    -- See decisions.md Day 9 on temporal consistency.
    select
        a.customer_id,
        round(sum(t.amount)::numeric, 2) as total_fees_paid_lifetime
    from {{ ref('fct_transactions') }} as t
    inner join {{ ref('dim_account') }} as a
        on t.account_id = a.account_id
    where t.transaction_type = 'fee'
      and t.status = 'completed'
    group by a.customer_id

),

interest_by_customer as (

    -- Monthly interest income accrued on active loans.
    -- Default and paid_off loans accrue no interest (status filter).
    -- Day 9 fix: uses interest_rate_decimal (0-1 scale), not interest_rate
    -- (percent scale). See decisions.md Day 9.
    select
        customer_id,
        round(sum(outstanding_balance * interest_rate_decimal / 12)::numeric, 2)
            as monthly_interest_income
    from {{ ref('fct_loans') }}
    where status in ('current', 'delinquent')
      and outstanding_balance is not null
      and interest_rate_decimal is not null
    group by customer_id

),

final as (

    select
        -- ---------- Identity ----------
        c.customer_id,

        -- ---------- Demographics ----------
        c.country,
        c.city,
        c.gender,
        c.age,
        c.age_bucket,

        -- ---------- Relationship ----------
        c.customer_segment,
        c.tenure_months,
        c.tenure_bucket,
        c.kyc_status,
        c.status,

        -- ---------- Risk ----------
        c.risk_score,
        {{ get_risk_bucket('c.risk_score') }} as risk_bucket,

        -- ---------- Credit profile ----------
        cr.credit_score,
        {{ get_credit_score_bucket('cr.credit_score') }} as credit_score_bucket,
        cr.utilization_pct,
        {{ get_utilization_bucket('cr.utilization_pct') }} as utilization_bucket,
        cr.total_limit,
        cr.total_used,
        cr.late_payments_12m,
        cr.bankruptcy_flag,

        -- ---------- Digital engagement ----------
        d.mobile_app_registered,
        d.web_banking_registered,

        -- ---------- Products (from agg) ----------
        coalesce(act.accounts_count, 0)     as accounts_count,
        coalesce(act.loans_count, 0)        as loans_count,
        coalesce(act.transactions_count, 0) as transactions_count,
        coalesce(act.total_products, 0)     as total_products,

        -- ---------- Revenue (per decisions.md Day 9 + Day 11) ----------
        -- Lifetime cumulative fees, kept for auditability and reconciliation.
        coalesce(f.total_fees_paid_lifetime, 0) as total_fees_paid_lifetime,

        -- Monthly fee revenue = lifetime fees / GREATEST(tenure_months, 3).
        -- Clamp at 3 months prevents division-by-near-zero blowups for
        -- customers with tenure 1-2 months. See decisions.md Day 11.
        round(
            (coalesce(f.total_fees_paid_lifetime, 0) /
             greatest(c.tenure_months, 3))::numeric,
            2
        ) as monthly_fee_revenue,

        coalesce(i.monthly_interest_income, 0) as monthly_interest_income,

        -- Total monthly revenue: temporally consistent (both components monthly).
        -- Fee component uses same clamp as monthly_fee_revenue above.
        round(
            (coalesce(f.total_fees_paid_lifetime, 0) /
             greatest(c.tenure_months, 3))::numeric,
            2
        ) + coalesce(i.monthly_interest_income, 0) as total_revenue_monthly

    from base_customer       as c
    left join credit                as cr  on c.customer_id = cr.customer_id
    left join digital               as d   on c.customer_id = d.customer_id
    left join activity              as act on c.customer_id = act.customer_id
    left join fees_by_customer      as f   on c.customer_id = f.customer_id
    left join interest_by_customer  as i   on c.customer_id = i.customer_id

)

select * from final