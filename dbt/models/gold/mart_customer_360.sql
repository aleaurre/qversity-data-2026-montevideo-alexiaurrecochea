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
      - silver.int_customer_monthly_revenue : pre-computed revenue metrics
                                              (Day 10 refactor — see decisions.md)

    Revenue: previously defined inline in this mart, now centralized in
    int_customer_monthly_revenue. Both this mart and mart_revenue_by_segment_usd
    consume the intermediate; see that model's header for the full definition
    and the Day 9 corrections (interest_rate scale, temporal consistency).

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
      - Zero-tenure customers (~0.9 %, 45 of 5,000) receive monthly_fee_revenue=0
        from int_customer_monthly_revenue (COALESCE there); see decisions.md Day 9.
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

revenue as (

    -- Centralized revenue computation. See int_customer_monthly_revenue
    -- for the full definition and rationale.
    select
        customer_id,
        total_fees_paid_lifetime_native as total_fees_paid_lifetime,
        monthly_fee_revenue_native       as monthly_fee_revenue,
        monthly_interest_revenue_native  as monthly_interest_income,
        total_monthly_revenue_native     as total_revenue_monthly
    from {{ ref('int_customer_monthly_revenue') }}

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

        -- ---------- Revenue (from int_customer_monthly_revenue) ----------
        coalesce(rev.total_fees_paid_lifetime, 0) as total_fees_paid_lifetime,
        coalesce(rev.monthly_fee_revenue, 0)      as monthly_fee_revenue,
        coalesce(rev.monthly_interest_income, 0)  as monthly_interest_income,
        coalesce(rev.total_revenue_monthly, 0)    as total_revenue_monthly

    from base_customer       as c
    left join credit         as cr  on c.customer_id = cr.customer_id
    left join digital        as d   on c.customer_id = d.customer_id
    left join activity       as act on c.customer_id = act.customer_id
    left join revenue        as rev on c.customer_id = rev.customer_id

)

select * from final