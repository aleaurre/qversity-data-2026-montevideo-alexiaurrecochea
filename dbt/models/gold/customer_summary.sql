{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    gold.customer_summary
    ---------------------
    Day-5 MVP gold model. One row per customer. Used to validate the
    end-to-end pipeline and to answer the simplest business question:

        Q10 — What is the customer count by country and city?

    Grain: customer_id.

    What's here:
      * Identity + demographics from silver.dim_customer.
      * Product activity counts (accounts / transactions / loans) joined
        from the Spark-produced staging tables.

    What's intentionally NOT here:
      * Revenue, balances, delinquency, credit utilization. Those are
        separate gold marts on days 7-10 with their own grains
        (customer-month, loan, segment-month, etc.). Cramming them all
        into one wide table is the classic anti-pattern that makes BI
        slow and ambiguous.

    Why LEFT JOIN and not INNER JOIN against the staging tables:
        Not every customer must have loans — the spec says loans[] is
        0-3 per customer. Using INNER would silently drop those customers
        from gold and inflate "average loans per customer". COALESCE +
        LEFT JOIN gives a correct zero.
*/

with customers as (
    select * from {{ ref('dim_customer') }}
),

account_counts as (

    select
        customer_id,
        count(*) as accounts_count
    from {{ source('silver_staging', 'stg_accounts') }}
    group by customer_id

),

transaction_counts as (

    select
        customer_id,
        count(*) as transactions_count
    from {{ source('silver_staging', 'stg_transactions') }}
    group by customer_id

),

loan_counts as (

    select
        customer_id,
        count(*) as loans_count
    from {{ source('silver_staging', 'stg_loans') }}
    group by customer_id

),

final as (

    select
        -- ---------- Identity ----------
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,

        -- ---------- Demographics for Q10/Q11/Q13/Q14 ----------
        c.country,
        c.city,
        c.gender,
        c.age,
        c.age_bucket,
        c.customer_segment,
        c.status,
        c.kyc_status,
        c.risk_score,

        -- ---------- Product activity counts ----------
        -- COALESCE(...,0) so the metric "average accounts per customer"
        -- computed downstream divides by the right denominator.
        coalesce(a.accounts_count, 0)     as accounts_count,
        coalesce(t.transactions_count, 0) as transactions_count,
        coalesce(l.loans_count, 0)        as loans_count,

        -- Convenience: total products. Useful for Q24 "average products per
        -- customer by segment". Excludes transactions because those are
        -- events, not products.
        coalesce(a.accounts_count, 0) + coalesce(l.loans_count, 0)
            as total_products,

        -- Audit
        c.load_timestamp

    from customers c
    left join account_counts     a on c.customer_id = a.customer_id
    left join transaction_counts t on c.customer_id = t.customer_id
    left join loan_counts        l on c.customer_id = l.customer_id

)

select * from final