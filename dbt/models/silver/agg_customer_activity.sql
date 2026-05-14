{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    silver.agg_customer_activity
    ----------------------------
    Per-customer activity aggregate. One row per customer with product
    activity counts (accounts, transactions, loans).

    Originally lived at gold.customer_summary (created in a prior session
    as a day-5 MVP gold model). On reflection (day 6), its grain (1 row per
    customer) and contents (counts) make it a silver-layer aggregate, not
    a business mart. Moved to silver/agg_customer_activity with this
    refactor. Rationale: gold marts in this project have business-question-
    specific grains (customer-month, segment-month, loan, etc.); a generic
    per-customer count table fits better as silver scaffolding.

    Reads from stg_accounts / stg_transactions / stg_loans (Spark outputs
    cleaned by dbt staging), not the raw silver_raw tables. This ensures
    counts reflect the deduplicated and normalized data.

    Why LEFT JOIN and not INNER JOIN:
    Not every customer has loans (the spec says loans[] is 0-3 per
    customer). Using INNER JOIN would silently drop customers without
    loans, inflating "average loans per customer" downstream. LEFT JOIN
    + COALESCE(...,0) preserves all customers with correct zeros.

    Grain: 1 row per customer_id.

    What this is for:
      - Powers business question 24 ("average products per customer by
        segment") directly via a gold join on dim_customer.
      - Provides a useful per-customer summary view for ad-hoc analysis.

    What this is NOT for:
      - Revenue, balances, delinquency, credit utilization. Those live in
        dedicated gold marts with their own grains.
*/

with customers as (

    select customer_id from {{ ref('dim_customer') }}

),

account_counts as (

    select
        customer_id,
        count(*) as accounts_count
    from {{ ref('stg_accounts') }}
    group by customer_id

),

transaction_counts as (

    select
        customer_id,
        count(*) as transactions_count
    from {{ ref('stg_transactions') }}
    group by customer_id

),

loan_counts as (

    select
        customer_id,
        count(*) as loans_count
    from {{ ref('stg_loans') }}
    group by customer_id

),

final as (

    select
        c.customer_id,

        -- COALESCE(...,0) so "average products per customer" downstream
        -- divides by the right denominator. NULL would skew aggregations.
        coalesce(a.accounts_count, 0)     as accounts_count,
        coalesce(t.transactions_count, 0) as transactions_count,
        coalesce(l.loans_count, 0)        as loans_count,

        -- Convenience: total products. Excludes transactions (events, not
        -- products). Used for business question 24.
        coalesce(a.accounts_count, 0) + coalesce(l.loans_count, 0)
            as total_products

    from customers c
    left join account_counts     a on c.customer_id = a.customer_id
    left join transaction_counts t on c.customer_id = t.customer_id
    left join loan_counts        l on c.customer_id = l.customer_id

)

select * from final