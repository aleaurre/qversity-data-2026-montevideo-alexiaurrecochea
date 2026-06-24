{{
    config(
        materialized='table',
        unique_key='loan_id'
    )
}}

/*
    silver.dim_loan
    ---------------
    Loan dimension. One row per loan.

    Reads from stg_loans (already normalized by Spark + dbt staging).
    Adds derived attributes:
      - loan_age_months: months since start_date.
      - remaining_term_months: months between current_date and end_date.
      - principal_repaid: principal - outstanding_balance.
      - principal_repaid_pct: principal_repaid / principal * 100.

    Like dim_account, this is a narrow dimension. Customer attributes
    are NOT denormalized here; joined via customer_id in gold marts.

    Materialization: table. Joined by every gold mart that needs
    loan-level grain.

    Grain: 1 row per loan_id. Customers without loans do NOT appear.

    Note: loan_id is unique within the latest load thanks to Spark dedup
    (deduplicate_by_pk in utils.py). The unique test guards against
    regression if dedup ever fails upstream.
*/

with source as (

    select
        loan_id,
        customer_id,
        loan_type,
        currency,
        principal,
        outstanding_balance,
        interest_rate,
        term_months,
        monthly_payment,
        start_date,
        end_date,
        status,
        days_past_due,
        collateral_type,
        load_timestamp
    from {{ ref('stg_loans') }}

),

enriched as (

    select
        -- ---------- Identity ----------
        loan_id,
        customer_id,

        -- ---------- Loan attributes ----------
        loan_type,
        currency,
        principal,
        outstanding_balance,
        interest_rate,
        term_months,
        monthly_payment,
        start_date,
        end_date,
        status,
        days_past_due,
        collateral_type,

        -- ---------- Derived attributes ----------
        -- loan_age_months: months elapsed since start_date.
        -- NULL when start_date is null.
        case
            when start_date is null then null
            else cast(floor(months_between(current_date(), start_date)) as int)
        end as loan_age_months,

        -- remaining_term_months: months between now and end_date.
        -- Positive = future maturity. Negative = past maturity (likely
        -- paid_off or default). NULL when end_date is null.
        case
            when end_date is null then null
            else cast(floor(months_between(end_date, current_date())) as int)
        end as remaining_term_months,

        -- principal_repaid: how much of the principal has been paid down.
        -- NULL when either principal or outstanding_balance is NULL.
        case
            when principal is null or outstanding_balance is null then null
            else (principal - outstanding_balance)
        end as principal_repaid,

        -- principal_repaid_pct: percentage of principal paid down.
        -- NULL when principal is NULL or 0 (avoid division by zero).
        case
            when principal is null or principal = 0 then null
            when outstanding_balance is null then null
            else ((principal - outstanding_balance) / principal * 100)
        end as principal_repaid_pct,

        -- ---------- Audit ----------
        load_timestamp

    from source

)

select * from enriched