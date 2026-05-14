{{
    config(
        materialized='table',
        unique_key='account_id'
    )
}}

/*
    silver.dim_account
    ------------------
    Account dimension. One row per account.

    Reads from stg_accounts (already normalized by Spark + dbt staging).
    Adds derived attributes:
      - account_age_months: months since opened_date.

    Does NOT denormalize customer attributes (segment, country, etc.).
    Those are joined in gold marts via customer_id. Keeping the
    dimension narrow honors classical Kimball: dims are reused across
    facts, and denormalization happens at the mart level where the
    grain and use case are known.

    Materialization: table. Joined by every downstream gold mart that
    needs account-level grain.

    Grain: 1 row per account_id.
*/

with source as (

    select
        account_id,
        customer_id,
        account_type,
        currency,
        balance,
        credit_limit,
        interest_rate,
        opened_date,
        status,
        branch_code,
        load_timestamp
    from {{ ref('stg_accounts') }}

),

enriched as (

    select
        -- ---------- Identity ----------
        account_id,
        customer_id,

        -- ---------- Account attributes ----------
        account_type,
        currency,
        balance,
        credit_limit,
        interest_rate,
        opened_date,
        status,
        branch_code,

        -- ---------- Derived attributes ----------
        -- account_age_months: months elapsed since opened_date.
        -- NULL when opened_date is NULL (unparseable in bronze).
        case
            when opened_date is null then null
            else (
                extract(year  from age(current_date, opened_date)) * 12
              + extract(month from age(current_date, opened_date))
            )::int
        end as account_age_months,

        -- ---------- Audit ----------
        load_timestamp

    from source

)

select * from enriched