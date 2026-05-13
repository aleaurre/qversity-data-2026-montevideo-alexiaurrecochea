{{
    config(
        materialized='view'
    )
}}

/*
    silver.stg_accounts
    -------------------
    Staging view over silver_staging.stg_accounts (produced by PySpark's
    flatten_accounts.py).

    Responsibilities:
      - Defensive trim on text fields (Spark already trimmed, but cheap insurance).
      - Semantic normalization of categoricals via macros:
          * status        -> normalize_account_status (casing + Spanish)
          * account_type  -> normalize_casing only (data is already clean,
                             but defensive against future generator drift)
      - currency kept as-is (ISO 4217 standard, uppercase canonical).
      - No filtering: pass through all rows from the spark output.
      - Materialized as view: downstream silver dims/facts join through this;
        no need to materialize a table that just reshapes the spark output.

    Grain: 1 row per account (account_id is unique).

    Decisions:
      - `opened_date` arrives as `date` from spark (already parsed). No
        multi-format string handling needed here, unlike dim_customer.
      - `currency` is NOT lowercased: ISO 4217 codes are canonically uppercase.
        Tested via accepted_values with the 8 codes observed in EDA.
      - `branch_code` left as text without normalization: it's an opaque
        identifier, not a categorical to enforce.
*/

with source as (

    select
        customer_id,
        account_id,
        account_type,
        currency,
        balance,
        credit_limit,
        interest_rate,
        opened_date,
        status,
        branch_code,
        bronze_id,
        load_timestamp
    from {{ source('silver_staging', 'stg_accounts') }}

),

normalized as (

    select
        -- ---------- Identity ----------
        trim(account_id)                                as account_id,
        trim(customer_id)                               as customer_id,

        -- ---------- Account attributes ----------
        {{ normalize_casing('account_type') }}          as account_type,
        upper(trim(currency))                           as currency,
        balance,
        credit_limit,
        interest_rate,
        {{ parse_date_multi_format('opened_date') }}    as opened_date,
        {{ normalize_account_status('status') }}        as status,

        -- ---------- Audit ----------
        bronze_id,
        load_timestamp

    from source

)

select * from normalized