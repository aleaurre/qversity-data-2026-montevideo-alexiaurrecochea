{{
    config(
        materialized='view'
    )
}}

/*
    silver.stg_loans
    ----------------
    Staging view over silver_raw.stg_loans (produced by PySpark's
    flatten_loans.py).

    Responsibilities:
      - Defensive trim on text fields.
      - Date parsing via parse_date_multi_format for start_date and end_date.
      - Semantic normalization of categoricals via macros:
          * loan_type        -> normalize_loan_type (casing only)
          * status           -> normalize_loan_status (casing only)
          * collateral_type  -> normalize_collateral_type (casing + NULL-eo
                                of missing markers, preserving 'none' which
                                is a meaningful canonical value)
      - currency kept as-is (ISO 4217 uppercase canonical).
      - Numerics (principal, outstanding_balance, interest_rate,
        monthly_payment, term_months, days_past_due) pass through;
        financial-grade casts done in downstream gold marts.

    Grain: 1 row per loan (loan_id is unique within latest load).
    Customers without loans do NOT appear here — fact-table semantics
    (per the comment in flatten_loans.py).
*/

with source as (

    select
        customer_id,
        loan_id,
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
        bronze_id,
        load_timestamp
    from {{ source('silver_staging', 'stg_loans') }}

),

normalized as (

    select
        -- ---------- Identity ----------
        trim(loan_id)                                          as loan_id,
        trim(customer_id)                                      as customer_id,

        -- ---------- Loan attributes ----------
        {{ normalize_loan_type('loan_type') }}                 as loan_type,
        upper(trim(currency))                                  as currency,
        principal,
        outstanding_balance,
        interest_rate,
        term_months,
        monthly_payment,
        {{ parse_date_multi_format('start_date') }}            as start_date,
        {{ parse_date_multi_format('end_date') }}              as end_date,
        {{ normalize_loan_status('status') }}                  as status,
        days_past_due,
        {{ normalize_collateral_type('collateral_type') }}     as collateral_type,

        -- ---------- Audit ----------
        bronze_id,
        load_timestamp

    from source

)

select * from normalized