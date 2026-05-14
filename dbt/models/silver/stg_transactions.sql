{{
    config(
        materialized='view'
    )
}}

/*
    silver.stg_transactions
    -----------------------
    Staging view over silver_raw.stg_transactions (produced by PySpark's
    flatten_transactions.py).

    Responsibilities:
      - Defensive trim on text fields.
      - Date parsing via parse_date_multi_format macro (handles 4 formats
        from the source: ISO, compact, slash-DMY, dash-MDY).
      - Semantic normalization of categoricals via macros:
          * transaction_type -> normalize_transaction_type (casing + Spanish)
          * status           -> normalize_transaction_status (casing only)
          * category         -> normalize_transaction_category (casing + NULL-eo
                                of '', 'NA', 'N/A', 'null' missing markers)
          * channel          -> normalize_casing inline (5 canonicals already
                                clean in EDA, defensive only)
      - currency kept as-is (ISO 4217 uppercase canonical).
      - merchant, description: trim only (free-text fields, no normalization).
      - amount kept as double precision; financial-grade numeric casts done
        in downstream gold marts where the grain is known.

    Grain: 1 row per transaction (transaction_id is unique within latest load).

    Materialization: view. Downstream silver dims/facts join through this.
*/

with source as (

    select
        customer_id,
        transaction_id,
        account_id,
        transaction_date,
        amount,
        currency,
        transaction_type,
        category,
        merchant,
        channel,
        status,
        description,
        bronze_id,
        load_timestamp
    from {{ source('silver_staging', 'stg_transactions') }}

),

normalized as (

    select
        -- ---------- Identity ----------
        trim(transaction_id)                                  as transaction_id,
        trim(customer_id)                                     as customer_id,
        trim(account_id)                                      as account_id,

        -- ---------- Transaction attributes ----------
        {{ parse_date_multi_format('transaction_date') }}     as transaction_date,
        amount,
        upper(trim(currency))                                 as currency,
        {{ normalize_transaction_type('transaction_type') }}  as transaction_type,
        {{ normalize_transaction_category('category') }}      as category,
        trim(merchant)                                        as merchant,
        {{ normalize_casing('channel') }}                     as channel,
        {{ normalize_transaction_status('status') }}          as status,
        trim(description)                                     as description,

        -- ---------- Audit ----------
        bronze_id,
        load_timestamp

    from source

)

select * from normalized