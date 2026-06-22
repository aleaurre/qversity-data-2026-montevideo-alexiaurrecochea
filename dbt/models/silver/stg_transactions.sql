{{
    config(
        materialized='view'
    )
}}

/*
    silver.stg_transactions  — versión Databricks.

    Conversión casi 1:1 desde la versión Postgres. Por qué tan poco cambia:
    este staging NO toca JSONB — el flatten de JSONB->columnas ya ocurrió en
    el notebook PySpark (silver_raw.stg_transactions). Acá solo hay trim, casts
    y normalización semántica vía macros, todo SQL estándar que Spark soporta.

    Lo único que cambia respecto al original:
      - upper(trim()), trim(): idénticos en Spark SQL.
      - parse_date_multi_format: misma firma, internamente ya usa el dialecto
        Spark (try_to_date + rlike).
      - normalize_*: idénticos (lower/trim + CASE), portan sin cambios.
      - source(): resuelve a qversity.silver_raw.stg_transactions vía _sources.yml.

    Grain: 1 fila por transacción. Materialización: view.
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
