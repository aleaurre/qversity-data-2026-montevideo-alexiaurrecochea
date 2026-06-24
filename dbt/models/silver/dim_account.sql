{{
    config(
        materialized='table',
        unique_key='account_id'
    )
}}

/*
    silver.dim_account — versión Databricks.
    Único cambio: age(current_date, opened_date) -> months_between(current_date(), opened_date).
    El resto lee de stg_accounts (ya limpio) y porta sin tocar.
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

        -- account_age_months: meses desde opened_date. NULL si opened_date es NULL.
        case
            when opened_date is null then null
            else cast(floor(months_between(current_date(), opened_date)) as int)
        end as account_age_months,

        load_timestamp

    from source

)

select * from enriched
