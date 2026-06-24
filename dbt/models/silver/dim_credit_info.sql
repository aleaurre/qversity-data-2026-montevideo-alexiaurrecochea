{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    silver.dim_credit_info — versión Databricks.
    Único cambio: los operadores JSONB en los argumentos de los macros pasan a
    get_json_object. El macro safe_cast_numeric ya está convertido (rlike + cast),
    así que la lógica de DQ no cambia. La CTE validated es SQL estándar (between,
    not between) y porta sin tocar.
*/

with bronze_dedup as (

    select
        data,
        load_timestamp,
        row_number() over (
            partition by get_json_object(data, '$.customer_id')
            order by load_timestamp desc
        ) as rn
    from {{ source('bronze', 'raw_fintech_data') }}
    where get_json_object(data, '$.customer_id') is not null

),

extracted as (

    select
        get_json_object(data, '$.customer_id') as customer_id,

        -- ---------- Credit info: raw values ----------
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.credit_score')", 'int') }}                as credit_score_raw,
        upper(trim(get_json_object(data, '$.credit_info.currency')))                                         as currency,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.utilization_pct')", 'numeric') }}         as utilization_pct_raw,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.total_limit')", 'numeric') }}             as total_limit,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.total_used')", 'numeric') }}              as total_used,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.num_credit_accounts')", 'int') }}         as num_credit_accounts,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.oldest_account_age_months')", 'int') }}   as oldest_account_age_months,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.late_payments_12m')", 'int') }}           as late_payments_12m,
        {{ safe_cast_numeric("get_json_object(data, '$.credit_info.inquiries_6m')", 'int') }}                as inquiries_6m,
        cast(get_json_object(data, '$.credit_info.bankruptcy_flag') as boolean)                              as bankruptcy_flag,

        load_timestamp

    from bronze_dedup
    where rn = 1

),

validated as (

    select
        customer_id,

        credit_score_raw,
        case
            when credit_score_raw is null then false
            when credit_score_raw not between 300 and 850 then false
            else true
        end as is_credit_score_valid,
        case
            when credit_score_raw is null then null
            when credit_score_raw not between 300 and 850 then null
            else credit_score_raw
        end as credit_score,

        currency,

        utilization_pct_raw,
        case
            when utilization_pct_raw is null then false
            when utilization_pct_raw not between 0 and 100 then false
            else true
        end as is_utilization_pct_valid,
        case
            when utilization_pct_raw is null then null
            when utilization_pct_raw not between 0 and 100 then null
            else utilization_pct_raw
        end as utilization_pct,

        total_limit,
        total_used,
        num_credit_accounts,
        oldest_account_age_months,
        late_payments_12m,
        inquiries_6m,
        bankruptcy_flag,
        load_timestamp

    from extracted

)

select * from validated
