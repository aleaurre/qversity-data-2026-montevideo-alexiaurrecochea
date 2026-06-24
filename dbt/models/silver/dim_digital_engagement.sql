{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    silver.dim_digital_engagement — versión Databricks.
    Único cambio: operadores JSONB en los args de los macros -> get_json_object.
    safe_cast_boolean y normalize_casing son SQL puro y no cambian;
    safe_cast_numeric ya está convertido; parse_date_multi_format también.
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

        {{ safe_cast_boolean("get_json_object(data, '$.digital_engagement.mobile_app_registered')") }}  as mobile_app_registered,
        {{ safe_cast_boolean("get_json_object(data, '$.digital_engagement.web_banking_registered')") }} as web_banking_registered,
        {{ parse_date_multi_format("get_json_object(data, '$.digital_engagement.last_login_date')") }}  as last_login_date,
        {{ safe_cast_numeric("get_json_object(data, '$.digital_engagement.avg_monthly_logins')", 'int') }} as avg_monthly_logins,
        {{ normalize_casing("get_json_object(data, '$.digital_engagement.preferred_channel')") }}        as preferred_channel,
        {{ safe_cast_boolean("get_json_object(data, '$.digital_engagement.push_notifications')") }}      as push_notifications,
        {{ safe_cast_boolean("get_json_object(data, '$.digital_engagement.paperless_statements')") }}    as paperless_statements,

        load_timestamp

    from bronze_dedup
    where rn = 1

)

select * from extracted
