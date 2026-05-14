{{
    config(
        materialized='view'
    )
}}

/*
    silver.stg_digital_engagement
    -----------------------------
    Staging view extracting the `digital_engagement` nested object from bronze.

    Same pattern as stg_credit_info: 1:1 nested object, extracted via
    Postgres jsonb operators, deduplicated by latest load_timestamp.

    Boolean fields are cast directly from jsonb (jsonb supports native
    boolean parsing of 'true'/'false' strings).

    `last_login_date` uses the multi-format date parser since EDA day 6
    confirmed the same format diversity affects date fields across the
    dataset (ISO, slash, dash, compact).

    `preferred_channel` is normalized with normalize_casing — EDA confirmed
    5 clean values (mobile, atm, branch, phone, web), but defensive
    against generator drift. Note: `phone` here is a digital_engagement
    preference (channel-of-choice for customer support / banking ops),
    distinct from transaction.channel which uses 'pos' instead of 'phone'.
    These are different semantic dimensions despite overlapping vocabulary.

    Grain: 1 row per customer.
*/

with bronze_dedup as (

    select
        data,
        load_timestamp,
        row_number() over (
            partition by data ->> 'customer_id'
            order by load_timestamp desc
        ) as rn
    from {{ source('bronze', 'raw_fintech_data') }}
    where data ->> 'customer_id' is not null

),

extracted as (

    select
        -- ---------- Identity ----------
        (data ->> 'customer_id')::text                                          as customer_id,

        -- ---------- Digital engagement (1:1 nested object) ----------
        (data -> 'digital_engagement' ->> 'mobile_app_registered')::boolean     as mobile_app_registered,
        (data -> 'digital_engagement' ->> 'web_banking_registered')::boolean    as web_banking_registered,
        {{ parse_date_multi_format("data -> 'digital_engagement' ->> 'last_login_date'") }} as last_login_date,
        {{ safe_cast_numeric("data -> 'digital_engagement' ->> 'avg_monthly_logins'", 'int') }} as avg_monthly_logins,
        {{ normalize_casing("data -> 'digital_engagement' ->> 'preferred_channel'") }} as preferred_channel,
        (data -> 'digital_engagement' ->> 'push_notifications')::boolean        as push_notifications,
        (data -> 'digital_engagement' ->> 'paperless_statements')::boolean      as paperless_statements,

        -- ---------- Audit ----------
        load_timestamp

    from bronze_dedup
    where rn = 1

)

select * from extracted