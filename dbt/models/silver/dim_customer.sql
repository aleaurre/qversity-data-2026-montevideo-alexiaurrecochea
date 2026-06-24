{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    silver.dim_customer — versión Databricks (Spark SQL).
    Conversiones vs Postgres:
      - data ->> 'x'            -> get_json_object(data, '$.x')
      - (x)::text               -> (drop; get_json_object ya devuelve string)
      - nullif(x,'')::numeric   -> cast(nullif(x,'') as double)
      - age(current_date, d)    -> months_between(current_date(), d)
        * años:  floor(months_between(...) / 12)
        * meses: floor(months_between(...))
    Las macros normalize_* y la validación geo son SQL estándar; no cambian.
    Las fechas usan parse_date_multi_format (ya convertido).
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

flat as (

    select
        -- ---------- Identity ----------
        get_json_object(data, '$.customer_id')    as customer_id,
        get_json_object(data, '$.first_name')     as first_name,
        get_json_object(data, '$.last_name')      as last_name,
        get_json_object(data, '$.email')          as email,
        get_json_object(data, '$.phone_number')   as phone_number,

        -- ---------- Demographics ----------
        {{ parse_date_multi_format("get_json_object(data, '$.date_of_birth')") }} as date_of_birth,
        get_json_object(data, '$.gender')         as gender,
        get_json_object(data, '$.nationality')    as nationality,
        get_json_object(data, '$.city')           as city,
        get_json_object(data, '$.country')        as country,
        get_json_object(data, '$.address')        as address,

        -- ---------- Geo (raw, validated below) ----------
        cast(nullif(get_json_object(data, '$.lat'), '') as double) as lat_raw,
        cast(nullif(get_json_object(data, '$.lon'), '') as double) as lon_raw,

        -- ---------- Relationship & status ----------
        {{ parse_date_multi_format("get_json_object(data, '$.registration_date')") }} as registration_date,
        get_json_object(data, '$.kyc_status')            as kyc_status,
        cast(nullif(get_json_object(data, '$.risk_score'), '') as double) as risk_score,
        get_json_object(data, '$.customer_segment')      as customer_segment,
        get_json_object(data, '$.relationship_manager')  as relationship_manager,
        get_json_object(data, '$.status')                as status_raw,

        load_timestamp

    from bronze_dedup
    where rn = 1

),

enriched as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,

        -- ---------- Demographics ----------
        date_of_birth,
        case
            when date_of_birth is null then null
            else cast(floor(months_between(current_date(), date_of_birth) / 12) as int)
        end as age,
        case
            when date_of_birth is null then 'unknown'
            when floor(months_between(current_date(), date_of_birth) / 12) < 18 then 'under_18'
            when floor(months_between(current_date(), date_of_birth) / 12) <= 25 then '18-25'
            when floor(months_between(current_date(), date_of_birth) / 12) <= 35 then '26-35'
            when floor(months_between(current_date(), date_of_birth) / 12) <= 50 then '36-50'
            when floor(months_between(current_date(), date_of_birth) / 12) <= 65 then '51-65'
            else '65+'
        end as age_bucket,

        gender,
        nationality,
        city,
        country,
        address,

        -- ---------- Geo (validated) ----------
        case
            when lat_raw is null or lon_raw is null then false
            when lat_raw < -90  or lat_raw > 90     then false
            when lon_raw < -180 or lon_raw > 180    then false
            else true
        end as is_geo_valid,
        case
            when lat_raw is null or lon_raw is null then null
            when lat_raw < -90 or lat_raw > 90      then null
            when lon_raw < -180 or lon_raw > 180    then null
            else lat_raw
        end as lat,
        case
            when lat_raw is null or lon_raw is null then null
            when lat_raw < -90 or lat_raw > 90      then null
            when lon_raw < -180 or lon_raw > 180    then null
            else lon_raw
        end as lon,

        -- ---------- Relationship & status ----------
        registration_date,
        case
            when registration_date is null then null
            else cast(floor(months_between(current_date(), registration_date)) as int)
        end as tenure_months,
        case
            when registration_date is null then 'unknown'
            when floor(months_between(current_date(), registration_date)) < 6  then 'new'
            when floor(months_between(current_date(), registration_date)) <= 24 then 'established'
            else 'loyal'
        end as tenure_bucket,

        {{ normalize_casing('kyc_status') }}                 as kyc_status,
        risk_score,
        {{ normalize_customer_segment('customer_segment') }} as customer_segment,
        relationship_manager,
        {{ normalize_customer_status('status_raw') }}        as status,

        load_timestamp

    from flat

)

select * from enriched
