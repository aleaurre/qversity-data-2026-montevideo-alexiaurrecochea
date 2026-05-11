{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    silver.dim_customer
    -------------------
    One row per customer. Extracts the FLAT fields from bronze.raw_fintech_data
    and casts them to proper types.

    -----------------------------------------------------------------------
    DATE FORMAT NOTE (discovered in EDA, day 5)
    -----------------------------------------------------------------------
    The source dataset uses FOUR different date string formats in both
    `date_of_birth` and `registration_date`. Each format AND locale was
    validated empirically before deciding the parser:

      Format               Locale  Example      Approx share  How confirmed
      -------------------  ------  -----------  ------------  --------------------------------
      ISO YYYY-MM-DD       n/a     1950-05-08   ~82%          unambiguous
      Slash DD/MM/YYYY     DMY     26/04/1975   ~6%           found rows with first comp > 12,
                                                              none with second > 12 -> DMY
      Dash  MM-DD-YYYY     MDY     06-13-2024   ~6%           a row with "06-13-..." disproved
                                                              the DMY hypothesis (no month 13);
                                                              so dash = MDY (US locale)
      Compact YYYYMMDD     n/a     19500509     ~6%           unambiguous

    Why slash is DMY and dash is MDY in the SAME dataset:
    the data appears to have been synthetically generated with intentional
    format and locale variability. This is unusual and not what one would
    expect from a single source system. The cleanest representation
    in silver is still a proper `date` type, regardless of input chaos.

    Strategy:
      1. Detect input format by regex (4 explicit branches + NULL guard).
      2. Apply TO_DATE() with the format string matching that branch.
      3. Unknown formats become NULL. We'll add a custom dbt test on
         day 6 to flag if the NULL rate climbs above a small threshold.

    Why not change Postgres' `datestyle` GUC:
    a single global setting can pick only one of {ISO, DMY, MDY}; it
    cannot represent multi-format data. Per-row parsing via CASE is the
    only correct approach here.
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

flat as (

    select
        -- ---------- Identity ----------
        (data ->> 'customer_id')::text         as customer_id,
        (data ->> 'first_name')::text          as first_name,
        (data ->> 'last_name')::text           as last_name,
        (data ->> 'email')::text               as email,
        (data ->> 'phone_number')::text        as phone_number,

        -- ---------- Demographics ----------
        case
            when data ->> 'date_of_birth' is null
                 or data ->> 'date_of_birth' = ''
                then null

            when data ->> 'date_of_birth' ~ '^\d{4}-\d{2}-\d{2}$'
                then (data ->> 'date_of_birth')::date

            when data ->> 'date_of_birth' ~ '^\d{8}$'
                then to_date(data ->> 'date_of_birth', 'YYYYMMDD')

            -- Slash = DMY (LATAM).
            when data ->> 'date_of_birth' ~ '^\d{2}/\d{2}/\d{4}$'
                then to_date(data ->> 'date_of_birth', 'DD/MM/YYYY')

            -- Dash = MDY (US). Confirmed by "06-13-2024" type rows.
            when data ->> 'date_of_birth' ~ '^\d{2}-\d{2}-\d{4}$'
                then to_date(data ->> 'date_of_birth', 'MM-DD-YYYY')

            else null
        end as date_of_birth,

        (data ->> 'gender')::text              as gender,
        (data ->> 'nationality')::text         as nationality,
        (data ->> 'city')::text                as city,
        (data ->> 'country')::text             as country,
        (data ->> 'address')::text             as address,

        -- ---------- Geo ----------
        nullif(data ->> 'lat', '')::numeric    as lat,
        nullif(data ->> 'lon', '')::numeric    as lon,

        -- ---------- Relationship & status ----------
        -- registration_date: same multi-format treatment as date_of_birth.
        case
            when data ->> 'registration_date' is null
                 or data ->> 'registration_date' = ''
                then null

            when data ->> 'registration_date' ~ '^\d{4}-\d{2}-\d{2}$'
                then (data ->> 'registration_date')::date

            when data ->> 'registration_date' ~ '^\d{8}$'
                then to_date(data ->> 'registration_date', 'YYYYMMDD')

            when data ->> 'registration_date' ~ '^\d{2}/\d{2}/\d{4}$'
                then to_date(data ->> 'registration_date', 'DD/MM/YYYY')

            when data ->> 'registration_date' ~ '^\d{2}-\d{2}-\d{4}$'
                then to_date(data ->> 'registration_date', 'MM-DD-YYYY')

            else null
        end as registration_date,

        (data ->> 'kyc_status')::text          as kyc_status,
        nullif(data ->> 'risk_score', '')::numeric as risk_score,
        (data ->> 'customer_segment')::text    as customer_segment,
        (data ->> 'relationship_manager')::text as relationship_manager,
        (data ->> 'status')::text              as status,

        -- ---------- Audit ----------
        load_timestamp

    from bronze_dedup
    where rn = 1

),

enriched as (

    select
        *,

        case
            when date_of_birth is null then null
            else extract(year from age(current_date, date_of_birth))::int
        end as age,

        case
            when date_of_birth is null                                       then 'unknown'
            when extract(year from age(current_date, date_of_birth)) < 18    then 'under_18'
            when extract(year from age(current_date, date_of_birth)) < 25    then '18-24'
            when extract(year from age(current_date, date_of_birth)) < 35    then '25-34'
            when extract(year from age(current_date, date_of_birth)) < 45    then '35-44'
            when extract(year from age(current_date, date_of_birth)) < 55    then '45-54'
            when extract(year from age(current_date, date_of_birth)) < 65    then '55-64'
            else '65_plus'
        end as age_bucket

    from flat

)

select * from enriched