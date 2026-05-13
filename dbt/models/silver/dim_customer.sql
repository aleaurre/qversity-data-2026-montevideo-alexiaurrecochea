{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

/*
    silver.dim_customer
    -------------------
    One row per customer. Extracts flat customer fields from
    bronze.raw_fintech_data, deduplicates, casts to proper types, normalizes
    categorical values, validates geographic coordinates, and enriches with
    derived dimensions (age, age_bucket, tenure_months, tenure_bucket).

    -----------------------------------------------------------------------
    DEDUP STRATEGY
    -----------------------------------------------------------------------
    The bronze layer uses append-with-load_id semantics: each DAG run inserts
    a fresh batch of records, all tagged with the same UUID load_id. EDA on
    day 1 found ~100 customer_id duplicates within a single batch (likely a
    generator bug). We deduplicate by `customer_id` keeping the most recent
    `load_timestamp` — this also handles the future case of re-runs producing
    multiple batches.

    Why dedup here in dbt and not in Spark like accounts/transactions/loans:
    customer is the root of the JSON record, not an array. A Spark script
    would not actually flatten anything — only dedup — which Postgres handles
    trivially via ROW_NUMBER() at 5k-row scale. Documented in decisions.md
    under "Refined Spark vs dbt responsibility split".

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

    Strategy: detect format by regex, apply TO_DATE() with matching pattern,
    NULL on unknown formats. Custom test below flags excessive NULL rates.

    -----------------------------------------------------------------------
    GEO VALIDATION
    -----------------------------------------------------------------------
    EDA found 340 of 5000 customers (6.8%) have lat/lon outside valid ranges
    ([-90, 90] for lat, [-180, 180] for lon). Treatment: NULL the invalid
    coordinates AND emit an `is_geo_valid` boolean flag. This preserves the
    customer record (no row loss) while making the data quality issue
    queryable from downstream models.

    -----------------------------------------------------------------------
    STATUS NORMALIZATION
    -----------------------------------------------------------------------
    Customer.status has 20 surface variants across 4 canonical values
    (active / inactive / suspended / closed). Casing chaos + Spanish
    translations. Normalized via `normalize_customer_status` macro.
    See decisions.md for full mapping.
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

        -- ---------- Geo (raw, validated below) ----------
        nullif(data ->> 'lat', '')::numeric    as lat_raw,
        nullif(data ->> 'lon', '')::numeric    as lon_raw,

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

        (data ->> 'kyc_status')::text              as kyc_status,
        nullif(data ->> 'risk_score', '')::numeric as risk_score,
        (data ->> 'customer_segment')::text        as customer_segment,
        (data ->> 'relationship_manager')::text    as relationship_manager,
        (data ->> 'status')::text                  as status_raw,

        -- ---------- Audit ----------
        load_timestamp

    from bronze_dedup
    where rn = 1

),

enriched as (

    select
        -- ---------- Identity ----------
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,

        -- ---------- Demographics ----------
        date_of_birth,
        case
            when date_of_birth is null then null
            else extract(year from age(current_date, date_of_birth))::int
        end as age,
        {{ age_bucket('date_of_birth') }} as age_bucket,

        gender,
        nationality,
        city,
        country,
        address,

        -- ---------- Geo (validated) ----------
        -- Coordinates outside valid lat/lon ranges are nulled out and flagged.
        case
            when lat_raw is null or lon_raw is null then false
            when lat_raw < -90  or lat_raw > 90     then false
            when lon_raw < -180 or lon_raw > 180    then false
            else true
        end as is_geo_valid,

        case
            when lat_raw is null or lon_raw is null            then null
            when lat_raw < -90 or lat_raw > 90                 then null
            when lon_raw < -180 or lon_raw > 180               then null
            else lat_raw
        end as lat,

        case
            when lat_raw is null or lon_raw is null            then null
            when lat_raw < -90 or lat_raw > 90                 then null
            when lon_raw < -180 or lon_raw > 180               then null
            else lon_raw
        end as lon,

        -- ---------- Relationship & status ----------
        registration_date,
        case
            when registration_date is null then null
            else (
                extract(year  from age(current_date, registration_date)) * 12
              + extract(month from age(current_date, registration_date))
            )::int
        end as tenure_months,
        {{ tenure_bucket('registration_date') }} as tenure_bucket,

        {{ normalize_kyc_status('kyc_status') }} as kyc_status,
        risk_score,
        {{ normalize_customer_segment('customer_segment') }} as customer_segment,
        relationship_manager,
        {{ normalize_customer_status('status_raw') }} as status,

        -- ---------- Audit ----------
        load_timestamp

    from flat

)

select * from enriched