{{
    config(
        materialized='table',
        unique_key='date_day'
    )
}}

/*
    silver.dim_date
    ---------------
    Calendar dimension generated via Postgres generate_series.

    Date range: 2015-01-01 to 2030-12-31 (~16 years, ~5800 rows).
    Rationale:
      - Earliest possible event: registration_date min observed ~2018 in EDA;
        extending to 2015 covers any older edge-case dates that may surface.
      - Latest possible event: account opening dates in the dataset go up to
        mid-2026; extending to 2030 future-proofs for ongoing analysis
        without needing to regenerate the dimension.

    Materialization: table. dim_date is queried by every gold mart that
    joins time-based facts; materializing avoids re-running generate_series
    on every PowerBI refresh.

    Grain: 1 row per calendar day (date_day is unique).

    Columns provided to downstream:
      - Calendar attributes (year, quarter, month, week, day-of-week, etc.)
      - Names (month_name, day_name) — useful for PowerBI labels.
      - Boolean flags (is_weekend) for common analytical splits.
      - Sortable integer keys (year_month_key, year_quarter_key) for
        PowerBI sort-by-column on year/quarter labels.

    Note on time zones: dates are zoneless. Source dataset transactions and
    registration events have no time component — the date the event occurred
    in the customer's local time is what matters analytically, not UTC.
*/

with date_spine as (

    select generate_series(
        '2015-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date as date_day

),

calendar as (

    select
        date_day,

        -- ---------- Year / Quarter / Month / Week ----------
        extract(year     from date_day)::int           as year,
        extract(quarter  from date_day)::int           as quarter,
        extract(month    from date_day)::int           as month,
        extract(week     from date_day)::int           as week_of_year,

        -- ---------- Day attributes ----------
        extract(day      from date_day)::int           as day_of_month,
        extract(dow      from date_day)::int           as day_of_week_num,  -- 0=Sunday, 6=Saturday
        extract(doy      from date_day)::int           as day_of_year,

        -- ---------- Names (English for PowerBI consistency) ----------
        to_char(date_day, 'Month')                     as month_name,
        to_char(date_day, 'Mon')                       as month_name_short,
        to_char(date_day, 'Day')                       as day_name,
        to_char(date_day, 'Dy')                        as day_name_short,

        -- ---------- Flags ----------
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                            as is_weekend,

        -- ---------- Sortable keys for PowerBI ----------
        -- These ensure PowerBI's "sort by column" produces correct
        -- chronological order on year-month or year-quarter labels.
        (extract(year from date_day) * 100 + extract(month from date_day))::int      as year_month_key,
        (extract(year from date_day) * 10  + extract(quarter from date_day))::int    as year_quarter_key,

        -- ---------- Period labels (for PowerBI display) ----------
        to_char(date_day, 'YYYY-MM')                   as year_month_label,
        to_char(date_day, '"Q"Q YYYY')                 as year_quarter_label,

        -- ---------- Start-of-period anchors (for rolling aggregations) ----------
        date_trunc('week',    date_day)::date          as week_start,
        date_trunc('month',   date_day)::date          as month_start,
        date_trunc('quarter', date_day)::date          as quarter_start,
        date_trunc('year',    date_day)::date          as year_start

    from date_spine

)

select * from calendar