{{
    config(
        materialized='table',
        unique_key='date_day'
    )
}}

/*
    silver.dim_date — versión Databricks.
    Conversiones:
      - generate_series('2015-01-01'::date, '2030-12-31'::date, '1 day')::date
            -> explode(sequence(to_date('2015-01-01'), to_date('2030-12-31'), interval 1 day))
      - extract(year|quarter|month|day|week|doy ...)::int  -> year()|quarter()|month()|dayofmonth()|weekofyear()|dayofyear()
      - extract(dow ...)  (0=Dom..6=Sáb)  -> dayofweek() - 1   (dayofweek es 1=Dom..7=Sáb)
      - is_weekend: extract(dow) in (0,6)  -> dayofweek() in (1,7)
      - to_char(d,'YYYY-MM')  -> date_format(d,'yyyy-MM')
      - to_char(d,'"Q"Q YYYY')-> concat('Q', quarter(d), ' ', year(d))
      - to_char(d,'Month'|'Mon'|'Day'|'Dy') -> date_format(d,'MMMM'|'MMM'|'EEEE'|'EEE')
      - date_trunc('week'|'month'|'quarter'|'year', d)::date -> trunc(d, 'WEEK'|'MONTH'|'QUARTER'|'YEAR')
*/

with date_spine as (

    select explode(
        sequence(to_date('2015-01-01'), to_date('2030-12-31'), interval 1 day)
    ) as date_day

),

calendar as (

    select
        date_day,

        -- ---------- Year / Quarter / Month / Week ----------
        cast(year(date_day) as int)        as year,
        cast(quarter(date_day) as int)     as quarter,
        cast(month(date_day) as int)       as month,
        cast(weekofyear(date_day) as int)  as week_of_year,

        -- ---------- Day attributes ----------
        cast(dayofmonth(date_day) as int)  as day_of_month,
        cast(dayofweek(date_day) - 1 as int) as day_of_week_num,   -- 0=Sunday, 6=Saturday
        cast(dayofyear(date_day) as int)   as day_of_year,

        -- ---------- Names (English for PowerBI) ----------
        date_format(date_day, 'MMMM')      as month_name,
        date_format(date_day, 'MMM')       as month_name_short,
        date_format(date_day, 'EEEE')      as day_name,
        date_format(date_day, 'EEE')       as day_name_short,

        -- ---------- Flags ----------
        case
            when dayofweek(date_day) in (1, 7) then true
            else false
        end as is_weekend,

        -- ---------- Sortable keys for PowerBI ----------
        cast(year(date_day) * 100 + month(date_day) as int)   as year_month_key,
        cast(year(date_day) * 10  + quarter(date_day) as int) as year_quarter_key,

        -- ---------- Period labels ----------
        date_format(date_day, 'yyyy-MM')                    as year_month_label,
        concat('Q', quarter(date_day), ' ', year(date_day)) as year_quarter_label,

        -- ---------- Start-of-period anchors ----------
        trunc(date_day, 'WEEK')    as week_start,
        trunc(date_day, 'MONTH')   as month_start,
        trunc(date_day, 'QUARTER') as quarter_start,
        trunc(date_day, 'YEAR')    as year_start

    from date_spine

)

select * from calendar
