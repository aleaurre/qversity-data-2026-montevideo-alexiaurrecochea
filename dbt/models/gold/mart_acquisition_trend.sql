{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    mart_acquisition_trend
    ----------------------
    Grain: 1 row per calendar month.
    Answers: Q12 (customer acquisition trend over time, monthly).

    Source: silver.dim_customer (one row per customer, registration_date::date).

    Logic:
      - Truncate registration_date to month
      - Count distinct customers per month (new acquisitions)
      - Running sum across months (cumulative base)
      - MoM growth %: (this_month - prev_month) / prev_month * 100

    Notes:
      - Customers with NULL registration_date are excluded (cannot be placed
        on a timeline). Their count is small per EDA and would distort the
        trend if bucketed into an "unknown" month.
      - month_label is included for PowerBI: lexicographic sort = chronological
        sort, which avoids the classic "Jan after Sep" ordering bug.
*/

with monthly_acquisitions as (

    select
        date_trunc('month', registration_date)::date as month,
        count(distinct customer_id) as new_customers
    from {{ ref('dim_customer') }}
    where registration_date is not null
    group by 1

),

with_running_total as (

    select
        month,
        extract(year  from month)::int  as year,
        extract(month from month)::int  as month_number,
        to_char(month, 'YYYY-MM')       as month_label,
        new_customers,
        sum(new_customers) over (
            order by month
            rows between unbounded preceding and current row
        ) as cumulative_customers,
        lag(new_customers) over (order by month) as prev_month_customers
    from monthly_acquisitions

),

final as (

    select
        month,
        year,
        month_number,
        month_label,
        new_customers,
        cumulative_customers,
        case
            when prev_month_customers is null or prev_month_customers = 0 then null
            else round(
                ((new_customers - prev_month_customers)::numeric / prev_month_customers) * 100,
                2
            )
        end as mom_growth_pct
    from with_running_total

)

select * from final
order by month