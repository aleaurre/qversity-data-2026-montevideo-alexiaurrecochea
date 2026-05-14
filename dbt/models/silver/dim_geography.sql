{{
    config(
        materialized='table',
        unique_key='country_code'
    )
}}

/*
    silver.dim_geography
    --------------------
    Geography lookup dimension. One row per ISO country code.

    Hard-coded with the 7 LATAM countries from the project spec (§3.2):
    Colombia, Uruguay, Argentina, Mexico, Chile, Peru, Brazil. The
    dataset doesn't contain other countries, so a complete lookup
    suffices — no need to derive from data.

    What's NOT here:
      - city: dim_customer.city has known typos (e.g., 'Lma' for 'Lima'
        documented in decisions.md), so a city dimension would inherit
        that inconsistency. Power BI can group by dim_customer.city
        directly when needed. A future city dimension would require
        a deterministic city normalization pipeline.
      - customer counts: aggregates live in gold marts (e.g., one
        gold mart for business question 10 "customer count by country
        and city").

    Materialization: table. Tiny (7 rows) but joined by every gold
    mart that displays country names in PowerBI.

    Grain: 1 row per country_code.

    Region: all 7 countries are in LATAM. The column exists for future
    extensibility (e.g., adding North America or EU markets).
*/

with countries as (

    select * from (values
        ('CO', 'Colombia',  'LATAM', 'COP'),
        ('UY', 'Uruguay',   'LATAM', 'UYU'),
        ('AR', 'Argentina', 'LATAM', 'ARS'),
        ('MX', 'Mexico',    'LATAM', 'MXN'),
        ('CL', 'Chile',     'LATAM', 'CLP'),
        ('PE', 'Peru',      'LATAM', 'PEN'),
        ('BR', 'Brazil',    'LATAM', 'BRL')
    ) as t(country_code, country_name, region, default_currency)

)

select
    country_code,
    country_name,
    region,
    default_currency
from countries