{{
    config(
        materialized='table'
    )
}}

/*
    mart_account_mix
    ----------------
    Grain: 1 row per (country × account_type × currency).
    Answers: Q22 (most popular account types), Q2 (balances by country).

    Sources:
      - silver.dim_account (account-level: balance, currency, account_type)
      - silver.dim_customer (for country attribution)

    Why country lives on the customer, not the account:
      The dataset places `country` on the customer record, not on accounts.
      A customer is anchored to one country, and all their accounts inherit
      that attribution. This is a simplification — in reality a customer
      could hold accounts in multiple jurisdictions — but it matches the
      source structure.

    Multi-currency handling:
      Balances are kept in their source currency. The grain explicitly
      includes `currency` so that sums never mix incompatible units
      (e.g. USD + ARS). PowerBI consumers should either filter to one
      currency or aggregate per-currency. A future enhancement would be
      a dim_fx_rate + mart_balances_usd for consolidated reporting.
      See decisions.md for full rationale.

    Status & data quality filters:
      Only `status = 'active'` accounts are included. Closed/frozen accounts
      have stale balances that would inflate the picture and are not relevant
      to "most popular account types" or "current balances by country".

      Active accounts with NULL balance (~183 of 5,877 active, ~3.1%) are
      INCLUDED in `accounts_count` (Q22 is about popularity, balance presence
      is not the criterion) but contribute nothing to balance aggregations
      (SQL SUM/AVG/MIN/MAX ignore NULL by definition). A separate column
      `accounts_with_balance` exposes the count of accounts that contributed
      to balance metrics, making missing-data rates queryable from BI.
*/

with accounts_with_country as (

    select
        a.account_id,
        a.account_type,
        a.currency,
        a.balance,
        a.status as account_status,
        c.country
    from {{ ref('dim_account') }} as a
    inner join {{ ref('dim_customer') }} as c
        on a.customer_id = c.customer_id
    where a.status = 'active'

),

aggregated as (

    select
        country,
        account_type,
        currency,
        count(distinct account_id)                                              as accounts_count,
        count(distinct case when balance is not null then account_id end)       as accounts_with_balance,
        round(sum(balance)::numeric, 2)                                         as total_balance,
        round(avg(balance)::numeric, 2)                                         as avg_balance,
        round(min(balance)::numeric, 2)                                         as min_balance,
        round(max(balance)::numeric, 2)                                         as max_balance
    from accounts_with_country
    group by country, account_type, currency

),

with_share as (

    select
        country,
        account_type,
        currency,
        accounts_count,
        accounts_with_balance,         -- ← AGREGAR ESTA LÍNEA
        total_balance,
        avg_balance,
        min_balance,
        max_balance,
        round(
            100.0 * accounts_count
            / sum(accounts_count) over (partition by country)
        , 2) as pct_of_country_accounts,
        round(
            100.0 * accounts_count
            / sum(accounts_count) over ()
        , 2) as pct_of_total_accounts
    from aggregated

)

select * from with_share
order by country, account_type, currency