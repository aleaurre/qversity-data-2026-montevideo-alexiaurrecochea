

/*
    silver.dim_credit_info
    ----------------------
    Dimensional table extracting the `credit_info` nested object from bronze.

    Pattern (different from stg_accounts/transactions/loans):
    credit_info is a 1:1 nested OBJECT (not a 1:N array). It doesn't need
    Spark's explode — Postgres jsonb operators are both performant (5k rows)
    and idiomatic. Per the responsibility split documented in decisions.md:
    Spark handles arrays (cardinality changes via explode); dbt handles flat
    fields and nested objects (cardinality preserved 1:1 with customer).

    Source: bronze.raw_fintech_data (jsonb)
    Grain: 1 row per customer (customer_id is unique within latest batch)

    Latest-batch semantics: same dedup approach as dim_customer — use
    ROW_NUMBER() over customer_id ordered by load_timestamp desc.
    Customers with multiple bronze entries (one per DAG run) keep only
    their latest credit_info snapshot.

    All numeric fields cast explicitly: jsonb->>'foo' returns text, so
    naked numeric operations would error or silently coerce. The casts
    here are the boundary between "raw text from json" and "typed
    values for downstream dimensional models".
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
        (data ->> 'customer_id')::text                                                                  as customer_id,

        -- ---------- Credit info: raw values ----------
        -- credit_score_raw preserves whatever the generator emitted
        -- (including sentinel values like 999999 and negatives).
        -- credit_score (computed below) is the validated version.
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'credit_score'", 'int') }}                      as credit_score_raw,
        upper(trim(data -> 'credit_info' ->> 'currency'))                                               as currency,

        -- utilization_pct_raw: contains the result of safe_cast_numeric.
        -- Already NULL for dirty values ($78.26, 89.5 USD, 83,5) due to
        -- the regex guard in safe_cast_numeric. utilization_pct (computed
        -- below) is the validated version with the in-range check.
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'utilization_pct'", 'numeric') }}               as utilization_pct_raw,

        {{ safe_cast_numeric("data -> 'credit_info' ->> 'total_limit'", 'numeric') }}                   as total_limit,
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'total_used'", 'numeric') }}                    as total_used,
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'num_credit_accounts'", 'int') }}               as num_credit_accounts,
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'oldest_account_age_months'", 'int') }}         as oldest_account_age_months,
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'late_payments_12m'", 'int') }}                 as late_payments_12m,
        {{ safe_cast_numeric("data -> 'credit_info' ->> 'inquiries_6m'", 'int') }}                      as inquiries_6m,
        (data -> 'credit_info' ->> 'bankruptcy_flag')::boolean                                          as bankruptcy_flag,

        -- ---------- Audit ----------
        load_timestamp

    from bronze_dedup
    where rn = 1

),

validated as (

    select
        customer_id,

        -- credit_score: validate the raw value against FICO range [300, 850].
        -- ~10% of records have sentinel/corrupt values (999999, 0, negatives).
        -- Both raw and validated are exposed: raw for audit, validated for use.
        credit_score_raw,
        case
            when credit_score_raw is null then false
            when credit_score_raw not between 300 and 850 then false
            else true
        end as is_credit_score_valid,
        case
            when credit_score_raw is null then null
            when credit_score_raw not between 300 and 850 then null
            else credit_score_raw
        end as credit_score,

        currency,

        -- utilization_pct: validate the raw value against [0, 100] range.
        -- Raw is already NULL for unparseable strings (38 records, 0.4%);
        -- validated additionally NULLs out-of-range values.
        utilization_pct_raw,
        case
            when utilization_pct_raw is null then false
            when utilization_pct_raw not between 0 and 100 then false
            else true
        end as is_utilization_pct_valid,
        case
            when utilization_pct_raw is null then null
            when utilization_pct_raw not between 0 and 100 then null
            else utilization_pct_raw
        end as utilization_pct,

        total_limit,
        total_used,
        num_credit_accounts,
        oldest_account_age_months,
        late_payments_12m,
        inquiries_6m,
        bankruptcy_flag,
        load_timestamp

    from extracted

)

select * from validated

