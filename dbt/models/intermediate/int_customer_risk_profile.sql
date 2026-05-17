{{
  config(
    materialized='view'
  )
}}

/*
  int_customer_risk_profile
  -------------------------
  Grain: one row per customer (preserves dim_customer grain).
  
  Purpose: shared analytics-ready base for downstream credit risk marts.
  Centralizes customer-level risk attributes (buckets) and the customer-level
  delinquency flag derived from their loans.
  
  Consumed by:
    - mart_delinquency_by_segment           : Q5
    - mart_credit_score_by_country          : Q6
    - mart_utilization_vs_delinquency       : Q7
    - mart_risk_buckets                     : Q9
  
  Business definitions applied (see decisions.md Day 9):
    - is_delinquent_customer = TRUE if customer has at least one loan with
      is_delinquent = TRUE (any-rule, decision Day 9 §2 customer-level).
      Customers without loans get FALSE (no active credit risk exposure).
    - risk_bucket, credit_score_bucket, utilization_bucket: applied via
      get_* macros (single source of truth, same as mart_customer_360).
  
  Materialized as view: ~5,000 rows, downstream marts aggregate further,
  no caching benefit. Aligns with int_loan_portfolio_metrics pattern.
*/

with customer_base as (
    select
        customer_id,
        country,
        customer_segment,
        risk_score,
        kyc_status,
        status
    from {{ ref('dim_customer') }}
    where risk_score is not null
),

customer_credit as (
    select
        customer_id,
        credit_score,
        utilization_pct,
        total_limit,
        total_used,
        late_payments_12m,
        bankruptcy_flag
    from {{ ref('dim_credit_info') }}
),

customer_delinquency as (
    -- Any-rule: customer is delinquent if at least one loan is delinquent.
    -- See decisions.md Day 9: customer-level delinquency definition.
    select
        customer_id,
        bool_or(is_delinquent) as is_delinquent_customer,
        bool_or(is_default)    as is_default_customer,
        count(*)               as loans_count,
        count(*) filter (where is_delinquent) as delinquent_loans_count,
        count(*) filter (where is_default)    as default_loans_count
    from {{ ref('int_loan_portfolio_metrics') }}
    group by customer_id
)

select
    -- Identity
    c.customer_id,
    
    -- Dimensions
    c.country,
    c.customer_segment,
    c.kyc_status,
    c.status,
    
    -- Risk score and bucket
    c.risk_score,
    {{ get_risk_bucket('c.risk_score') }} as risk_bucket,
    
    -- Credit profile
    cr.credit_score,
    {{ get_credit_score_bucket('cr.credit_score') }} as credit_score_bucket,
    cr.utilization_pct,
    {{ get_utilization_bucket('cr.utilization_pct') }} as utilization_bucket,
    cr.total_limit,
    cr.total_used,
    cr.late_payments_12m,
    cr.bankruptcy_flag,
    
    -- Delinquency flags (customer-level, from loan rollup)
    coalesce(cd.is_delinquent_customer, false) as is_delinquent_customer,
    coalesce(cd.is_default_customer, false)    as is_default_customer,
    coalesce(cd.loans_count, 0)                 as loans_count,
    coalesce(cd.delinquent_loans_count, 0)      as delinquent_loans_count,
    coalesce(cd.default_loans_count, 0)         as default_loans_count

from customer_base c
left join customer_credit       cr on cr.customer_id = c.customer_id
left join customer_delinquency  cd on cd.customer_id = c.customer_id