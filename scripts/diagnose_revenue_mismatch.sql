-- ============================================================================
-- scripts/diagnose_revenue_mismatch.sql
-- ----------------------------------------------------------------------------
-- Re-implements the OLD mart_revenue_by_segment_usd logic inline, then compares
-- its customer-level output against int_customer_monthly_revenue. Any customer
-- where the values diverge points us to the bug.
-- ============================================================================

\x off
\timing on

-- Build a temp CTE that recomputes the OLD logic exactly.
WITH

fx AS (
    SELECT currency, rate_to_usd FROM silver.fx_rates
),

-- OLD fee CTE: identical SQL to pre-refactor mart_revenue_by_segment_usd
old_fee AS (
    SELECT
        t.customer_id,
        SUM(t.amount * fx.rate_to_usd) AS fee_revenue_lifetime_usd
    FROM silver.fct_transactions t
    LEFT JOIN fx ON fx.currency = t.currency
    WHERE t.transaction_type = 'fee'
      AND t.status = 'completed'
    GROUP BY t.customer_id
),

-- OLD interest CTE
old_interest AS (
    SELECT
        l.customer_id,
        SUM(l.outstanding_balance * l.interest_rate_decimal / 12.0 * fx.rate_to_usd)
            AS interest_revenue_usd
    FROM silver.fct_loans l
    LEFT JOIN fx ON fx.currency = l.currency
    WHERE l.status IN ('current', 'delinquent')
    GROUP BY l.customer_id
),

-- OLD revenue_per_customer joined on dim_customer
old_revenue_per_customer AS (
    SELECT
        c.customer_id,
        COALESCE(fee.fee_revenue_lifetime_usd / NULLIF(c.tenure_months, 0), 0)
            AS old_monthly_fee_usd,
        COALESCE(interest.interest_revenue_usd, 0)
            AS old_monthly_interest_usd,
        COALESCE(fee.fee_revenue_lifetime_usd / NULLIF(c.tenure_months, 0), 0)
            + COALESCE(interest.interest_revenue_usd, 0)
            AS old_total_revenue_usd
    FROM silver.dim_customer c
    LEFT JOIN old_fee fee           ON fee.customer_id = c.customer_id
    LEFT JOIN old_interest interest ON interest.customer_id = c.customer_id
)

-- Compare against int_customer_monthly_revenue
SELECT
    COUNT(*)                                                                  AS total_customers,
    COUNT(*) FILTER (WHERE ABS(o.old_monthly_fee_usd      - i.monthly_fee_revenue_usd)      > 0.0001) AS diverge_fee,
    COUNT(*) FILTER (WHERE ABS(o.old_monthly_interest_usd - i.monthly_interest_revenue_usd) > 0.0001) AS diverge_interest,
    COUNT(*) FILTER (WHERE ABS(o.old_total_revenue_usd    - i.total_monthly_revenue_usd)    > 0.0001) AS diverge_total
FROM old_revenue_per_customer o
INNER JOIN silver.int_customer_monthly_revenue i USING (customer_id);


-- Show first 10 divergent customers (if any) for inspection.
WITH

fx AS (
    SELECT currency, rate_to_usd FROM silver.fx_rates
),
old_fee AS (
    SELECT t.customer_id,
           SUM(t.amount * fx.rate_to_usd) AS fee_revenue_lifetime_usd
    FROM silver.fct_transactions t
    LEFT JOIN fx ON fx.currency = t.currency
    WHERE t.transaction_type = 'fee' AND t.status = 'completed'
    GROUP BY t.customer_id
),
old_interest AS (
    SELECT l.customer_id,
           SUM(l.outstanding_balance * l.interest_rate_decimal / 12.0 * fx.rate_to_usd) AS interest_revenue_usd
    FROM silver.fct_loans l
    LEFT JOIN fx ON fx.currency = l.currency
    WHERE l.status IN ('current', 'delinquent')
    GROUP BY l.customer_id
),
old_revenue_per_customer AS (
    SELECT c.customer_id,
           COALESCE(fee.fee_revenue_lifetime_usd / NULLIF(c.tenure_months, 0), 0)   AS old_monthly_fee_usd,
           COALESCE(interest.interest_revenue_usd, 0)                                AS old_monthly_interest_usd,
           COALESCE(fee.fee_revenue_lifetime_usd / NULLIF(c.tenure_months, 0), 0)
               + COALESCE(interest.interest_revenue_usd, 0)                          AS old_total_revenue_usd
    FROM silver.dim_customer c
    LEFT JOIN old_fee fee           ON fee.customer_id = c.customer_id
    LEFT JOIN old_interest interest ON interest.customer_id = c.customer_id
)

SELECT
    o.customer_id,
    ROUND(o.old_monthly_fee_usd::numeric, 4)        AS old_fee,
    ROUND(i.monthly_fee_revenue_usd::numeric, 4)    AS new_fee,
    ROUND((o.old_monthly_fee_usd - i.monthly_fee_revenue_usd)::numeric, 4) AS delta_fee,
    ROUND(o.old_monthly_interest_usd::numeric, 4)        AS old_int,
    ROUND(i.monthly_interest_revenue_usd::numeric, 4)    AS new_int,
    ROUND((o.old_monthly_interest_usd - i.monthly_interest_revenue_usd)::numeric, 4) AS delta_int
FROM old_revenue_per_customer o
INNER JOIN silver.int_customer_monthly_revenue i USING (customer_id)
WHERE ABS(o.old_monthly_fee_usd      - i.monthly_fee_revenue_usd)      > 0.0001
   OR ABS(o.old_monthly_interest_usd - i.monthly_interest_revenue_usd) > 0.0001
ORDER BY ABS(o.old_total_revenue_usd - i.total_monthly_revenue_usd) DESC
LIMIT 10;