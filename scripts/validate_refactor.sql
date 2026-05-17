-- ============================================================================
-- scripts/validate_refactor.sql
-- ----------------------------------------------------------------------------
-- Post-refactor sanity check. Run AFTER applying the Day 10 refactor that
-- introduces int_customer_monthly_revenue.
--
-- Expected outcome: every query below must return EXACTLY the same numbers
-- as the snapshot in scripts/validation_output.txt (May 17 2026).
-- If any value drifts, the refactor changed semantics — investigate before
-- committing.
--
-- Usage (PowerShell):
--   Get-Content scripts/validate_refactor.sql | `
--       docker exec -i qversity_postgres psql -U qversity -d qversity_warehouse
-- ============================================================================

\x auto
\timing on

\echo
\echo ============================================================================
\echo CHECK 1 — Q1 revenue by segment (USD).
\echo Expected snapshot:
\echo   sme              1266  2890.38  1396.00  1494.38  0.483
\echo   premium          1230  2676.87  1200.68  1476.19  0.449
\echo   retail           1280  2635.99  1250.08  1385.91  0.474
\echo   private_banking  1224  2482.45  1031.23  1451.22  0.415
\echo ============================================================================

SELECT
    customer_segment,
    customer_count,
    ROUND(avg_revenue_per_customer_usd::numeric, 2)          AS avg_revenue_usd,
    ROUND(avg_fee_revenue_per_customer_usd::numeric, 2)      AS avg_fee_usd,
    ROUND(avg_interest_revenue_per_customer_usd::numeric, 2) AS avg_interest_usd,
    ROUND(fee_revenue_share::numeric, 3)                     AS fee_share
FROM gold.mart_revenue_by_segment_usd
ORDER BY avg_revenue_per_customer_usd DESC;

\echo
\echo ============================================================================
\echo CHECK 2 — mart_customer_360 revenue columns rolled up to segment.
\echo Should match Q1 above for fee+interest+total monthly metrics.
\echo This validates that mart_customer_360 (native currency) is internally
\echo consistent with mart_revenue_by_segment_usd (USD). Numbers will NOT
\echo match Q1 directly (native vs USD), but the ranking must be the same.
\echo ============================================================================

SELECT
    customer_segment,
    COUNT(*)                                       AS customer_count,
    ROUND(AVG(total_revenue_monthly)::numeric, 2)  AS avg_total_revenue_native,
    ROUND(AVG(monthly_fee_revenue)::numeric, 2)    AS avg_fee_revenue_native,
    ROUND(AVG(monthly_interest_income)::numeric, 2) AS avg_interest_revenue_native
FROM gold.mart_customer_360
GROUP BY customer_segment
ORDER BY avg_total_revenue_native DESC;

\echo
\echo ============================================================================
\echo CHECK 3 — int_customer_monthly_revenue row count and basic invariants.
\echo Expected: 5000 rows (one per customer in dim_customer with risk_score
\echo non-null), all monetary columns >= 0, no NULLs.
\echo ============================================================================

SELECT
    COUNT(*)                                          AS row_count,
    COUNT(*) FILTER (WHERE customer_id IS NULL)       AS null_pk,
    COUNT(*) FILTER (WHERE monthly_fee_revenue_native  < 0) AS negative_fee_native,
    COUNT(*) FILTER (WHERE monthly_fee_revenue_usd     < 0) AS negative_fee_usd,
    COUNT(*) FILTER (WHERE monthly_interest_revenue_native < 0) AS negative_int_native,
    COUNT(*) FILTER (WHERE monthly_interest_revenue_usd    < 0) AS negative_int_usd,
    COUNT(*) FILTER (WHERE total_monthly_revenue_native < 0) AS negative_total_native,
    COUNT(*) FILTER (WHERE total_monthly_revenue_usd    < 0) AS negative_total_usd
FROM silver.int_customer_monthly_revenue;

\echo
\echo ============================================================================
\echo CHECK 4 — Spot-check 5 customers across mart_customer_360 vs
\echo int_customer_monthly_revenue. Values must match per-customer.
\echo ============================================================================

SELECT
    m.customer_id,
    m.monthly_fee_revenue        AS mart_fee,
    i.monthly_fee_revenue_native AS int_fee_native,
    m.monthly_interest_income        AS mart_int,
    i.monthly_interest_revenue_native AS int_int_native,
    m.total_revenue_monthly       AS mart_total,
    i.total_monthly_revenue_native AS int_total_native,
    CASE
        WHEN m.monthly_fee_revenue       = i.monthly_fee_revenue_native
         AND m.monthly_interest_income   = i.monthly_interest_revenue_native
         AND m.total_revenue_monthly     = i.total_monthly_revenue_native
        THEN 'OK'
        ELSE 'MISMATCH'
    END AS match_status
FROM gold.mart_customer_360 m
JOIN silver.int_customer_monthly_revenue i USING (customer_id)
ORDER BY m.total_revenue_monthly DESC
LIMIT 5;

\echo
\echo ============================================================================
\echo CHECK 5 — Aggregate match: mart_revenue_by_segment_usd should equal
\echo a direct rollup from int_customer_monthly_revenue. Both numbers must
\echo agree exactly.
\echo ============================================================================

SELECT
    'from_mart'        AS source,
    customer_segment,
    customer_count,
    ROUND(avg_revenue_per_customer_usd::numeric, 4) AS avg_revenue_usd
FROM gold.mart_revenue_by_segment_usd

UNION ALL

SELECT
    'from_intermediate' AS source,
    c.customer_segment,
    COUNT(*)             AS customer_count,
    ROUND(AVG(i.total_monthly_revenue_usd)::numeric, 4) AS avg_revenue_usd
FROM silver.int_customer_monthly_revenue i
INNER JOIN silver.dim_customer c USING (customer_id)
GROUP BY c.customer_segment

ORDER BY customer_segment, source;

\echo
\echo ============================================================================
\echo Validation complete. Compare numbers manually against the snapshots above.
\echo ============================================================================