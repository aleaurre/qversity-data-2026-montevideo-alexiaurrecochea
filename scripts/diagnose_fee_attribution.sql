-- ============================================================================
-- scripts/diagnose_fee_attribution.sql
-- ----------------------------------------------------------------------------
-- Diagnostic for the Day 10 refactor finding: how often does a fee
-- transaction's executing customer (t.customer_id) differ from the account
-- owner (a.customer_id)?
--
-- Used to fill in the percentage placeholder in decisions.md Day 10
-- ("Finding 1: fee attribution diverged across marts").
-- ============================================================================

\x off
\timing on

SELECT
    COUNT(*)                                                     AS total_completed_fees,
    COUNT(*) FILTER (WHERE t.customer_id != a.customer_id)       AS divergent_fees,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE t.customer_id != a.customer_id)
              / NULLIF(COUNT(*), 0),
        4
    )                                                            AS divergent_pct
FROM silver.fct_transactions t
INNER JOIN silver.dim_account a ON t.account_id = a.account_id
WHERE t.transaction_type = 'fee'
  AND t.status = 'completed';

-- Optional: distribution of divergence by segment, to see if it concentrates
-- in any cohort. Useful for the decisions.md narrative if the pct is non-trivial.

SELECT
    c_executor.customer_segment   AS executor_segment,
    c_owner.customer_segment      AS owner_segment,
    COUNT(*)                      AS divergent_fee_count
FROM silver.fct_transactions t
INNER JOIN silver.dim_account a    ON t.account_id = a.account_id
INNER JOIN silver.dim_customer c_executor ON c_executor.customer_id = t.customer_id
INNER JOIN silver.dim_customer c_owner    ON c_owner.customer_id = a.customer_id
WHERE t.transaction_type = 'fee'
  AND t.status = 'completed'
  AND t.customer_id != a.customer_id
GROUP BY c_executor.customer_segment, c_owner.customer_segment
ORDER BY divergent_fee_count DESC;