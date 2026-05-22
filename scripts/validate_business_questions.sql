-- ============================================================================
-- scripts/validate_business_questions.sql
-- ----------------------------------------------------------------------------
-- Validation script for the 24 business questions documented in
-- docs/business_questions.md. Runs one query per question with an \echo
-- separator so the operator can identify each result block in the output.
--
-- Usage (PowerShell, Windows host):
--     Get-Content scripts/validate_business_questions.sql | `
--         docker exec -i qversity_postgres psql -U qversity -d qversity_warehouse
--
-- Or copy to the container and run with -f:
--     docker cp scripts/validate_business_questions.sql qversity_postgres:/tmp/
--     docker exec -it qversity_postgres psql -U qversity -d qversity_warehouse -f /tmp/validate_business_questions.sql
--
-- Or interactive: paste blocks manually after `\x auto`.
-- ============================================================================

\x auto
\timing on
\pset null '<NULL>'

\echo
\echo ============================================================================
\echo PRELIMINARY — Sanity: confirm all gold marts exist
\echo ============================================================================

SELECT
    table_name,
    (SELECT n_live_tup
     FROM pg_stat_user_tables s
     WHERE s.schemaname = 'gold' AND s.relname = t.table_name) AS approx_rows
FROM information_schema.tables t
WHERE table_schema = 'gold'
ORDER BY table_name;

-- ============================================================================
-- REVENUE & PROFITABILITY
-- ============================================================================

\echo
\echo ============================================================================
\echo Q1 — Average revenue per customer by segment (USD)
\echo Mart: gold.mart_revenue_by_segment_usd
\echo Expected: 4 rows, one per segment, ordered desc by avg_revenue.
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
\echo Q2 — Total account balances by country (native currency, active accounts)
\echo Mart: gold.mart_account_mix
\echo Expected: ~15-30 rows (country × currency pairs, sparse).
\echo ============================================================================

SELECT
    country,
    currency,
    SUM(accounts_with_balance)        AS accounts_with_balance,
    ROUND(SUM(total_balance)::numeric, 2) AS total_balance_native_ccy,
    ROUND(AVG(avg_balance)::numeric, 2)   AS avg_balance_native_ccy
FROM gold.mart_account_mix
WHERE total_balance IS NOT NULL
GROUP BY country, currency
ORDER BY country, currency;

\echo
\echo ============================================================================
\echo Q3 — Revenue throughput by transaction channel (completed transactions)
\echo Mart: gold.mart_tx_by_channel
\echo Expected: ~25-40 rows (channel × currency).
\echo ============================================================================

SELECT
    channel,
    currency,
    SUM(completed_tx_count)                AS completed_tx_count,
    ROUND(SUM(total_value)::numeric, 2)    AS total_value_native_ccy,
    ROUND(AVG(avg_ticket)::numeric, 2)     AS avg_ticket_native_ccy
FROM gold.mart_tx_by_channel
WHERE total_value IS NOT NULL
GROUP BY channel, currency
ORDER BY channel, currency;

\echo
\echo ============================================================================
\echo Q4 — Interest income by loan type (monthly accrued, native currency)
\echo Mart: gold.mart_loan_composition
\echo Note: status filter excludes default and paid_off (no interest accrues).
\echo ============================================================================

SELECT
    loan_type,
    currency,
    SUM(loan_count)                                          AS active_loan_count,
    ROUND(SUM(total_monthly_interest_accrued)::numeric, 2)   AS monthly_interest_native_ccy
FROM gold.mart_loan_composition
WHERE status IN ('current', 'delinquent')
GROUP BY loan_type, currency
ORDER BY loan_type, currency;

-- ============================================================================
-- RISK & CREDIT
-- ============================================================================

\echo
\echo ============================================================================
\echo Q5 — Loan delinquency rate by customer segment
\echo Mart: gold.mart_delinquency_by_segment
\echo Note: denominator excludes customers without loans.
\echo ============================================================================

SELECT
    customer_segment,
    customer_count,
    ROUND(delinquency_rate::numeric, 4) AS delinquency_rate,
    ROUND(default_rate::numeric, 4)     AS default_rate
FROM gold.mart_delinquency_by_segment
ORDER BY delinquency_rate DESC NULLS LAST;

\echo
\echo ============================================================================
\echo Q6 — Credit score distribution by country (FICO buckets)
\echo Mart: gold.mart_credit_score_by_country
\echo Expected: ~42 rows (7 countries × up to 6 buckets).
\echo ============================================================================

SELECT
    country,
    credit_score_bucket,
    customer_count,
    ROUND(bucket_share_within_country::numeric, 4) AS share
FROM gold.mart_credit_score_by_country
ORDER BY country,
         CASE credit_score_bucket
             WHEN 'poor'        THEN 1
             WHEN 'fair'        THEN 2
             WHEN 'good'        THEN 3
             WHEN 'very_good'   THEN 4
             WHEN 'exceptional' THEN 5
             WHEN 'unknown'     THEN 6
         END;

\echo
\echo ============================================================================
\echo Q7 — Credit utilization vs delinquency rate
\echo Mart: gold.mart_utilization_vs_delinquency
\echo ============================================================================

SELECT
    utilization_bucket,
    customer_count,
    ROUND(delinquency_rate::numeric, 4) AS delinquency_rate
FROM gold.mart_utilization_vs_delinquency
ORDER BY
    CASE utilization_bucket
        WHEN 'healthy'    THEN 1
        WHEN 'moderate'   THEN 2
        WHEN 'high'       THEN 3
        WHEN 'maxed'      THEN 4
        WHEN 'over_limit' THEN 5
        WHEN 'unknown'    THEN 6
    END;

\echo
\echo ============================================================================
\echo Q8 — Days past due distribution by loan type
\echo Mart: gold.mart_loan_dpd
\echo ============================================================================

SELECT
    loan_type,
    dpd_bucket,
    currency,
    loan_count,
    ROUND(total_outstanding_balance::numeric, 2)             AS outstanding_balance,
    ROUND(avg_days_past_due::numeric, 1)                     AS avg_dpd,
    ROUND(bucket_share_within_type_currency::numeric, 4)     AS share
FROM gold.mart_loan_dpd
ORDER BY loan_type, currency, dpd_bucket;

\echo
\echo ============================================================================
\echo Q9 — Risk score segmentation with observed delinquency
\echo Mart: gold.mart_risk_buckets
\echo Note: see decisions.md Day 9 finding — risk_score is non-monotonic
\echo with delinquency in this synthetic dataset.
\echo ============================================================================

SELECT
    risk_bucket,
    customer_count,
    ROUND(bucket_share::numeric, 4)     AS share_of_customers,
    ROUND(delinquency_rate::numeric, 4) AS observed_delinquency_rate
FROM gold.mart_risk_buckets
ORDER BY
    CASE risk_bucket
        WHEN 'low'      THEN 1
        WHEN 'medium'   THEN 2
        WHEN 'high'     THEN 3
        WHEN 'critical' THEN 4
    END;

-- ============================================================================
-- CUSTOMER DEMOGRAPHICS
-- ============================================================================

\echo
\echo ============================================================================
\echo Q10 — Customer count by country and city (top 30 cities by volume)
\echo Mart: gold.mart_customer_360
\echo ============================================================================

SELECT
    country,
    city,
    COUNT(*) AS customer_count
FROM gold.mart_customer_360
GROUP BY country, city
ORDER BY customer_count DESC
LIMIT 30;

\echo
\echo ============================================================================
\echo Q11 — Age distribution by customer segment
\echo Mart: gold.mart_customer_360
\echo ============================================================================

SELECT
    customer_segment,
    age_bucket,
    COUNT(*) AS customer_count
FROM gold.mart_customer_360
GROUP BY customer_segment, age_bucket
ORDER BY customer_segment,
         CASE age_bucket
             WHEN 'under_18' THEN 1
             WHEN '18-25'    THEN 2
             WHEN '26-35'    THEN 3
             WHEN '36-50'    THEN 4
             WHEN '51-65'    THEN 5
             WHEN '65+'      THEN 6
             WHEN 'unknown'  THEN 7
         END;

\echo
\echo ============================================================================
\echo Q12 — Customer acquisition trend by month
\echo Mart: gold.mart_acquisition_trend
\echo ============================================================================

SELECT
    month_label,
    new_customers,
    cumulative_customers,
    ROUND(mom_growth_pct::numeric, 2) AS mom_growth_pct
FROM gold.mart_acquisition_trend
ORDER BY month;

\echo
\echo ============================================================================
\echo Q13 — Customer status breakdown (PARTIAL: see decisions.md Day 1)
\echo Mart: gold.mart_customer_360
\echo Spec values: active/inactive/suspended/closed.
\echo Customer-level status conforms; account-level status has 'frozen'.
\echo ============================================================================

SELECT
    status,
    COUNT(*) AS customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM gold.mart_customer_360
GROUP BY status
ORDER BY customer_count DESC;

\echo
\echo ============================================================================
\echo Q14 — KYC status distribution
\echo Mart: gold.mart_customer_360
\echo ============================================================================

SELECT
    kyc_status,
    COUNT(*) AS customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM gold.mart_customer_360
GROUP BY kyc_status
ORDER BY customer_count DESC;

-- ============================================================================
-- TRANSACTION PATTERNS
-- ============================================================================

\echo
\echo ============================================================================
\echo Q15a — Most common transaction categories by VOLUME (count)
\echo Mart: gold.mart_tx_by_category
\echo ============================================================================

SELECT
    category,
    SUM(completed_tx_count) AS completed_tx_count,
    SUM(tx_count)           AS total_tx_count
FROM gold.mart_tx_by_category
GROUP BY category
ORDER BY completed_tx_count DESC
LIMIT 15;

\echo
\echo ============================================================================
\echo Q15b — Most common transaction categories by VALUE (USD only, comparable)
\echo Mart: gold.mart_tx_by_category
\echo Note: filtering to a single currency to avoid mixing units.
\echo ============================================================================

SELECT
    category,
    completed_tx_count,
    tx_count
FROM gold.mart_tx_by_category
WHERE currency = 'USD'
ORDER BY completed_tx_count DESC;

\echo
\echo ============================================================================
\echo Q16 — Transaction volume by day of week
\echo Mart: gold.mart_tx_by_dow
\echo ============================================================================

SELECT
    day_of_week,
    day_of_week_name,
    SUM(tx_count) AS tx_count
FROM gold.mart_tx_by_dow
GROUP BY day_of_week, day_of_week_name
ORDER BY day_of_week;

\echo
\echo ============================================================================
\echo Q17 — Average transaction size by channel (native currency, completed only)
\echo Mart: gold.mart_tx_by_channel
\echo ============================================================================

SELECT
    channel,
    currency,
    ROUND(avg_ticket::numeric, 2) AS avg_ticket_native_ccy,
    completed_tx_count
FROM gold.mart_tx_by_channel
WHERE avg_ticket IS NOT NULL
ORDER BY channel, currency;

\echo
\echo ============================================================================
\echo Q18 — Failed transaction rate by channel (rolled up across currencies)
\echo Mart: gold.mart_tx_by_channel
\echo ============================================================================

SELECT
    channel,
    SUM(tx_count)        AS total_tx,
    SUM(failed_tx_count) AS failed_tx,
    ROUND(
        SUM(failed_tx_count)::numeric / NULLIF(SUM(tx_count), 0),
        4
    ) AS failed_rate
FROM gold.mart_tx_by_channel
GROUP BY channel
ORDER BY failed_rate DESC;

\echo
\echo ============================================================================
\echo Q19 — International transfer corridors (top 20 by international share)
\echo Mart: gold.mart_international_transfers
\echo Definition: see decisions.md Day 9 §8.
\echo ============================================================================

SELECT
    origin_country,
    tx_currency,
    tx_count,
    international_tx_count,
    ROUND(international_share::numeric, 4) AS international_share,
    ROUND(total_international_value::numeric, 2) AS total_intl_value
FROM gold.mart_international_transfers
WHERE international_tx_count > 0
ORDER BY international_share DESC, international_tx_count DESC
LIMIT 20;

-- ============================================================================
-- DIGITAL ENGAGEMENT
-- ============================================================================

\echo
\echo ============================================================================
\echo Q20 — Mobile app adoption rate by segment
\echo Mart: gold.mart_digital_adoption_by_segment
\echo ============================================================================

SELECT
    customer_segment,
    customer_count,
    ROUND(mobile_adoption_rate::numeric, 4)      AS mobile_adoption,
    ROUND(web_adoption_rate::numeric, 4)         AS web_adoption,
    ROUND(any_digital_adoption_rate::numeric, 4) AS any_digital_adoption
FROM gold.mart_digital_adoption_by_segment
ORDER BY mobile_adoption_rate DESC NULLS LAST;

\echo
\echo ============================================================================
\echo Q21 — Digital vs branch preference by age group
\echo Mart: gold.mart_channel_preference_by_age
\echo ============================================================================

SELECT
    age_bucket,
    SUM(CASE WHEN preferred_channel IN ('mobile', 'web')
             THEN customer_count ELSE 0 END) AS digital_customers,
    SUM(CASE WHEN preferred_channel IN ('atm', 'branch', 'phone')
             THEN customer_count ELSE 0 END) AS non_digital_customers,
    SUM(customer_count)                       AS total_customers,
    ROUND(
        SUM(CASE WHEN preferred_channel IN ('mobile', 'web')
                 THEN customer_count ELSE 0 END)::numeric
        / NULLIF(SUM(customer_count), 0),
        4
    ) AS digital_share
FROM gold.mart_channel_preference_by_age
GROUP BY age_bucket
ORDER BY
    CASE age_bucket
        WHEN 'under_18' THEN 1
        WHEN '18-25'    THEN 2
        WHEN '26-35'    THEN 3
        WHEN '36-50'    THEN 4
        WHEN '51-65'    THEN 5
        WHEN '65+'      THEN 6
        WHEN 'unknown'  THEN 7
    END;

-- ============================================================================
-- PRODUCT ANALYSIS
-- ============================================================================

\echo
\echo ============================================================================
\echo Q22 — Most popular account types (active only, rolled across countries)
\echo Mart: gold.mart_account_mix
\echo ============================================================================

SELECT
    account_type,
    SUM(accounts_count) AS total_accounts,
    ROUND(100.0 * SUM(accounts_count) / SUM(SUM(accounts_count)) OVER (), 2)
        AS pct_of_total
FROM gold.mart_account_mix
GROUP BY account_type
ORDER BY total_accounts DESC;

\echo
\echo ============================================================================
\echo Q23 — Loan portfolio composition (by type, status, currency)
\echo Mart: gold.mart_loan_composition
\echo ============================================================================

SELECT
    loan_type,
    status,
    currency,
    loan_count,
    ROUND(total_outstanding_balance::numeric, 2)              AS outstanding_balance,
    ROUND(outstanding_share_within_type_currency::numeric, 4) AS share_within_type
FROM gold.mart_loan_composition
ORDER BY loan_type, currency, status;

\echo
\echo ============================================================================
\echo Q24 — Average number of products per customer by segment
\echo Mart: gold.mart_customer_360
\echo Note: total_products = accounts + loans (transactions excluded — events, not products).
\echo ============================================================================

SELECT
    customer_segment,
    COUNT(*)                               AS customer_count,
    ROUND(AVG(accounts_count)::numeric, 2) AS avg_accounts,
    ROUND(AVG(loans_count)::numeric, 2)    AS avg_loans,
    ROUND(AVG(total_products)::numeric, 2) AS avg_total_products
FROM gold.mart_customer_360
GROUP BY customer_segment
ORDER BY avg_total_products DESC;

-- ============================================================================
-- DONE
-- ============================================================================

\echo
\echo ============================================================================
\echo Validation complete — 24 questions covered (23 full + Q13 partial).
\echo See docs/business_questions.md for the documented coverage.
\echo ============================================================================
