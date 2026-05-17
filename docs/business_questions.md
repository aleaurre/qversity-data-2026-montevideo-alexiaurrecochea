# Business Questions — Gold Layer Coverage Map

> **Purpose.** Map each of the 24 business questions in the project brief to
> the Gold mart(s) that answer it, with an executable SQL query against the
> `gold` schema and the expected shape of the result.
>
> **Coverage status legend:**
>
> - ✅ **Covered** — answerable directly from a Gold mart.
> - ⚠️ **Partial** — answerable with documented caveat (dataset divergence
>   from spec, scope adjustment, etc.). Still counts toward coverage.
> - ❌ **Out of scope** — not answerable from the current Gold layer.
>   Documented reason; does not count toward coverage.
>
> **Coverage target:** ≥ 21 / 24.
> **Coverage achieved:** 24 / 24 (23 fully covered, 1 partial with documented
> dataset divergence — see Q13).
>
> Business-logic definitions (revenue, delinquency, age buckets, risk buckets,
> utilization buckets, currency strategy, customer tenure) are documented in
> [`decisions.md`](./decisions.md). This document references them rather than
> redefining them.

---

## Coverage Summary

| #  | Theme               | Question                                                     | Status | Primary mart                          | Power BI page          |
|----|---------------------|--------------------------------------------------------------|--------|---------------------------------------|------------------------|
| 1  | Revenue             | Avg revenue per customer by segment                          | ✅     | `mart_revenue_by_segment_usd`         | Executive Overview     |
| 2  | Revenue             | Total account balances by country                            | ✅     | `mart_account_mix`                    | Executive Overview     |
| 3  | Revenue             | Revenue breakdown by transaction channel                     | ✅     | `mart_tx_by_channel`                  | Revenue & Transactions |
| 4  | Revenue             | Interest income by loan type                                 | ✅     | `mart_loan_composition`               | Risk & Credit          |
| 5  | Risk                | Loan delinquency rate by customer segment                    | ✅     | `mart_delinquency_by_segment`         | Risk & Credit          |
| 6  | Risk                | Credit score distribution by country                         | ✅     | `mart_credit_score_by_country`        | Risk & Credit          |
| 7  | Risk                | Credit utilization vs delinquency relationship               | ✅     | `mart_utilization_vs_delinquency`     | Risk & Credit          |
| 8  | Risk                | Days past due distribution by loan type                      | ✅     | `mart_loan_dpd`                       | Risk & Credit          |
| 9  | Risk                | Risk-score segmentation (low/medium/high/critical)           | ✅     | `mart_risk_buckets`                   | Risk & Credit          |
| 10 | Demographics        | Customer count by country and city                           | ✅     | `mart_customer_360`                   | Customer & Engagement  |
| 11 | Demographics        | Age distribution by customer segment                         | ✅     | `mart_customer_360`                   | Customer & Engagement  |
| 12 | Demographics        | Customer acquisition trend over time (monthly)               | ✅     | `mart_acquisition_trend`              | Customer & Engagement  |
| 13 | Demographics        | Customer status breakdown                                    | ⚠️     | `mart_customer_360`                   | Executive Overview     |
| 14 | Demographics        | KYC status distribution                                      | ✅     | `mart_customer_360`                   | Customer & Engagement  |
| 15 | Transactions        | Most common transaction categories by volume and value       | ✅     | `mart_tx_by_category`                 | Revenue & Transactions |
| 16 | Transactions        | Transaction volume by day of week                            | ✅     | `mart_tx_by_dow`                      | Revenue & Transactions |
| 17 | Transactions        | Average transaction size by channel                          | ✅     | `mart_tx_by_channel`                  | Revenue & Transactions |
| 18 | Transactions        | Failed transaction rate by channel                           | ✅     | `mart_tx_by_channel`                  | Revenue & Transactions |
| 19 | Transactions        | International transfer patterns                              | ✅     | `mart_international_transfers`        | Revenue & Transactions |
| 20 | Digital engagement  | Mobile app adoption rate by segment                          | ✅     | `mart_digital_adoption_by_segment`    | Customer & Engagement  |
| 21 | Digital engagement  | Digital vs branch preference by age group                    | ✅     | `mart_channel_preference_by_age`      | Customer & Engagement  |
| 22 | Products            | Most popular account types                                   | ✅     | `mart_account_mix`                    | Executive Overview     |
| 23 | Products            | Loan portfolio composition (outstanding by type and status)  | ✅     | `mart_loan_composition`               | Risk & Credit          |
| 24 | Products            | Average number of products per customer by segment           | ✅     | `mart_customer_360`                   | Executive Overview     |

**Result: 24 / 24 (23 covered + 1 partial with documented divergence).**

---

# Revenue & Profitability

## Q1 — Average revenue per customer by segment

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_revenue_by_segment_usd` &nbsp;·&nbsp; **Dashboard page:** Executive Overview

**Business definition** (see `decisions.md` §1 + Day 9 corrections):
Revenue = monthly fee revenue (lifetime completed fees ÷ tenure_months)
+ monthly interest accrued on active loans (status ∈ {`current`, `delinquent`}).
All amounts converted to USD via the `fx_rates` seed (May 2026 snapshot).
Zero-tenure customers contribute 0 fee revenue (preserves population count).

### Query

```sql
SELECT
    customer_segment,
    customer_count,
    ROUND(avg_revenue_per_customer_usd::numeric, 2)          AS avg_revenue_usd,
    ROUND(avg_fee_revenue_per_customer_usd::numeric, 2)      AS avg_fee_usd,
    ROUND(avg_interest_revenue_per_customer_usd::numeric, 2) AS avg_interest_usd,
    ROUND(fee_revenue_share::numeric, 3)                     AS fee_share
FROM gold.mart_revenue_by_segment_usd
ORDER BY avg_revenue_per_customer_usd DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| customer_segment | customer_count | avg_revenue_usd | avg_fee_usd | avg_interest_usd | fee_share |
|---|---|---|---|---|---|
| sme             | 1266 | 2897.17 | 1402.79 | 1494.38 | 0.484 |
| premium         | 1230 | 2666.19 | 1190.00 | 1476.19 | 0.446 |
| retail          | 1280 | 2583.02 | 1197.11 | 1385.91 | 0.463 |
| private_banking | 1224 | 2534.46 | 1083.24 | 1451.22 | 0.427 |

4 rows, one per segment, sorted descending by avg_revenue.

### Notes

- Revenue is **monthly**, not lifetime or annual. Day 9 fix established
  temporal consistency between fees (originally lifetime) and interest
  (already monthly). See `decisions.md` Day 9.
- `fee_revenue_share` is NULL when the segment has zero total revenue.
- **Business observation:** the four segments cluster tightly in the
  $2.4 k – $2.9 k range. In real-world banking, `private_banking` would
  typically be 5-50× `retail`. The flatness is a property of the synthetic
  data generator (it does not correlate financial dimensions with segment
  in realistic ways), not a pipeline bug. See **Key findings** at the
  bottom of this document.

---

## Q2 — Total account balances by country

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_account_mix` &nbsp;·&nbsp; **Dashboard page:** Executive Overview

**Business definition:**
Only **active** accounts (`status = 'active'`) are counted — closed and frozen
accounts do not represent live held balance. Balances are reported in **source
currency** (no FX conversion at this grain) so sums never mix incompatible
units. ~3.1 % of active accounts have NULL balance (synthetic-dataset noise,
uniformly distributed — `decisions.md` Day 8).

### Query

```sql
SELECT
    country,
    currency,
    SUM(accounts_with_balance) AS accounts_with_balance,
    SUM(total_balance)         AS total_balance_native_ccy
FROM gold.mart_account_mix
WHERE total_balance IS NOT NULL
GROUP BY country, currency
ORDER BY country, currency;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| country | currency | accounts_with_balance | total_balance_native_ccy | avg_balance_native_ccy |
|---|---|---|---|---|
| AR | ARS | 403 | 97270356185.31 | 241651721.45 |
| AR | USD | 411 | 100903692.11 | 246136.13 |
| BR | BRL | 402 | 491192098.90 | 1221370.07 |
| BR | USD | 451 | 108669379.18 | 241677.12 |
| CL | CLP | 360 | 85730821391.66 | 239255542.30 |
| CL | USD | 398 | 95909038.74 | 241705.65 |
| CO | COP | 403 | 406955392586.38 | 1008211199.21 |
| CO | USD | 360 | 86930254.05 | 241802.86 |
| MX | MXN | 419 | 1860869525.61 | 4450414.72 |
| MX | USD | 419 | 107537335.23 | 256511.76 |
| PE | PEN | 417 | 391118039.16 | 937615.35 |
| PE | USD | 430 | 109477047.73 | 254089.59 |
| UY | USD | 423 | 100283302.94 | 236974.03 |
| UY | UYU | 398 | 4047826380.67 | 10170613.64 |

14 rows.

### Notes

- Power BI page "Executive Overview" can layer a USD-equivalent total on top
  by joining with the `fx_rates` seed at visualization time, or by filtering
  to `currency = 'USD'` for the comparable slice.
- See `decisions.md` for the regional dollarization pattern observed in the
  source data — relevant context for interpreting per-country totals.

---

## Q3 — Revenue breakdown by transaction channel

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_tx_by_channel` &nbsp;·&nbsp; **Dashboard page:** Revenue & Transactions

**Business definition:**
"Revenue" here is the **transactional throughput** routed through each channel
(value of completed transactions), not P&L revenue. The mart's grain is
(`channel`, `currency`) to keep sums dimensionally consistent.

### Query

```sql
SELECT
    channel,
    currency,
    SUM(completed_tx_count) AS completed_tx_count,
    SUM(total_value)        AS total_value_native_ccy,
    ROUND(AVG(avg_ticket)::numeric, 2) AS avg_ticket_native_ccy
FROM gold.mart_tx_by_channel
GROUP BY channel, currency
ORDER BY channel, currency;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| channel | currency | completed_tx_count | total_value_native_ccy | avg_ticket_native_ccy |
|---|---|---|---|---|
| atm | ARS | 318 | 8053278177.06 | 25324774.14 |
| atm | BRL | 326 | 41630043.49 | 127699.52 |
| atm | CLP | 279 | 6242975977.19 | 22376257.98 |
| atm | COP | 320 | 32671255864.40 | 102097674.58 |
| atm | EUR | 47 | 1020119.79 | 21704.68 |
| atm | MXN | 301 | 145407504.32 | 483081.41 |
| atm | PEN | 344 | 31675467.29 | 92079.85 |
| atm | USD | 1942 | 46917672.45 | 24159.46 |
| atm | UYU | 320 | 309731252.06 | 967910.16 |
| branch | ARS | 322 | 7905658544.17 | 24551734.61 |
| branch | BRL | 275 | 35626714.74 | 129551.69 |
| branch | CLP | 313 | 7344831027.11 | 23465913.82 |
| branch | COP | 315 | 33717400667.32 | 107039367.20 |
| branch | EUR | 55 | 1077585.65 | 19592.47 |
| branch | MXN | 362 | 164320834.62 | 453924.96 |
| branch | PEN | 354 | 32822519.85 | 92718.98 |
| branch | USD | 2005 | 48882304.59 | 24380.20 |
| branch | UYU | 312 | 304600515.57 | 976283.70 |
| mobile | ARS | 326 | 8531526597.70 | 26170326.99 |
| mobile | BRL | 328 | 40083397.71 | 122205.48 |
| mobile | CLP | 299 | 7014357861.09 | 23459390.84 |
| mobile | COP | 285 | 26764020478.16 | 93908843.78 |
| mobile | EUR | 51 | 1108748.01 | 21740.16 |
| mobile | MXN | 364 | 162408894.92 | 446178.28 |
| mobile | PEN | 319 | 29451312.02 | 92323.86 |
| mobile | USD | 2024 | 49925171.49 | 24666.59 |
| mobile | UYU | 327 | 313449361.52 | 958560.74 |
| pos | ARS | 354 | 8654676231.94 | 24448237.94 |
| pos | BRL | 308 | 35490661.96 | 115229.42 |
| pos | CLP | 297 | 7200912970.15 | 24245498.22 |
| pos | COP | 291 | 28483180649.03 | 97880345.87 |
| pos | EUR | 50 | 1027816.05 | 20556.32 |
| pos | MXN | 328 | 145347440.61 | 443132.44 |
| pos | PEN | 338 | 31162792.67 | 92197.61 |
| pos | USD | 1968 | 49855690.45 | 25333.18 |
| pos | UYU | 317 | 300992221.47 | 949502.28 |
| web | ARS | 340 | 8563603335.37 | 25187068.63 |
| web | BRL | 311 | 39842810.52 | 128111.93 |
| web | CLP | 298 | 6960281365.73 | 23356648.88 |
| web | COP | 333 | 32032070709.79 | 96192404.53 |
| web | EUR | 34 | 763025.31 | 22441.92 |
| web | MXN | 322 | 151155641.62 | 469427.46 |
| web | PEN | 300 | 27920021.47 | 93066.74 |
| web | USD | 1924 | 47866836.26 | 24878.81 |
| web | UYU | 309 | 322617556.46 | 1044069.76 |

45 rows.

### Notes

- For P&L revenue (fees + interest), see Q1.
- Failed and pending transactions are excluded from `total_value` (they are
  not realized throughput). Their counts are available in the same mart
  (see Q18).

---

## Q4 — Interest income by loan type

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_loan_composition` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

**Business definition:**
Interest income = `outstanding_balance × interest_rate_decimal / 12`,
aggregated over active loans (status ∈ {`current`, `delinquent`}). Default
and paid-off loans accrue no interest. Per `decisions.md` Day 9, the
`interest_rate_decimal` is on the 0–1 scale.

### Query

```sql
SELECT
    loan_type,
    currency,
    SUM(loan_count)                      AS active_loan_count,
    SUM(total_monthly_interest_accrued)  AS monthly_interest_income_native_ccy
FROM gold.mart_loan_composition
WHERE status IN ('current', 'delinquent')
GROUP BY loan_type, currency
ORDER BY loan_type, currency;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| loan_type | currency | active_loan_count | monthly_interest_native_ccy |
|---|---|---|---|
| auto | ARS | 61 | 99416828.01 |
| auto | BRL | 49 | 400932.80 |
| auto | CLP | 48 | 98112328.34 |
| auto | COP | 75 | 430035790.72 |
| auto | MXN | 57 | 2465090.89 |
| auto | PEN | 62 | 395548.88 |
| auto | USD | 399 | 775637.92 |
| auto | UYU | 55 | 3951357.81 |
| business | ARS | 47 | 80269745.26 |
| business | BRL | 53 | 526747.50 |
| business | CLP | 58 | 112065252.20 |
| business | COP | 55 | 450393352.53 |
| business | MXN | 53 | 1646337.87 |
| business | PEN | 60 | 404995.17 |
| business | USD | 394 | 786535.40 |
| business | UYU | 44 | 4445403.13 |
| education | ARS | 51 | 92413763.89 |
| education | BRL | 54 | 554326.53 |
| education | CLP | 61 | 119576086.02 |
| education | COP | 60 | 427460981.50 |
| education | MXN | 40 | 1164383.47 |
| education | PEN | 42 | 353759.69 |
| education | USD | 372 | 717764.27 |
| education | UYU | 59 | 3840935.89 |
| mortgage | ARS | 42 | 84412839.20 |
| mortgage | BRL | 66 | 600674.45 |
| mortgage | CLP | 52 | 91911417.85 |
| mortgage | COP | 48 | 338329013.64 |
| mortgage | MXN | 66 | 2267761.28 |
| mortgage | PEN | 53 | 292125.51 |
| mortgage | USD | 389 | 737010.95 |
| mortgage | UYU | 54 | 5201414.20 |
| personal | ARS | 64 | 107344439.78 |
| personal | BRL | 73 | 570390.80 |
| personal | CLP | 44 | 69070452.41 |
| personal | COP | 36 | 187450304.03 |
| personal | MXN | 58 | 2091577.06 |
| personal | PEN | 53 | 412713.96 |
| personal | USD | 394 | 712359.42 |
| personal | UYU | 63 | 5393792.58 |

40 rows.

### Notes

- Reported in source currency. Power BI can convert via `fx_rates` join for
  a USD-comparable view.
- Default and paid_off loans are intentionally excluded — they generate no
  ongoing interest.

---

# Risk & Credit

## Q5 — Loan delinquency rate by customer segment

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_delinquency_by_segment` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

**Business definition:**
Delinquency rate = share of customers (with at least one loan) who have ≥ 1
delinquent loan. **Denominator excludes customers without loan exposure** —
they cannot be delinquent on a product they don't hold.

### Query

```sql
SELECT
    customer_segment,
    customer_count,
    ROUND(delinquency_rate::numeric, 4) AS delinquency_rate,
    ROUND(default_rate::numeric, 4)     AS default_rate
FROM gold.mart_delinquency_by_segment
ORDER BY delinquency_rate DESC NULLS LAST;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| customer_segment | customer_count | delinquency_rate | default_rate |
|---|---|---|---|
| private_banking | 1224 | 0.6462 | 0.4312 |
| premium         | 1230 | 0.6342 | 0.4340 |
| sme             | 1266 | 0.6314 | 0.4375 |
| retail          | 1280 | 0.6221 | 0.4144 |

4 rows, one per segment.

### Notes

- **Headline rates are unrealistic by industry standards.** Real LATAM
  retail-banking portfolios run delinquency at 2-8 %, default at < 2 %.
  Observed ~63 % delinquent / ~43 % default reflect the synthetic data
  generator's distribution, not pipeline error. See **Key findings**
  at the bottom of this document.
- The *ordering* across segments (private_banking highest, retail lowest)
  is also counterintuitive — in real banking, retail typically has the
  highest delinquency. This is consistent with the broader finding that
  the generator does not correlate financial dimensions with customer
  attributes in realistic ways.

---

## Q6 — Credit score distribution by country

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_credit_score_by_country` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

**Business definition:**
Credit-score buckets follow the FICO convention (`poor / fair / good /
very_good / exceptional`); customers with corrupt credit_score values
(~10 % of records, `decisions.md` Day 1) fall into `unknown`.

### Query

```sql
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
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| country | credit_score_bucket | customer_count | share |
|---|---|---|---|
| AR | poor | 332 | 0.4689 |
| AR | fair | 101 | 0.1427 |
| AR | good | 66 | 0.0932 |
| AR | very_good | 58 | 0.0819 |
| AR | exceptional | 68 | 0.0960 |
| AR | unknown | 83 | 0.1172 |
| BR | poor | 333 | 0.4518 |
| BR | fair | 89 | 0.1208 |
| BR | good | 107 | 0.1452 |
| BR | very_good | 65 | 0.0882 |
| BR | exceptional | 63 | 0.0855 |
| BR | unknown | 80 | 0.1085 |
| CL | poor | 320 | 0.4805 |
| CL | fair | 90 | 0.1351 |
| CL | good | 74 | 0.1111 |
| CL | very_good | 63 | 0.0946 |
| CL | exceptional | 53 | 0.0796 |
| CL | unknown | 66 | 0.0991 |
| CO | poor | 330 | 0.4688 |
| CO | fair | 98 | 0.1392 |
| CO | good | 80 | 0.1136 |
| CO | very_good | 75 | 0.1065 |
| CO | exceptional | 58 | 0.0824 |
| CO | unknown | 63 | 0.0895 |
| MX | poor | 348 | 0.4847 |
| MX | fair | 80 | 0.1114 |
| MX | good | 84 | 0.1170 |
| MX | very_good | 81 | 0.1128 |
| MX | exceptional | 58 | 0.0808 |
| MX | unknown | 67 | 0.0933 |
| PE | poor | 334 | 0.4532 |
| PE | fair | 110 | 0.1493 |
| PE | good | 79 | 0.1072 |
| PE | very_good | 68 | 0.0923 |
| PE | exceptional | 59 | 0.0801 |
| PE | unknown | 87 | 0.1180 |
| UY | poor | 339 | 0.4644 |
| UY | fair | 102 | 0.1397 |
| UY | good | 84 | 0.1151 |
| UY | very_good | 59 | 0.0808 |
| UY | exceptional | 77 | 0.1055 |
| UY | unknown | 69 | 0.0945 |

42 rows.

---

## Q7 — Credit utilization vs delinquency relationship

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_utilization_vs_delinquency` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

**Business definition:**
Customers bucketed by `utilization_pct` (`healthy / moderate / high / maxed /
over_limit / unknown` — see `decisions.md` Day 9 §4). Delinquency rate
computed within each bucket. The shape of this distribution is the answer to
"is there a relationship".

### Query

```sql
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
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| utilization_bucket | customer_count | delinquency_rate |
|---|---|---|
| healthy | 1468 | 0.6438 |
| moderate | 1520 | 0.6162 |
| high | 1504 | 0.6421 |
| maxed | 489 | 0.6307 |
| unknown | 19 | 0.5833 |

5 rows.

### Notes

- The visual answer (Power BI line/bar chart): if `delinquency_rate` rises
  monotonically with bucket order, there is a positive relationship; flat
  curve = no relationship.

---

## Q8 — Days past due distribution by loan type

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_loan_dpd` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

**Business definition:**
DPD buckets follow industry conventions for credit-risk reporting:
`00 - Current` / `01 - Early (1-29)` / `02 - 30-59 DPD` / `03 - 60-89 DPD` /
`04 - 90-179 DPD` / `05 - 180+ DPD`. Prefix-numbered for natural sort order
in Power BI.

### Query

```sql
SELECT
    loan_type,
    dpd_bucket,
    currency,
    loan_count,
    ROUND(bucket_share_within_type_currency::numeric, 4) AS share
FROM gold.mart_loan_dpd
ORDER BY loan_type, currency, dpd_bucket;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| loan_type | dpd_bucket | currency | loan_count | outstanding_balance | avg_dpd | share |
|---|---|---|---|---|---|---|
| auto | 00 - Current | ARS | 63 | 3304810022.57 | 0.0 | 0.5385 |
| auto | 01 - Early (1-29) | ARS | 11 | 1299979946.73 | 19.4 | 0.0940 |
| auto | 02 - 30-59 DPD | ARS | 10 | 1671146552.98 | 42.4 | 0.0855 |
| auto | 03 - 60-89 DPD | ARS | 6 | 365224002.90 | 74.2 | 0.0513 |
| auto | 04 - 90-179 DPD | ARS | 9 | 1562762747.18 | 143.1 | 0.0769 |
| auto | 05 - 180+ DPD | ARS | 18 | 2008758687.47 | 278.8 | 0.1538 |
| auto | 00 - Current | BRL | 59 | 15885572.50 | 0.0 | 0.5619 |
| auto | 01 - Early (1-29) | BRL | 7 | 4727268.76 | 20.4 | 0.0667 |
| auto | 02 - 30-59 DPD | BRL | 2 | 2872323.54 | 38.0 | 0.0190 |
| auto | 03 - 60-89 DPD | BRL | 10 | 4471719.95 | 73.2 | 0.0952 |
| auto | 04 - 90-179 DPD | BRL | 11 | 9114168.63 | 125.2 | 0.1048 |
| auto | 05 - 180+ DPD | BRL | 16 | 6074426.91 | 265.0 | 0.1524 |
| auto | 00 - Current | CLP | 52 | 3136188019.86 | 0.0 | 0.4906 |
| auto | 01 - Early (1-29) | CLP | 9 | 675092752.33 | 14.4 | 0.0849 |
| auto | 02 - 30-59 DPD | CLP | 6 | 918609880.45 | 44.5 | 0.0566 |
| auto | 03 - 60-89 DPD | CLP | 8 | 888470894.26 | 78.0 | 0.0755 |
| auto | 04 - 90-179 DPD | CLP | 12 | 1168966209.89 | 122.0 | 0.1132 |
| auto | 05 - 180+ DPD | CLP | 19 | 2368073685.10 | 262.3 | 0.1792 |
| auto | 00 - Current | COP | 62 | 16753655134.33 | 0.0 | 0.4806 |
| auto | 01 - Early (1-29) | COP | 12 | 7351407666.14 | 16.6 | 0.0930 |
| auto | 02 - 30-59 DPD | COP | 11 | 4787250735.16 | 45.8 | 0.0853 |
| auto | 03 - 60-89 DPD | COP | 16 | 5012294755.24 | 71.8 | 0.1240 |
| auto | 04 - 90-179 DPD | COP | 12 | 4733919421.13 | 129.3 | 0.0930 |
| auto | 05 - 180+ DPD | COP | 16 | 7565538339.66 | 274.2 | 0.1240 |
| auto | 00 - Current | MXN | 61 | 76246121.93 | 0.0 | 0.5351 |
| auto | 01 - Early (1-29) | MXN | 7 | 17988929.34 | 15.7 | 0.0614 |
| auto | 02 - 30-59 DPD | MXN | 8 | 28752269.24 | 45.0 | 0.0702 |
| auto | 03 - 60-89 DPD | MXN | 11 | 33945606.68 | 76.1 | 0.0965 |
| auto | 04 - 90-179 DPD | MXN | 10 | 18182369.64 | 125.5 | 0.0877 |
| auto | 05 - 180+ DPD | MXN | 17 | 42547416.00 | 277.9 | 0.1491 |
| auto | 00 - Current | PEN | 57 | 15742739.36 | 0.0 | 0.5534 |
| auto | 01 - Early (1-29) | PEN | 10 | 2033751.57 | 16.0 | 0.0971 |
| auto | 02 - 30-59 DPD | PEN | 13 | 6002103.57 | 43.7 | 0.1262 |
| auto | 03 - 60-89 DPD | PEN | 6 | 1090228.49 | 73.8 | 0.0583 |
| auto | 04 - 90-179 DPD | PEN | 7 | 3356066.43 | 121.3 | 0.0680 |
| auto | 05 - 180+ DPD | PEN | 10 | 5732470.53 | 257.0 | 0.0971 |
| auto | 00 - Current | USD | 363 | 21647807.66 | 0.0 | 0.4808 |
| auto | 01 - Early (1-29) | USD | 67 | 10812215.96 | 14.9 | 0.0887 |
| auto | 02 - 30-59 DPD | USD | 59 | 6892381.60 | 42.5 | 0.0781 |
| auto | 03 - 60-89 DPD | USD | 73 | 9704407.11 | 74.7 | 0.0967 |
| auto | 04 - 90-179 DPD | USD | 55 | 8199889.09 | 133.9 | 0.0728 |
| auto | 05 - 180+ DPD | USD | 138 | 16864067.52 | 272.7 | 0.1828 |
| auto | 00 - Current | UYU | 52 | 132245919.35 | 0.0 | 0.4906 |
| auto | 01 - Early (1-29) | UYU | 9 | 43158749.10 | 13.4 | 0.0849 |
| auto | 02 - 30-59 DPD | UYU | 8 | 35161258.37 | 45.3 | 0.0755 |
| auto | 03 - 60-89 DPD | UYU | 9 | 39168092.24 | 74.9 | 0.0849 |
| auto | 04 - 90-179 DPD | UYU | 12 | 39034710.01 | 127.4 | 0.1132 |
| auto | 05 - 180+ DPD | UYU | 16 | 110008541.92 | 270.3 | 0.1509 |
| business | 00 - Current | ARS | 42 | 2319381935.72 | 0.0 | 0.4200 |
| business | 01 - Early (1-29) | ARS | 11 | 1108298137.97 | 13.5 | 0.1100 |
| business | 02 - 30-59 DPD | ARS | 8 | 1144680902.39 | 41.9 | 0.0800 |
| business | 03 - 60-89 DPD | ARS | 7 | 519687673.54 | 76.9 | 0.0700 |
| business | 04 - 90-179 DPD | ARS | 9 | 631917862.14 | 129.7 | 0.0900 |
| business | 05 - 180+ DPD | ARS | 23 | 2905527385.31 | 278.0 | 0.2300 |
| business | 00 - Current | BRL | 46 | 19743366.61 | 0.0 | 0.4694 |
| business | 01 - Early (1-29) | BRL | 9 | 3657290.85 | 14.9 | 0.0918 |
| business | 02 - 30-59 DPD | BRL | 7 | 4649918.64 | 44.0 | 0.0714 |
| business | 03 - 60-89 DPD | BRL | 7 | 3827991.53 | 69.4 | 0.0714 |
| business | 04 - 90-179 DPD | BRL | 10 | 8198425.41 | 122.6 | 0.1020 |
| business | 05 - 180+ DPD | BRL | 19 | 11343732.70 | 268.1 | 0.1939 |
| business | 00 - Current | CLP | 53 | 3413092245.66 | 0.0 | 0.5248 |
| business | 01 - Early (1-29) | CLP | 10 | 1406864314.09 | 16.1 | 0.0990 |
| business | 02 - 30-59 DPD | CLP | 8 | 786718837.46 | 46.1 | 0.0792 |
| business | 03 - 60-89 DPD | CLP | 7 | 1138391562.91 | 75.7 | 0.0693 |
| business | 04 - 90-179 DPD | CLP | 9 | 1168585165.28 | 144.2 | 0.0891 |
| business | 05 - 180+ DPD | CLP | 14 | 1559224546.85 | 258.1 | 0.1386 |
| business | 00 - Current | COP | 54 | 16072904728.47 | 0.0 | 0.5047 |
| business | 01 - Early (1-29) | COP | 6 | 2557568038.78 | 17.0 | 0.0561 |
| business | 02 - 30-59 DPD | COP | 7 | 5232541956.91 | 45.1 | 0.0654 |
| business | 03 - 60-89 DPD | COP | 11 | 6134268145.18 | 73.3 | 0.1028 |
| business | 04 - 90-179 DPD | COP | 14 | 8151151397.90 | 132.8 | 0.1308 |
| business | 05 - 180+ DPD | COP | 15 | 4933775557.81 | 262.7 | 0.1402 |
| business | 00 - Current | MXN | 51 | 58167928.77 | 0.0 | 0.5667 |
| business | 01 - Early (1-29) | MXN | 7 | 18421893.42 | 16.9 | 0.0778 |
| business | 02 - 30-59 DPD | MXN | 9 | 23908086.46 | 44.7 | 0.1000 |
| business | 03 - 60-89 DPD | MXN | 6 | 11767705.52 | 69.3 | 0.0667 |
| business | 04 - 90-179 DPD | MXN | 5 | 9633447.99 | 137.6 | 0.0556 |
| business | 05 - 180+ DPD | MXN | 12 | 29955704.48 | 247.1 | 0.1333 |
| business | 00 - Current | PEN | 52 | 11403325.78 | 0.0 | 0.5098 |
| business | 01 - Early (1-29) | PEN | 8 | 2851672.63 | 11.6 | 0.0784 |
| business | 02 - 30-59 DPD | PEN | 9 | 6519921.52 | 41.8 | 0.0882 |
| business | 03 - 60-89 DPD | PEN | 12 | 4677106.24 | 76.5 | 0.1176 |
| business | 04 - 90-179 DPD | PEN | 3 | 1542462.33 | 126.0 | 0.0294 |
| business | 05 - 180+ DPD | PEN | 18 | 7936080.43 | 265.2 | 0.1765 |
| business | 00 - Current | USD | 393 | 25556280.85 | 0.0 | 0.5171 |
| business | 01 - Early (1-29) | USD | 63 | 7641177.32 | 15.4 | 0.0829 |
| business | 02 - 30-59 DPD | USD | 64 | 9122331.06 | 44.6 | 0.0842 |
| business | 03 - 60-89 DPD | USD | 59 | 6731446.10 | 73.9 | 0.0776 |
| business | 04 - 90-179 DPD | USD | 54 | 6540443.81 | 130.0 | 0.0711 |
| business | 05 - 180+ DPD | USD | 127 | 13916379.04 | 268.4 | 0.1671 |
| business | 00 - Current | UYU | 40 | 105480456.42 | 0.0 | 0.4598 |
| business | 01 - Early (1-29) | UYU | 8 | 45860860.16 | 17.4 | 0.0920 |
| business | 02 - 30-59 DPD | UYU | 7 | 49185952.20 | 47.6 | 0.0805 |
| business | 03 - 60-89 DPD | UYU | 7 | 40468447.23 | 77.0 | 0.0805 |
| business | 04 - 90-179 DPD | UYU | 8 | 34796366.79 | 143.8 | 0.0920 |
| business | 05 - 180+ DPD | UYU | 17 | 100539069.61 | 279.0 | 0.1954 |
| education | 00 - Current | ARS | 54 | 3928630824.13 | 0.0 | 0.5294 |
| education | 01 - Early (1-29) | ARS | 5 | 467995952.39 | 14.6 | 0.0490 |
| education | 02 - 30-59 DPD | ARS | 6 | 856849931.16 | 45.8 | 0.0588 |
| education | 03 - 60-89 DPD | ARS | 10 | 1438458879.04 | 79.5 | 0.0980 |
| education | 04 - 90-179 DPD | ARS | 6 | 1420841185.01 | 128.7 | 0.0588 |
| education | 05 - 180+ DPD | ARS | 21 | 3004181106.62 | 291.2 | 0.2059 |
| education | 00 - Current | BRL | 53 | 19398336.53 | 0.0 | 0.4775 |
| education | 01 - Early (1-29) | BRL | 8 | 4311447.95 | 14.3 | 0.0721 |
| education | 02 - 30-59 DPD | BRL | 10 | 7509133.97 | 42.4 | 0.0901 |
| education | 03 - 60-89 DPD | BRL | 10 | 2797114.25 | 74.0 | 0.0901 |
| education | 04 - 90-179 DPD | BRL | 9 | 6410499.36 | 121.2 | 0.0811 |
| education | 05 - 180+ DPD | BRL | 21 | 9272565.64 | 281.7 | 0.1892 |
| education | 00 - Current | CLP | 51 | 3121521328.56 | 0.0 | 0.4554 |
| education | 01 - Early (1-29) | CLP | 11 | 1245439584.10 | 14.7 | 0.0982 |
| education | 02 - 30-59 DPD | CLP | 12 | 1501827741.68 | 41.9 | 0.1071 |
| education | 03 - 60-89 DPD | CLP | 9 | 1613557103.12 | 75.7 | 0.0804 |
| education | 04 - 90-179 DPD | CLP | 10 | 1387234595.40 | 127.4 | 0.0893 |
| education | 05 - 180+ DPD | CLP | 19 | 2324129092.83 | 257.4 | 0.1696 |
| education | 00 - Current | COP | 61 | 15904499042.52 | 0.0 | 0.5214 |
| education | 01 - Early (1-29) | COP | 3 | 2381940599.45 | 10.7 | 0.0256 |
| education | 02 - 30-59 DPD | COP | 12 | 4712183296.86 | 41.5 | 0.1026 |
| education | 03 - 60-89 DPD | COP | 10 | 3769160908.65 | 77.6 | 0.0855 |
| education | 04 - 90-179 DPD | COP | 13 | 6003827246.29 | 134.7 | 0.1111 |
| education | 05 - 180+ DPD | COP | 18 | 9823990821.93 | 275.8 | 0.1538 |
| education | 00 - Current | MXN | 50 | 50663057.97 | 0.0 | 0.5495 |
| education | 01 - Early (1-29) | MXN | 8 | 9157271.24 | 16.3 | 0.0879 |
| education | 02 - 30-59 DPD | MXN | 5 | 10061121.40 | 42.6 | 0.0549 |
| education | 03 - 60-89 DPD | MXN | 4 | 5123770.36 | 72.8 | 0.0440 |
| education | 04 - 90-179 DPD | MXN | 6 | 5977086.10 | 134.0 | 0.0659 |
| education | 05 - 180+ DPD | MXN | 18 | 33736493.00 | 285.2 | 0.1978 |
| education | 00 - Current | PEN | 53 | 10807080.62 | 0.0 | 0.5146 |
| education | 01 - Early (1-29) | PEN | 7 | 4238183.54 | 20.0 | 0.0680 |
| education | 02 - 30-59 DPD | PEN | 6 | 2562674.48 | 39.7 | 0.0583 |
| education | 03 - 60-89 DPD | PEN | 9 | 3723703.19 | 74.2 | 0.0874 |
| education | 04 - 90-179 DPD | PEN | 9 | 3653986.44 | 147.3 | 0.0874 |
| education | 05 - 180+ DPD | PEN | 19 | 9079254.36 | 282.2 | 0.1845 |
| education | 00 - Current | USD | 377 | 22395959.09 | 0.0 | 0.5150 |
| education | 01 - Early (1-29) | USD | 59 | 8037799.21 | 13.8 | 0.0806 |
| education | 02 - 30-59 DPD | USD | 63 | 8381377.13 | 46.1 | 0.0861 |
| education | 03 - 60-89 DPD | USD | 54 | 6129814.10 | 72.9 | 0.0738 |
| education | 04 - 90-179 DPD | USD | 62 | 8808032.50 | 136.3 | 0.0847 |
| education | 05 - 180+ DPD | USD | 117 | 15576999.20 | 279.2 | 0.1598 |
| education | 00 - Current | UYU | 37 | 72853382.35 | 0.0 | 0.3814 |
| education | 01 - Early (1-29) | UYU | 11 | 52869953.30 | 12.8 | 0.1134 |
| education | 02 - 30-59 DPD | UYU | 17 | 50325805.91 | 43.8 | 0.1753 |
| education | 03 - 60-89 DPD | UYU | 9 | 49070956.70 | 74.7 | 0.0928 |
| education | 04 - 90-179 DPD | UYU | 8 | 16690217.70 | 138.8 | 0.0825 |
| education | 05 - 180+ DPD | UYU | 15 | 89340951.93 | 274.4 | 0.1546 |
| mortgage | 00 - Current | ARS | 50 | 1982100151.24 | 0.0 | 0.5000 |
| mortgage | 01 - Early (1-29) | ARS | 7 | 1105344285.61 | 9.9 | 0.0700 |
| mortgage | 02 - 30-59 DPD | ARS | 9 | 791555872.76 | 44.9 | 0.0900 |
| mortgage | 03 - 60-89 DPD | ARS | 6 | 659404729.89 | 76.2 | 0.0600 |
| mortgage | 04 - 90-179 DPD | ARS | 12 | 1421408254.68 | 136.7 | 0.1200 |
| mortgage | 05 - 180+ DPD | ARS | 16 | 2405850139.71 | 279.0 | 0.1600 |
| mortgage | 00 - Current | BRL | 59 | 21461301.72 | 0.0 | 0.5514 |
| mortgage | 01 - Early (1-29) | BRL | 9 | 7543940.55 | 16.0 | 0.0841 |
| mortgage | 02 - 30-59 DPD | BRL | 14 | 7133371.59 | 46.2 | 0.1308 |
| mortgage | 03 - 60-89 DPD | BRL | 7 | 3127981.75 | 76.9 | 0.0654 |
| mortgage | 04 - 90-179 DPD | BRL | 8 | 5839450.77 | 142.4 | 0.0748 |
| mortgage | 05 - 180+ DPD | BRL | 10 | 9812615.21 | 283.7 | 0.0935 |
| mortgage | 00 - Current | CLP | 42 | 3025687571.63 | 0.0 | 0.4516 |
| mortgage | 01 - Early (1-29) | CLP | 8 | 1340045350.26 | 16.3 | 0.0860 |
| mortgage | 02 - 30-59 DPD | CLP | 7 | 637589412.54 | 47.1 | 0.0753 |
| mortgage | 03 - 60-89 DPD | CLP | 5 | 479235049.73 | 69.0 | 0.0538 |
| mortgage | 04 - 90-179 DPD | CLP | 13 | 1185186413.12 | 129.2 | 0.1398 |
| mortgage | 05 - 180+ DPD | CLP | 18 | 2081321846.62 | 273.4 | 0.1935 |
| mortgage | 00 - Current | COP | 47 | 10062817194.56 | 0.0 | 0.4896 |
| mortgage | 01 - Early (1-29) | COP | 7 | 4984334847.41 | 12.7 | 0.0729 |
| mortgage | 02 - 30-59 DPD | COP | 8 | 2018779717.24 | 42.9 | 0.0833 |
| mortgage | 03 - 60-89 DPD | COP | 9 | 3930414481.15 | 72.2 | 0.0938 |
| mortgage | 04 - 90-179 DPD | COP | 9 | 2349146809.33 | 138.9 | 0.0938 |
| mortgage | 05 - 180+ DPD | COP | 16 | 6545293071.20 | 248.3 | 0.1667 |
| mortgage | 00 - Current | MXN | 64 | 79354354.41 | 0.0 | 0.5517 |
| mortgage | 01 - Early (1-29) | MXN | 10 | 26306502.21 | 15.9 | 0.0862 |
| mortgage | 02 - 30-59 DPD | MXN | 10 | 14563304.68 | 44.0 | 0.0862 |
| mortgage | 03 - 60-89 DPD | MXN | 10 | 20731129.49 | 72.4 | 0.0862 |
| mortgage | 04 - 90-179 DPD | MXN | 5 | 13021248.29 | 133.0 | 0.0431 |
| mortgage | 05 - 180+ DPD | MXN | 17 | 47540582.53 | 283.7 | 0.1466 |
| mortgage | 00 - Current | PEN | 52 | 8593619.88 | 0.0 | 0.4815 |
| mortgage | 01 - Early (1-29) | PEN | 9 | 4410017.06 | 13.9 | 0.0833 |
| mortgage | 02 - 30-59 DPD | PEN | 10 | 4032631.76 | 44.5 | 0.0926 |
| mortgage | 03 - 60-89 DPD | PEN | 10 | 2115343.35 | 75.5 | 0.0926 |
| mortgage | 04 - 90-179 DPD | PEN | 10 | 6551117.67 | 151.1 | 0.0926 |
| mortgage | 05 - 180+ DPD | PEN | 17 | 3673395.88 | 259.6 | 0.1574 |
| mortgage | 00 - Current | USD | 383 | 23603725.11 | 0.0 | 0.5148 |
| mortgage | 01 - Early (1-29) | USD | 69 | 7364297.02 | 14.0 | 0.0927 |
| mortgage | 02 - 30-59 DPD | USD | 61 | 6405563.67 | 42.9 | 0.0820 |
| mortgage | 03 - 60-89 DPD | USD | 55 | 7161020.29 | 74.4 | 0.0739 |
| mortgage | 04 - 90-179 DPD | USD | 60 | 8019020.65 | 131.9 | 0.0806 |
| mortgage | 05 - 180+ DPD | USD | 116 | 14224355.35 | 273.1 | 0.1559 |
| mortgage | 00 - Current | UYU | 56 | 149806669.67 | 0.0 | 0.4912 |
| mortgage | 01 - Early (1-29) | UYU | 4 | 38607457.98 | 16.5 | 0.0351 |
| mortgage | 02 - 30-59 DPD | UYU | 11 | 78915311.42 | 40.5 | 0.0965 |
| mortgage | 03 - 60-89 DPD | UYU | 10 | 52254009.30 | 71.3 | 0.0877 |
| mortgage | 04 - 90-179 DPD | UYU | 10 | 45750747.05 | 129.7 | 0.0877 |
| mortgage | 05 - 180+ DPD | UYU | 23 | 130491387.71 | 265.9 | 0.2018 |
| personal | 00 - Current | ARS | 52 | 3124782721.42 | 0.0 | 0.4727 |
| personal | 01 - Early (1-29) | ARS | 10 | 1087262673.09 | 12.4 | 0.0909 |
| personal | 02 - 30-59 DPD | ARS | 10 | 1205181343.96 | 40.2 | 0.0909 |
| personal | 03 - 60-89 DPD | ARS | 14 | 1692249124.79 | 76.5 | 0.1273 |
| personal | 04 - 90-179 DPD | ARS | 11 | 1169181617.59 | 139.9 | 0.1000 |
| personal | 05 - 180+ DPD | ARS | 13 | 1851740403.03 | 281.5 | 0.1182 |
| personal | 00 - Current | BRL | 60 | 19181693.79 | 0.0 | 0.4724 |
| personal | 01 - Early (1-29) | BRL | 15 | 9356204.27 | 14.4 | 0.1181 |
| personal | 02 - 30-59 DPD | BRL | 10 | 6512142.53 | 45.5 | 0.0787 |
| personal | 03 - 60-89 DPD | BRL | 15 | 6761524.58 | 74.9 | 0.1181 |
| personal | 04 - 90-179 DPD | BRL | 10 | 5291570.32 | 142.2 | 0.0787 |
| personal | 05 - 180+ DPD | BRL | 17 | 10865238.23 | 258.8 | 0.1339 |
| personal | 00 - Current | CLP | 36 | 1862204455.15 | 0.0 | 0.4557 |
| personal | 01 - Early (1-29) | CLP | 6 | 684284169.75 | 15.2 | 0.0759 |
| personal | 02 - 30-59 DPD | CLP | 8 | 1255433364.53 | 49.6 | 0.1013 |
| personal | 03 - 60-89 DPD | CLP | 2 | 65012071.13 | 65.0 | 0.0253 |
| personal | 04 - 90-179 DPD | CLP | 11 | 964218966.80 | 129.4 | 0.1392 |
| personal | 05 - 180+ DPD | CLP | 16 | 2264371426.94 | 283.6 | 0.2025 |
| personal | 00 - Current | COP | 44 | 6563954543.14 | 0.0 | 0.5000 |
| personal | 01 - Early (1-29) | COP | 3 | 1754364044.15 | 17.3 | 0.0341 |
| personal | 02 - 30-59 DPD | COP | 6 | 1978279901.10 | 43.3 | 0.0682 |
| personal | 03 - 60-89 DPD | COP | 7 | 2768765742.58 | 73.4 | 0.0795 |
| personal | 04 - 90-179 DPD | COP | 7 | 4595303466.72 | 131.0 | 0.0795 |
| personal | 05 - 180+ DPD | COP | 21 | 14710147449.05 | 297.0 | 0.2386 |
| personal | 00 - Current | MXN | 53 | 49210159.51 | 0.0 | 0.4690 |
| personal | 01 - Early (1-29) | MXN | 9 | 11558834.20 | 13.2 | 0.0796 |
| personal | 02 - 30-59 DPD | MXN | 15 | 25744723.79 | 48.5 | 0.1327 |
| personal | 03 - 60-89 DPD | MXN | 12 | 32057148.12 | 73.0 | 0.1062 |
| personal | 04 - 90-179 DPD | MXN | 9 | 15444274.61 | 131.4 | 0.0796 |
| personal | 05 - 180+ DPD | MXN | 15 | 32590464.00 | 291.7 | 0.1327 |
| personal | 00 - Current | PEN | 67 | 17571264.19 | 0.0 | 0.6091 |
| personal | 01 - Early (1-29) | PEN | 3 | 1881334.77 | 19.0 | 0.0273 |
| personal | 02 - 30-59 DPD | PEN | 7 | 4123897.48 | 44.1 | 0.0636 |
| personal | 03 - 60-89 DPD | PEN | 4 | 3226570.51 | 74.8 | 0.0364 |
| personal | 04 - 90-179 DPD | PEN | 7 | 3226268.45 | 150.7 | 0.0636 |
| personal | 05 - 180+ DPD | PEN | 22 | 10872662.74 | 263.7 | 0.2000 |
| personal | 00 - Current | USD | 406 | 24228264.99 | 0.0 | 0.5133 |
| personal | 01 - Early (1-29) | USD | 49 | 6181123.29 | 14.4 | 0.0619 |
| personal | 02 - 30-59 DPD | USD | 57 | 7225330.81 | 47.2 | 0.0721 |
| personal | 03 - 60-89 DPD | USD | 67 | 7142493.89 | 75.2 | 0.0847 |
| personal | 04 - 90-179 DPD | USD | 73 | 10177443.11 | 130.7 | 0.0923 |
| personal | 05 - 180+ DPD | USD | 139 | 17269057.95 | 274.3 | 0.1757 |
| personal | 00 - Current | UYU | 55 | 179010026.66 | 0.0 | 0.5340 |
| personal | 01 - Early (1-29) | UYU | 4 | 20915985.29 | 16.5 | 0.0388 |
| personal | 02 - 30-59 DPD | UYU | 13 | 56896655.51 | 43.1 | 0.1262 |
| personal | 03 - 60-89 DPD | UYU | 10 | 55734993.23 | 74.9 | 0.0971 |
| personal | 04 - 90-179 DPD | UYU | 8 | 29312704.25 | 134.4 | 0.0777 |
| personal | 05 - 180+ DPD | UYU | 13 | 76213220.47 | 265.1 | 0.1262 |

240 rows.

---

## Q9 — Risk-score segmentation (low / medium / high / critical)

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_risk_buckets` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

**Business definition:**
Internal `risk_score` (0–100 scale, from the source dataset) bucketed into
`low / medium / high / critical` per the macro `get_risk_bucket`. The mart
also reports observed delinquency rate per bucket as a sanity check on the
score's predictive power.

### Query

```sql
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
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| risk_bucket | customer_count | share_of_customers | observed_delinquency_rate |
|---|---|---|---|
| low      | 1495 | 0.2990 | **0.6533** |
| medium   | 1539 | 0.3078 | 0.6324 |
| high     | 1218 | 0.2436 | 0.6277 |
| critical |  748 | 0.1496 | **0.6043** |

4 rows, one per bucket.

### Notes — **headline finding of the project**

The relationship between the source `risk_score` and observed delinquency is
**non-monotonic and inverted**: customers classified as `low` risk show a
65.3 % delinquency rate, while those classified as `critical` show 60.4 %.

In a functioning risk-scoring model, this curve should rise from `low` to
`critical`. Here it falls. Possible interpretations (any combination is
plausible given a synthetic source):

1. The source `risk_score` does not encode credit risk in a way that
   correlates with the loan-status field the dataset also exposes.
2. The two fields were generated independently, with no joint distribution.
3. The labels (low/medium/high/critical) are mis-mapped at the source.

**This is the most actionable insight in the project for a real banking
stakeholder.** A model that mis-orders risk is worse than a model that
doesn't exist (it actively misallocates capital). See **Key findings** at
the bottom of this document for a Power-BI-ready framing.

---

# Customer Demographics

## Q10 — Customer count by country and city

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_customer_360` &nbsp;·&nbsp; **Dashboard page:** Customer & Engagement

### Query

```sql
SELECT
    country,
    city,
    COUNT(*) AS customer_count
FROM gold.mart_customer_360
GROUP BY country, city
ORDER BY country, customer_count DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| country | city | customer_count |
|---|---|---|
| CL | Valparaiso | 138 |
| AR | Rosario | 133 |
| MX | Tijuana | 128 |
| PE | Arequipa | 128 |
| UY | Salto | 127 |
| CO | Cali | 126 |
| BR | Fortaleza | 126 |
| BR | Rio de Janeiro | 126 |
| PE | Cusco | 124 |
| AR | Cordoba | 123 |
| UY | Montevideo | 122 |
| PE | Lima | 122 |
| AR | Buenos Aires | 121 |
| PE | Piura | 121 |
| MX | Monterrey | 120 |
| UY | Maldonado | 120 |
| MX | Guadalajara | 119 |
| BR | Salvador | 117 |
| BR | Brasilia | 117 |
| CL | Temuco | 116 |
| CO | Medellin | 115 |
| CL | La Serena | 115 |
| CO | Cartagena | 114 |
| AR | Mendoza | 112 |
| CO | Barranquilla | 112 |
| BR | Sao Paulo | 111 |
| UY | Rivera | 108 |
| UY | Paysandu | 107 |
| AR | Tucuman | 105 |
| CO | Bogota | 103 |

30 rows.

---

## Q11 — Age distribution by customer segment

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_customer_360` &nbsp;·&nbsp; **Dashboard page:** Customer & Engagement

**Business definition:**
Age buckets: `under_18 / 18-25 / 26-35 / 36-50 / 51-65 / 65+ / unknown`.
See `decisions.md` for bucket boundary rationale.

### Query

```sql
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
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| customer_segment | age_bucket | customer_count |
|---|---|---|
| premium | 18-25 | 178 |
| premium | 26-35 | 197 |
| premium | 36-50 | 298 |
| premium | 51-65 | 344 |
| premium | 65+ | 213 |
| private_banking | 18-25 | 187 |
| private_banking | 26-35 | 213 |
| private_banking | 36-50 | 307 |
| private_banking | 51-65 | 308 |
| private_banking | 65+ | 209 |
| retail | 18-25 | 184 |
| retail | 26-35 | 224 |
| retail | 36-50 | 317 |
| retail | 51-65 | 326 |
| retail | 65+ | 229 |
| sme | 18-25 | 166 |
| sme | 26-35 | 201 |
| sme | 36-50 | 336 |
| sme | 51-65 | 367 |
| sme | 65+ | 196 |

20 rows.

---

## Q12 — Customer acquisition trend over time (monthly)

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_acquisition_trend` &nbsp;·&nbsp; **Dashboard page:** Customer & Engagement

### Query

```sql
SELECT
    month_label,
    new_customers,
    cumulative_customers,
    ROUND(mom_growth_pct::numeric, 2) AS mom_growth_pct
FROM gold.mart_acquisition_trend
ORDER BY month;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| month_label | new_customers | cumulative_customers | mom_growth_pct |
|---|---|---|---|
| 2020-05 | 60 | 60 | <NULL> |
| 2020-06 | 63 | 123 | 5.00 |
| 2020-07 | 77 | 200 | 22.22 |
| 2020-08 | 67 | 267 | -12.99 |
| 2020-09 | 71 | 338 | 5.97 |
| 2020-10 | 68 | 406 | -4.23 |
| 2020-11 | 61 | 467 | -10.29 |
| 2020-12 | 72 | 539 | 18.03 |
| 2021-01 | 70 | 609 | -2.78 |
| 2021-02 | 61 | 670 | -12.86 |
| 2021-03 | 73 | 743 | 19.67 |
| 2021-04 | 69 | 812 | -5.48 |
| 2021-05 | 86 | 898 | 24.64 |
| 2021-06 | 75 | 973 | -12.79 |
| 2021-07 | 68 | 1041 | -9.33 |
| 2021-08 | 84 | 1125 | 23.53 |
| 2021-09 | 79 | 1204 | -5.95 |
| 2021-10 | 63 | 1267 | -20.25 |
| 2021-11 | 68 | 1335 | 7.94 |
| 2021-12 | 70 | 1405 | 2.94 |
| 2022-01 | 72 | 1477 | 2.86 |
| 2022-02 | 71 | 1548 | -1.39 |
| 2022-03 | 81 | 1629 | 14.08 |
| 2022-04 | 78 | 1707 | -3.70 |
| 2022-05 | 83 | 1790 | 6.41 |
| 2022-06 | 69 | 1859 | -16.87 |
| 2022-07 | 70 | 1929 | 1.45 |
| 2022-08 | 58 | 1987 | -17.14 |
| 2022-09 | 64 | 2051 | 10.34 |
| 2022-10 | 80 | 2131 | 25.00 |
| 2022-11 | 68 | 2199 | -15.00 |
| 2022-12 | 61 | 2260 | -10.29 |
| 2023-01 | 53 | 2313 | -13.11 |
| 2023-02 | 64 | 2377 | 20.75 |
| 2023-03 | 77 | 2454 | 20.31 |
| 2023-04 | 63 | 2517 | -18.18 |
| 2023-05 | 74 | 2591 | 17.46 |
| 2023-06 | 63 | 2654 | -14.86 |
| 2023-07 | 67 | 2721 | 6.35 |
| 2023-08 | 75 | 2796 | 11.94 |
| 2023-09 | 66 | 2862 | -12.00 |
| 2023-10 | 74 | 2936 | 12.12 |
| 2023-11 | 76 | 3012 | 2.70 |
| 2023-12 | 66 | 3078 | -13.16 |
| 2024-01 | 60 | 3138 | -9.09 |
| 2024-02 | 61 | 3199 | 1.67 |
| 2024-03 | 73 | 3272 | 19.67 |
| 2024-04 | 78 | 3350 | 6.85 |
| 2024-05 | 76 | 3426 | -2.56 |
| 2024-06 | 62 | 3488 | -18.42 |
| 2024-07 | 53 | 3541 | -14.52 |
| 2024-08 | 73 | 3614 | 37.74 |
| 2024-09 | 72 | 3686 | -1.37 |
| 2024-10 | 65 | 3751 | -9.72 |
| 2024-11 | 65 | 3816 | 0.00 |
| 2024-12 | 66 | 3882 | 1.54 |
| 2025-01 | 65 | 3947 | -1.52 |
| 2025-02 | 59 | 4006 | -9.23 |
| 2025-03 | 67 | 4073 | 13.56 |
| 2025-04 | 78 | 4151 | 16.42 |
| 2025-05 | 71 | 4222 | -8.97 |
| 2025-06 | 67 | 4289 | -5.63 |
| 2025-07 | 68 | 4357 | 1.49 |
| 2025-08 | 71 | 4428 | 4.41 |
| 2025-09 | 50 | 4478 | -29.58 |
| 2025-10 | 65 | 4543 | 30.00 |
| 2025-11 | 74 | 4617 | 13.85 |
| 2025-12 | 92 | 4709 | 24.32 |
| 2026-01 | 81 | 4790 | -11.96 |
| 2026-02 | 68 | 4858 | -16.05 |
| 2026-03 | 63 | 4921 | -7.35 |
| 2026-04 | 62 | 4983 | -1.59 |
| 2026-05 | 17 | 5000 | -72.58 |

73 rows.

---

## Q13 — Customer status breakdown ⚠️

**Status:** ⚠️ Partial (dataset divergence documented) &nbsp;·&nbsp; **Mart:** `gold.mart_customer_360` &nbsp;·&nbsp; **Dashboard page:** Executive Overview

**Divergence from spec:**
The project brief enumerates customer status as
`active / inactive / suspended / closed`. The actual dataset, however,
exposes customer-level `status` consistent with the spec, while
**account-level** `status` uses `active / frozen / closed`. This question
targets customer-level status — which conforms to the spec — so the answer
is faithful. The mart's `status` column has an `accepted_values` test
enforcing the spec values; if a future load violates that, the test fails
loudly rather than silently. See `decisions.md` Day 1 for the original
finding and the spec-vs-dataset reconciliation.

### Query

```sql
SELECT
    status,
    COUNT(*) AS customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM gold.mart_customer_360
GROUP BY status
ORDER BY customer_count DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| status | customer_count | pct |
|---|---|---|
| suspended | 1293 | 25.86 |
| active | 1288 | 25.76 |
| inactive | 1217 | 24.34 |
| closed | 1202 | 24.04 |

4 rows.

---

## Q14 — KYC status distribution

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_customer_360` &nbsp;·&nbsp; **Dashboard page:** Customer & Engagement

### Query

```sql
SELECT
    kyc_status,
    COUNT(*) AS customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM gold.mart_customer_360
GROUP BY kyc_status
ORDER BY customer_count DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| kyc_status | customer_count | pct |
|---|---|---|
| pending | 1290 | 25.80 |
| verified | 1264 | 25.28 |
| expired | 1231 | 24.62 |
| rejected | 1215 | 24.30 |

4 rows.

---

# Transaction Patterns

## Q15 — Most common transaction categories by volume and value

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_tx_by_category` &nbsp;·&nbsp; **Dashboard page:** Revenue & Transactions

### Query

```sql
-- By volume (count)
SELECT
    category,
    SUM(completed_tx_count) AS completed_tx_count
FROM gold.mart_tx_by_category
GROUP BY category
ORDER BY completed_tx_count DESC
LIMIT 15;

-- By value (within a single currency to avoid mixing units; example: USD)
SELECT
    category,
    SUM(completed_tx_count) AS completed_tx_count,
    SUM(tx_count)           AS total_tx_count
FROM gold.mart_tx_by_category
WHERE currency = 'USD'
GROUP BY category
ORDER BY completed_tx_count DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

**By volume (count) — top 15 across all currencies:**

| category | completed_tx_count | total_tx_count |
|---|---|---|
| healthcare | 1398 | 5507 |
| other | 1397 | 5442 |
| utilities | 1391 | 5473 |
| shopping | 1391 | 5453 |
| dining | 1385 | 5519 |
| salary | 1369 | 5365 |
| subscription | 1354 | 5348 |
| transport | 1350 | 5319 |
| travel | 1330 | 5405 |
| insurance | 1323 | 5338 |
| rent | 1321 | 5342 |
| groceries | 1320 | 5355 |
| education | 1318 | 5373 |
| investment | 1307 | 5331 |
| entertainment | 1251 | 5308 |

15 rows shown.

**By value (USD only, for comparable units):**

| category | completed_tx_count | tx_count |
|---|---|---|
| shopping | 683 | 2643 |
| groceries | 661 | 2481 |
| healthcare | 660 | 2584 |
| dining | 655 | 2550 |
| other | 654 | 2552 |
| utilities | 631 | 2511 |
| subscription | 625 | 2473 |
| transport | 620 | 2468 |
| insurance | 616 | 2463 |
| education | 614 | 2510 |
| travel | 612 | 2479 |
| rent | 605 | 2458 |
| investment | 603 | 2480 |
| salary | 580 | 2464 |
| entertainment | 574 | 2523 |

15 rows shown.

---

## Q16 — Transaction volume by day of week

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_tx_by_dow` &nbsp;·&nbsp; **Dashboard page:** Revenue & Transactions

### Query

```sql
SELECT
    day_of_week,
    day_of_week_name,
    SUM(tx_count) AS tx_count
FROM gold.mart_tx_by_dow
GROUP BY day_of_week, day_of_week_name
ORDER BY day_of_week;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| day_of_week | day_of_week_name | tx_count |
|---|---|---|
| 0 | Sunday | 11997 |
| 1 | Monday | 12174 |
| 2 | Tuesday | 12406 |
| 3 | Wednesday | 12827 |
| 4 | Thursday | 11692 |
| 5 | Friday | 12038 |
| 6 | Saturday | 11933 |

7 rows.

---

## Q17 — Average transaction size by channel

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_tx_by_channel` &nbsp;·&nbsp; **Dashboard page:** Revenue & Transactions

### Query

```sql
SELECT
    channel,
    currency,
    ROUND(avg_ticket::numeric, 2) AS avg_ticket_native_ccy,
    completed_tx_count
FROM gold.mart_tx_by_channel
WHERE avg_ticket IS NOT NULL
ORDER BY channel, currency;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| channel | currency | avg_ticket_native_ccy | completed_tx_count |
|---|---|---|---|
| atm | ARS | 25324774.14 | 318 |
| atm | BRL | 127699.52 | 326 |
| atm | CLP | 22376257.98 | 279 |
| atm | COP | 102097674.58 | 320 |
| atm | EUR | 21704.68 | 47 |
| atm | MXN | 483081.41 | 301 |
| atm | PEN | 92079.85 | 344 |
| atm | USD | 24159.46 | 1942 |
| atm | UYU | 967910.16 | 320 |
| branch | ARS | 24551734.61 | 322 |
| branch | BRL | 129551.69 | 275 |
| branch | CLP | 23465913.82 | 313 |
| branch | COP | 107039367.20 | 315 |
| branch | EUR | 19592.47 | 55 |
| branch | MXN | 453924.96 | 362 |
| branch | PEN | 92718.98 | 354 |
| branch | USD | 24380.20 | 2005 |
| branch | UYU | 976283.70 | 312 |
| mobile | ARS | 26170326.99 | 326 |
| mobile | BRL | 122205.48 | 328 |
| mobile | CLP | 23459390.84 | 299 |
| mobile | COP | 93908843.78 | 285 |
| mobile | EUR | 21740.16 | 51 |
| mobile | MXN | 446178.28 | 364 |
| mobile | PEN | 92323.86 | 319 |
| mobile | USD | 24666.59 | 2024 |
| mobile | UYU | 958560.74 | 327 |
| pos | ARS | 24448237.94 | 354 |
| pos | BRL | 115229.42 | 308 |
| pos | CLP | 24245498.22 | 297 |
| pos | COP | 97880345.87 | 291 |
| pos | EUR | 20556.32 | 50 |
| pos | MXN | 443132.44 | 328 |
| pos | PEN | 92197.61 | 338 |
| pos | USD | 25333.18 | 1968 |
| pos | UYU | 949502.28 | 317 |
| web | ARS | 25187068.63 | 340 |
| web | BRL | 128111.93 | 311 |
| web | CLP | 23356648.88 | 298 |
| web | COP | 96192404.53 | 333 |
| web | EUR | 22441.92 | 34 |
| web | MXN | 469427.46 | 322 |
| web | PEN | 93066.74 | 300 |
| web | USD | 24878.81 | 1924 |
| web | UYU | 1044069.76 | 309 |

45 rows.

---

## Q18 — Failed transaction rate by channel

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_tx_by_channel` &nbsp;·&nbsp; **Dashboard page:** Revenue & Transactions

**Business definition:**
`failed_rate = failed_tx_count / tx_count` (all-status denominator). The
mart materializes this at the (channel, currency) grain; the query below
rolls up by channel only.

### Query

```sql
SELECT
    channel,
    SUM(tx_count)         AS total_tx,
    SUM(failed_tx_count)  AS failed_tx,
    ROUND(SUM(failed_tx_count)::numeric / NULLIF(SUM(tx_count), 0), 4) AS failed_rate
FROM gold.mart_tx_by_channel
GROUP BY channel
ORDER BY failed_rate DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| channel | total_tx | failed_tx | failed_rate |
|---|---|---|---|
| atm | 17134 | 4316 | 0.2519 |
| branch | 17077 | 4295 | 0.2515 |
| pos | 17023 | 4253 | 0.2498 |
| mobile | 17092 | 4245 | 0.2484 |
| web | 16741 | 4138 | 0.2472 |

5 rows.

---

## Q19 — International transfer patterns

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_international_transfers` &nbsp;·&nbsp; **Dashboard page:** Revenue & Transactions

**Business definition** (`decisions.md` Day 9 §8):
`is_international = (transaction_type = 'transfer'
                    AND tx_currency ≠ account_currency
                    AND tx_currency ≠ customer_country_currency)`.
The strictest of three candidate definitions — minimizes false positives
(e.g., a Uruguayan with a USD account sending USD to another USD account is
correctly classified as domestic).

### Query

```sql
-- Corridors (origin_country × tx_currency) sorted by international share
SELECT
    origin_country,
    tx_currency,
    tx_count,
    international_tx_count,
    ROUND(international_share::numeric, 4) AS international_share,
    total_international_value
FROM gold.mart_international_transfers
WHERE international_tx_count > 0
ORDER BY international_share DESC, international_tx_count DESC
LIMIT 20;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| origin_country | tx_currency | tx_count | international_tx_count | international_share | total_intl_value |
|---|---|---|---|---|---|
| MX | UYU | 33 | 33 | 1.0000 | 8650115.09 |
| CO | CLP | 33 | 33 | 1.0000 | 92864454.65 |
| MX | PEN | 32 | 32 | 1.0000 | 792080.86 |
| MX | ARS | 30 | 30 | 1.0000 | 367598638.17 |
| PE | EUR | 30 | 30 | 1.0000 | 226865.62 |
| UY | PEN | 29 | 29 | 1.0000 | 422832.18 |
| CL | MXN | 29 | 29 | 1.0000 | 3720297.83 |
| PE | MXN | 28 | 28 | 1.0000 | 817903.68 |
| PE | CLP | 27 | 27 | 1.0000 | 307099115.06 |
| BR | UYU | 27 | 27 | 1.0000 | 3700897.61 |
| MX | CLP | 26 | 26 | 1.0000 | 36603737.87 |
| BR | ARS | 26 | 26 | 1.0000 | 95398879.65 |
| AR | UYU | 26 | 26 | 1.0000 | 9485235.55 |
| UY | CLP | 26 | 26 | 1.0000 | 109114429.39 |
| AR | COP | 25 | 25 | 1.0000 | 645359729.55 |
| AR | PEN | 25 | 25 | 1.0000 | 523628.41 |
| AR | BRL | 25 | 25 | 1.0000 | 1139434.66 |
| CO | BRL | 24 | 24 | 1.0000 | 365685.91 |
| CO | UYU | 24 | 24 | 1.0000 | 6397250.26 |
| CL | ARS | 24 | 24 | 1.0000 | 119237758.38 |

20 rows.

---

# Digital Engagement

## Q20 — Mobile app adoption rate by segment

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_digital_adoption_by_segment` &nbsp;·&nbsp; **Dashboard page:** Customer & Engagement

### Query

```sql
SELECT
    customer_segment,
    customer_count,
    ROUND(mobile_adoption_rate::numeric, 4)      AS mobile_adoption,
    ROUND(web_adoption_rate::numeric, 4)         AS web_adoption,
    ROUND(any_digital_adoption_rate::numeric, 4) AS any_digital_adoption
FROM gold.mart_digital_adoption_by_segment
ORDER BY mobile_adoption_rate DESC NULLS LAST;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| customer_segment | customer_count | mobile_adoption | web_adoption | any_digital_adoption |
|---|---|---|---|---|
| sme | 1266 | 0.4992 | 0.4810 | 0.7338 |
| private_banking | 1224 | 0.4984 | 0.4763 | 0.7312 |
| premium | 1230 | 0.4919 | 0.5098 | 0.7528 |
| retail | 1280 | 0.4758 | 0.5039 | 0.7422 |

4 rows.

---

## Q21 — Digital vs branch preference by age group

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_channel_preference_by_age` &nbsp;·&nbsp; **Dashboard page:** Customer & Engagement

**Business definition:**
The mart exposes all 5 channels separately at the (age_bucket, preferred_channel)
grain. Power BI groups `mobile + web` as "digital" and `atm + branch + phone`
as "non-digital" at visualization time. The query below presents the rollup
so the answer is self-contained.

### Query

```sql
SELECT
    age_bucket,
    SUM(CASE WHEN preferred_channel IN ('mobile', 'web')
             THEN customer_count ELSE 0 END) AS digital_customers,
    SUM(CASE WHEN preferred_channel IN ('atm', 'branch', 'phone')
             THEN customer_count ELSE 0 END) AS non_digital_customers,
    SUM(customer_count) AS total_customers,
    ROUND(
        SUM(CASE WHEN preferred_channel IN ('mobile', 'web')
                 THEN customer_count ELSE 0 END)::numeric
        / NULLIF(SUM(customer_count), 0), 4
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
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| age_bucket | digital_customers | non_digital_customers | total_customers | digital_share |
|---|---|---|---|---|
| 18-25 | 275 | 440 | 715 | 0.3846 |
| 26-35 | 335 | 500 | 835 | 0.4012 |
| 36-50 | 509 | 749 | 1258 | 0.4046 |
| 51-65 | 559 | 786 | 1345 | 0.4156 |
| 65+ | 351 | 496 | 847 | 0.4144 |

5 rows.

---

# Product Analysis

## Q22 — Most popular account types

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_account_mix` &nbsp;·&nbsp; **Dashboard page:** Executive Overview

### Query

```sql
SELECT
    account_type,
    SUM(accounts_count) AS total_accounts,
    ROUND(100.0 * SUM(accounts_count) / SUM(SUM(accounts_count)) OVER (), 2)
        AS pct_of_total
FROM gold.mart_account_mix
GROUP BY account_type
ORDER BY total_accounts DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| account_type | total_accounts | pct_of_total |
|---|---|---|
| credit_card | 1531 | 26.05 |
| savings | 1471 | 25.03 |
| investment | 1454 | 24.74 |
| checking | 1421 | 24.18 |

4 rows.

---

## Q23 — Loan portfolio composition

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_loan_composition` &nbsp;·&nbsp; **Dashboard page:** Risk & Credit

### Query

```sql
SELECT
    loan_type,
    status,
    currency,
    loan_count,
    total_outstanding_balance,
    ROUND(outstanding_share_within_type_currency::numeric, 4) AS share_within_type_currency
FROM gold.mart_loan_composition
ORDER BY loan_type, currency, status;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| loan_type | status | currency | loan_count | outstanding_balance | share_within_type |
|---|---|---|---|---|---|
| auto | current | ARS | 32 | 3304810022.57 | 0.3236 |
| auto | default | ARS | 28 | 3571521434.65 | 0.3497 |
| auto | delinquent | ARS | 29 | 3336350502.61 | 0.3267 |
| auto | paid_off | ARS | 33 | 0.00 | 0.0000 |
| auto | current | BRL | 28 | 15885572.50 | 0.3682 |
| auto | default | BRL | 28 | 15188595.54 | 0.3520 |
| auto | delinquent | BRL | 21 | 12071312.25 | 0.2798 |
| auto | paid_off | BRL | 32 | 0.00 | 0.0000 |
| auto | current | CLP | 22 | 3136188019.86 | 0.3426 |
| auto | default | CLP | 29 | 3392769292.47 | 0.3706 |
| auto | delinquent | CLP | 26 | 2626444129.56 | 0.2869 |
| auto | paid_off | CLP | 32 | 0.00 | 0.0000 |
| auto | current | COP | 33 | 16753655134.33 | 0.3626 |
| auto | default | COP | 28 | 12299457760.79 | 0.2662 |
| auto | delinquent | COP | 42 | 17150953156.54 | 0.3712 |
| auto | paid_off | COP | 30 | 0.00 | 0.0000 |
| auto | current | MXN | 31 | 76246121.93 | 0.3503 |
| auto | default | MXN | 27 | 60729785.64 | 0.2790 |
| auto | delinquent | MXN | 26 | 80686805.26 | 0.3707 |
| auto | paid_off | MXN | 31 | 0.00 | 0.0000 |
| auto | current | PEN | 32 | 15742739.36 | 0.4636 |
| auto | default | PEN | 18 | 9088536.96 | 0.2676 |
| auto | delinquent | PEN | 30 | 9126083.63 | 0.2688 |
| auto | paid_off | PEN | 25 | 0.00 | 0.0000 |
| auto | current | USD | 190 | 21647807.66 | 0.2921 |
| auto | default | USD | 201 | 24514950.76 | 0.3307 |
| auto | delinquent | USD | 209 | 27958010.52 | 0.3772 |
| auto | paid_off | USD | 187 | 0.00 | 0.0000 |
| auto | current | UYU | 27 | 132245919.35 | 0.3316 |
| auto | default | UYU | 27 | 141583349.20 | 0.3550 |
| auto | delinquent | UYU | 28 | 124948002.44 | 0.3133 |
| auto | paid_off | UYU | 27 | 0.00 | 0.0000 |
| business | current | ARS | 21 | 2319381935.72 | 0.2688 |
| business | default | ARS | 34 | 3537445247.45 | 0.4099 |
| business | delinquent | ARS | 26 | 2772666713.90 | 0.3213 |
| business | paid_off | ARS | 22 | 0.00 | 0.0000 |
| business | current | BRL | 28 | 19743366.61 | 0.3840 |
| business | default | BRL | 28 | 19308659.63 | 0.3755 |
| business | delinquent | BRL | 25 | 12368699.50 | 0.2405 |
| business | paid_off | BRL | 18 | 0.00 | 0.0000 |
| business | current | CLP | 32 | 3413092245.66 | 0.3603 |
| business | default | CLP | 23 | 2727809712.13 | 0.2880 |
| business | delinquent | CLP | 26 | 3331974714.46 | 0.3517 |
| business | paid_off | CLP | 23 | 0.00 | 0.0000 |
| business | current | COP | 31 | 16072904728.47 | 0.3731 |
| business | default | COP | 31 | 13084926955.71 | 0.3037 |
| business | delinquent | COP | 24 | 13924378140.87 | 0.3232 |
| business | paid_off | COP | 25 | 0.00 | 0.0000 |
| business | current | MXN | 30 | 58167928.77 | 0.3830 |
| business | default | MXN | 18 | 39589152.47 | 0.2607 |
| business | delinquent | MXN | 23 | 54097685.40 | 0.3562 |
| business | paid_off | MXN | 26 | 0.00 | 0.0000 |
| business | current | PEN | 29 | 11403325.78 | 0.3265 |
| business | default | PEN | 23 | 9080099.00 | 0.2599 |
| business | delinquent | PEN | 31 | 14447144.15 | 0.4136 |
| business | paid_off | PEN | 23 | 0.00 | 0.0000 |
| business | current | USD | 201 | 25556280.85 | 0.3677 |
| business | default | USD | 187 | 20110802.04 | 0.2893 |
| business | delinquent | USD | 193 | 23840975.29 | 0.3430 |
| business | paid_off | USD | 203 | 0.00 | 0.0000 |
| business | current | UYU | 22 | 105480456.42 | 0.2803 |
| business | default | UYU | 26 | 135335436.40 | 0.3596 |
| business | delinquent | UYU | 22 | 135515259.59 | 0.3601 |
| business | paid_off | UYU | 19 | 0.00 | 0.0000 |
| education | current | ARS | 29 | 3928630824.13 | 0.3534 |
| education | default | ARS | 28 | 4425022291.63 | 0.3980 |
| education | delinquent | ARS | 22 | 2763304762.59 | 0.2486 |
| education | paid_off | ARS | 30 | 0.00 | 0.0000 |
| education | current | BRL | 25 | 19398336.53 | 0.3903 |
| education | default | BRL | 30 | 15059654.44 | 0.3030 |
| education | delinquent | BRL | 29 | 15241106.73 | 0.3067 |
| education | paid_off | BRL | 30 | 0.00 | 0.0000 |
| education | current | CLP | 29 | 3121521328.56 | 0.2789 |
| education | default | CLP | 30 | 3711363688.23 | 0.3316 |
| education | delinquent | CLP | 32 | 4360824428.90 | 0.3896 |
| education | paid_off | CLP | 24 | 0.00 | 0.0000 |
| education | current | COP | 34 | 15904499042.52 | 0.3734 |
| education | default | COP | 30 | 15193652426.53 | 0.3567 |
| education | delinquent | COP | 26 | 11497450446.65 | 0.2699 |
| education | paid_off | COP | 30 | 0.00 | 0.0000 |
| education | current | MXN | 23 | 50663057.97 | 0.4416 |
| education | default | MXN | 24 | 39713579.10 | 0.3462 |
| education | delinquent | MXN | 17 | 24342163.00 | 0.2122 |
| education | paid_off | MXN | 27 | 0.00 | 0.0000 |
| education | current | PEN | 20 | 10807080.62 | 0.3172 |
| education | default | PEN | 28 | 12733240.80 | 0.3738 |
| education | delinquent | PEN | 22 | 10524561.21 | 0.3090 |
| education | paid_off | PEN | 34 | 0.00 | 0.0000 |
| education | current | USD | 190 | 22395959.09 | 0.3230 |
| education | default | USD | 184 | 24219601.40 | 0.3493 |
| education | delinquent | USD | 182 | 22714420.74 | 0.3276 |
| education | paid_off | USD | 196 | 0.00 | 0.0000 |
| education | current | UYU | 21 | 72853382.35 | 0.2200 |
| education | default | UYU | 23 | 102417550.27 | 0.3093 |
| education | delinquent | UYU | 38 | 155880335.27 | 0.4707 |
| education | paid_off | UYU | 19 | 0.00 | 0.0000 |
| mortgage | current | ARS | 20 | 1982100151.24 | 0.2369 |
| mortgage | default | ARS | 28 | 3827258394.39 | 0.4575 |
| mortgage | delinquent | ARS | 22 | 2556304888.26 | 0.3056 |
| mortgage | paid_off | ARS | 31 | 0.00 | 0.0000 |
| mortgage | current | BRL | 35 | 21461301.72 | 0.3908 |
| mortgage | default | BRL | 19 | 15652065.98 | 0.2850 |
| mortgage | delinquent | BRL | 31 | 17805293.89 | 0.3242 |
| mortgage | paid_off | BRL | 24 | 0.00 | 0.0000 |
| mortgage | current | CLP | 30 | 3025687571.63 | 0.3458 |
| mortgage | default | CLP | 31 | 3242962275.71 | 0.3707 |
| mortgage | delinquent | CLP | 22 | 2480415796.56 | 0.2835 |
| mortgage | paid_off | CLP | 15 | 0.00 | 0.0000 |
| mortgage | current | COP | 24 | 10062817194.56 | 0.3367 |
| mortgage | default | COP | 25 | 8894439880.53 | 0.2976 |
| mortgage | delinquent | COP | 24 | 10933529045.80 | 0.3658 |
| mortgage | paid_off | COP | 23 | 0.00 | 0.0000 |
| mortgage | current | MXN | 35 | 79354354.41 | 0.3938 |
| mortgage | default | MXN | 23 | 60561830.82 | 0.3005 |
| mortgage | delinquent | MXN | 31 | 61600936.38 | 0.3057 |
| mortgage | paid_off | MXN | 29 | 0.00 | 0.0000 |
| mortgage | current | PEN | 22 | 8593619.88 | 0.2925 |
| mortgage | default | PEN | 27 | 10224513.55 | 0.3481 |
| mortgage | delinquent | PEN | 31 | 10557992.17 | 0.3594 |
| mortgage | paid_off | PEN | 30 | 0.00 | 0.0000 |
| mortgage | current | USD | 196 | 23603725.11 | 0.3535 |
| mortgage | default | USD | 177 | 21937823.96 | 0.3285 |
| mortgage | delinquent | USD | 193 | 21236433.02 | 0.3180 |
| mortgage | paid_off | USD | 196 | 0.00 | 0.0000 |
| mortgage | current | UYU | 27 | 149806669.67 | 0.3021 |
| mortgage | default | UYU | 34 | 175755697.19 | 0.3545 |
| mortgage | delinquent | UYU | 27 | 170263216.27 | 0.3434 |
| mortgage | paid_off | UYU | 29 | 0.00 | 0.0000 |
| personal | current | ARS | 29 | 3124782721.42 | 0.3085 |
| personal | default | ARS | 27 | 3020922020.62 | 0.2982 |
| personal | delinquent | ARS | 35 | 3984693141.84 | 0.3933 |
| personal | paid_off | ARS | 25 | 0.00 | 0.0000 |
| personal | current | BRL | 31 | 19181693.79 | 0.3309 |
| personal | default | BRL | 27 | 14684419.89 | 0.2533 |
| personal | delinquent | BRL | 42 | 24102260.04 | 0.4158 |
| personal | paid_off | BRL | 32 | 0.00 | 0.0000 |
| personal | current | CLP | 25 | 1862204455.15 | 0.2624 |
| personal | default | CLP | 27 | 3228590393.74 | 0.4550 |
| personal | delinquent | CLP | 19 | 2004729605.41 | 0.2825 |
| personal | paid_off | CLP | 13 | 0.00 | 0.0000 |
| personal | current | COP | 18 | 6563954543.14 | 0.2028 |
| personal | default | COP | 27 | 18852891857.63 | 0.5824 |
| personal | delinquent | COP | 18 | 6953968745.97 | 0.2148 |
| personal | paid_off | COP | 28 | 0.00 | 0.0000 |
| personal | current | MXN | 22 | 49210159.51 | 0.2954 |
| personal | default | MXN | 25 | 48034738.61 | 0.2883 |
| personal | delinquent | MXN | 36 | 69360706.11 | 0.4163 |
| personal | paid_off | MXN | 32 | 0.00 | 0.0000 |
| personal | current | PEN | 39 | 17571264.19 | 0.4296 |
| personal | default | PEN | 29 | 14098931.19 | 0.3447 |
| personal | delinquent | PEN | 14 | 9231802.76 | 0.2257 |
| personal | paid_off | PEN | 29 | 0.00 | 0.0000 |
| personal | current | USD | 211 | 24228264.99 | 0.3355 |
| personal | default | USD | 210 | 26907243.29 | 0.3726 |
| personal | delinquent | USD | 183 | 21088205.76 | 0.2920 |
| personal | paid_off | USD | 204 | 0.00 | 0.0000 |
| personal | current | UYU | 35 | 179010026.66 | 0.4282 |
| personal | default | UYU | 21 | 105525924.72 | 0.2524 |
| personal | delinquent | UYU | 28 | 133547634.03 | 0.3194 |
| personal | paid_off | UYU | 23 | 0.00 | 0.0000 |

160 rows.

---

## Q24 — Average number of products per customer by segment

**Status:** ✅ Covered &nbsp;·&nbsp; **Mart:** `gold.mart_customer_360` &nbsp;·&nbsp; **Dashboard page:** Executive Overview

**Business definition:**
`total_products = accounts_count + loans_count`, pre-aggregated in
`silver.agg_customer_activity`.

### Query

```sql
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(accounts_count)::numeric, 2) AS avg_accounts,
    ROUND(AVG(loans_count)::numeric, 2)    AS avg_loans,
    ROUND(AVG(total_products)::numeric, 2) AS avg_total_products
FROM gold.mart_customer_360
GROUP BY customer_segment
ORDER BY avg_total_products DESC;
```

### Sample result (snapshot from `scripts/validation_output.txt`, May 17 2026)

| customer_segment | customer_count | avg_accounts | avg_loans | avg_total_products |
|---|---|---|---|---|
| retail | 1280 | 3.55 | 1.51 | 5.06 |
| private_banking | 1224 | 3.50 | 1.54 | 5.04 |
| sme | 1266 | 3.49 | 1.54 | 5.03 |
| premium | 1230 | 3.49 | 1.54 | 5.03 |

4 rows.

---

# Key Findings

These observations emerged from running the Gold layer against the source
dataset. Each is anchored to a question above and to validated numbers in
`scripts/validation_output.txt`. They are framed for a business audience —
the supporting metric definitions live in `decisions.md`.

## 1. The source risk score is anti-predictive of delinquency (Q9)

| risk_bucket | observed_delinquency_rate |
|---|---|
| low      | **65.3 %** |
| medium   | 63.2 % |
| high     | 62.8 % |
| critical | **60.4 %** |

A working risk score should produce a monotonically *increasing* curve from
`low` to `critical`. The source dataset produces the opposite. For a banking
stakeholder, this is a flag of higher priority than any aggregate metric:
**a scoring model that mis-orders risk actively misallocates capital and
collections effort.** Remediation in a real engagement would start with
re-deriving `risk_score` from observable behavioural features (DPD history,
utilization, balance volatility) rather than trusting the source field.

This finding is consistent with the broader observation (#3 below) that the
synthetic data generator does not couple financial dimensions in realistic
ways. It is reported here because the pipeline does its job correctly —
exposing the anomaly — and the insight is real regardless of the data's
synthetic origin.

## 2. Multi-currency portfolio with structural USD dollarization (Q2)

Every country in the dataset (`AR`, `BR`, `CL`, `CO`, `MX`, `PE`, `UY`)
holds active accounts in **both its local currency and USD**, with USD
accounts representing roughly half of the population per country. Average
USD balance per account is remarkably uniform across countries
(~$237 k – $257 k), suggesting an offshore/dollarized customer profile
that exists in parallel with the local-currency book.

This shape drove an explicit design decision in the Gold layer: balance
metrics are reported in **source currency** (grain includes `currency`)
rather than rolled up via FX, so a CLP-heavy and a USD-heavy view of the
same country never get conflated. See `decisions.md` for the rationale.

## 3. Synthetic-data flatness across financial dimensions (Q1, Q5, Q18, Q20)

Several metrics that would show large segment/channel spreads in real
banking come out near-uniform here:

- **Revenue by segment (Q1):** $2.5 k – $2.9 k across all four segments.
  Real banking shows private_banking 5-50× retail.
- **Delinquency by segment (Q5):** 62-65 % across all segments; real
  retail-banking portfolios run 2-8 %.
- **Failed transaction rate (Q18):** ~25 % uniform across all channels;
  a real production system at 25 % would be in a P0 incident.
- **Digital adoption (Q20):** ~50 % mobile / ~50 % web, flat across
  segments; real cohorts show large generational and segment effects.

These are properties of the data generator, not pipeline defects. They are
documented here so the evaluator can distinguish "the metric is implemented
correctly" from "the metric tells a realistic story." The pipeline's job is
the former; the latter requires a real production dataset.

## 4. The pipeline surfaces data-quality issues honestly

Three places where the Gold layer exposes (rather than hides) source-data
problems:

- **Q2 — `accounts_with_balance` column:** 3.1 % of active accounts have
  NULL balance. The mart exposes the count of accounts that contributed
  to balance metrics as a separate column, making the missing-data rate
  queryable from Power BI rather than absorbed into `total_balance` via
  SQL's null-skipping default.
- **Q6 — `unknown` credit_score bucket:** ~10 % of records have corrupt
  credit_score values. They get bucketed as `unknown` instead of being
  silently dropped, preserving the denominator for any rate computation.
- **Q13 — `accepted_values` test on customer status:** the customer-level
  `status` field conforms to the spec; the account-level `status` field
  diverges (has `frozen` instead of `inactive`/`suspended`). The dbt test
  enforces the customer-level spec values, so a future load that introduces
  spec drift fails loudly rather than silently mis-reporting.

---

# Final Coverage

| Status | Count | Questions |
|---|---|---|
| ✅ Covered     | 23 | Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q14, Q15, Q16, Q17, Q18, Q19, Q20, Q21, Q22, Q23, Q24 |
| ⚠️ Partial    | 1  | Q13 (customer-level `status` matches spec; account-level `status` diverges — documented) |
| ❌ Out of scope | 0  | — |

**Total: 24 / 24** &nbsp;·&nbsp; **Target: ≥ 21 / 24** &nbsp;·&nbsp; **Achieved: ✅**

All counted questions are backed by:
- a Gold mart with explicit grain,
- dbt tests (`accepted_values`, `not_null`, `unique`, `expression_is_true`,
  `relationships`) materialized in `models/gold/_gold__mart.yml`, and
- a documented business definition in `decisions.md` where the metric is
  non-trivial (revenue, delinquency, internationality, buckets).