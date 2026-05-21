## Power BI Dashboard (v0.4.0)

The dashboard exposes the gold layer via four pages, each answering a 
distinct family of business questions. Connection is direct to the 
`gold` schema in PostgreSQL using PBI's Import mode.

### Cross-page design decisions

**Color palette (semantic):**
- Risk severity: green `#97C459` (healthy/current), amber `#FAC775` 
  (early warning), orange `#EF9F27` (delinquent), red `#A32D2D` (default), 
  dark red `#501313` (write-off), gray `#888780` (closed/inactive).
- Customer segments: violet `#7F77DD` (premium), teal `#1D9E75` 
  (private_banking), blue `#185FA5` (retail), orange `#EF9F27` (sme).
- The palette is deliberately not uniform across categories — risk uses 
  a sequential traffic-light system, segments use distinct hues. This 
  separation makes the semantic meaning of each color immediate.

**Slicer architecture:**
- Page 1 (Executive): Customer Segment, Country. No currency slicer — 
  revenue values come pre-converted to USD from `mart_revenue_by_segment_usd`.
- Page 2 (Revenue & Transactions): Currency (single-select, required), 
  Channel. Currency is forced single-select because cross-currency 
  aggregations are mathematically invalid (1 USD + 1 ARS ≠ 2). Defaulted 
  to USD.
- Page 3 (Risk & Credit): Currency (single-select), Country, Loan Type, 
  Customer Segment. Same currency rationale as page 2.
- Page 4 (Customer & Engagement): Year/Month, Country, Customer Segment. 
  No currency slicer — visuals are demographic, not monetary.

**Data model:**
The 15 gold marts are not connected directly. Four conformed dimensions 
(`dim_currency`, `dim_country`, `dim_segment`, `dim_loan_type`) act as 
filter propagators, connected one-to-many to the marts that share each 
attribute. This lets a single slicer filter visuals across multiple marts 
simultaneously while keeping the model star-shaped. Slicers point to 
the dimensions, never to mart columns directly.

A deliberate choice: `mart_customer_360` is NOT connected to 
`dim_currency`. A customer is not denominated in a currency — their 
accounts and loans are. Currency filters affect monetary visuals 
(loan exposure, transaction volume); customer-level visuals (credit 
score, utilization average) remain unfiltered by currency by design.

---

### Page 1 — Executive Overview

**Purpose:** Single-screen health check for executives. Customer base 
size, total AUM, geographic distribution, segment mix, customer status.

**Visuals:**
- 3 KPI cards: Total Customers (5,000), Total AUM USD (710M), 
  Median Revenue per Customer USD (3,936).
- Donut: Customer status distribution (4 states).
- Map: Customers by Country, with mini-pie per country showing segment mix.
- Horizontal bar: Customers by Segment (totals).

**Business questions answered:** Q1 (revenue), Q2 (AUM by segment), 
Q3 (geographic distribution).

**Key finding:** Customer status is nearly uniform across 4 states 
(active 25.86%, closed 25.76%, inactive 24.34%, suspended 24.04%). 
In a real bank, active customers should dominate (>80%). This is the 
first signal of the synthetic dataset's uniform distribution pattern, 
which recurs in pages 3 and 4.

**Design decisions:**
- Map with mini-pies is non-standard but effective for LATAM coverage 
  — it shows both geography AND segment mix in one visual.
- Segment bar uses uniform navy color (not the segment palette) because 
  the visual answers "how many customers per segment" not "what is each 
  segment." Coloring by segment would be redundant with the Y-axis labels.

---

### Page 2 — Revenue & Transactions

**Purpose:** Revenue performance, transaction patterns by channel and 
category, top merchants.

**Visuals:**
- 3 KPI cards: Median Ticket USD (25K), Failure Rate (25%), 
  Total Revenue USD (12M).
- Line: Monthly Revenue by Segment USD, 2021-2026.
- Bar: Transactions by Channel.
- Treemap: Transactions by Category (14 categories).
- Table: Top merchants by total_value, with tx_count and avg_ticket.

**Business questions answered:** Q4 (transactions by channel), 
Q5 (revenue trends), Q6 (top merchants), Q7 (transaction failure).

**Key finding 1 — Revenue growth:** Revenue grows from ~0 to ~5M USD/month 
across 2021-2026 in all segments simultaneously. This reflects the 
dataset's transaction-date distribution (synthetic generator front-loads 
recent dates), not a real business growth pattern. The shape is suspicious 
of an "exponential" curve that doesn't match the flat customer acquisition 
trend in page 4.

**Key finding 2 — Failure rate at 25%:** A real bank operates at 
<3% failure rate. The 25% here reflects uniform status distribution 
in the generator (`success/failed/pending/cancelled` each ~25%), not 
a realistic operational metric.

**Key finding 3 — Channel uniformity:** All 5 channels show ~7.8K-8K 
transactions when filtered to USD. Same uniform-distribution pattern.

**Design decisions:**
- Currency slicer is forced single-select. Without this, mixing ARS, 
  BRL, COP, MXN, USD, etc. into one revenue line produces meaningless 
  totals (1 ARS treated as 1 USD inflates the chart by ~1000x in 
  some periods).
- Treemap for categories was chosen over bar chart for visual density — 
  14 categories in a horizontal bar would dominate the page. The trade-off: 
  treemap is harder to read for small differences, but the uniform 
  distribution of this dataset makes comparison less critical.

---

### Page 3 — Risk & Credit

**Purpose:** Portfolio creditworthiness, delinquency analysis, loan 
aging, composition by type and status.

**Visuals:**
- 4 KPI cards: Avg Credit Score (573), Delinquency Rate (63%), 
  Default Rate (25%), Avg Utilization (50%).
- Column: Customer distribution by credit score band (6 buckets).
- Scatter: Utilization vs delinquency rate, with linear trend line 
  (4 bucket-level points).
- Horizontal bar: Loan portfolio aging by days past due (6 DPD buckets).
- 100% stacked column: Loan portfolio composition by type (5 types) × 
  status (4 statuses).

**Business questions answered:** Q8 (DPD aging), Q9 (risk distribution), 
Q10 (credit score distribution), Q11 (utilization analysis), 
Q12 (portfolio composition), Q23 (delinquency by loan type).

**Key finding 1 — Insolvent-looking portfolio:** 41% of total outstanding 
balance is in 180+ DPD (write-off territory). A real bank with this 
distribution would be insolvent. The generator samples DPD from a 
biased-uniform distribution rather than the heavy-tailed distribution 
seen in real lending portfolios.

**Key finding 2 — Utilization does not predict delinquency:** Across 
4 utilization buckets (healthy 18%, moderate 50%, high 75%, maxed 95%), 
delinquency rate stays in the 62-64% range. The FICO heuristic (higher 
utilization → higher delinquency) does not hold in this dataset. The 
linear trend line on the scatter is essentially flat. This is the same 
pattern documented for risk_score vs realized delinquency in the EDA.

**Key finding 3 — Uniform composition across loan types:** Every loan 
type (personal, auto, business, mortgage, education) shows ~25/25/25/25 
across status (current, delinquent, default, paid_off). In reality, 
mortgages have ~95% current and ~2% default; personal loans ~85% current. 
The generator does not model risk differential by product type.

**Key finding 4 — Average credit score (573) is in "Poor" band.** 
For a real portfolio the average sits around 700. The synthetic 
generator does not model the typical right-skewed score distribution 
where most customers cluster above 650.

**Design decisions:**
- Scatter is built at bucket level (4 points), not customer level 
  (~5,000 points). With 5,000 customers, the scatter would be noisy 
  and the trend line indistinguishable. The bucket-level aggregation 
  comes pre-built in `mart_utilization_vs_delinquency`, making the 
  insight readable in one glance.
- DPD bar uses `total_outstanding_balance` (USD) instead of `loan_count`. 
  Exposure-in-money is the bankable metric — a small number of loans 
  in 180+ DPD with large balances is more dangerous than many loans 
  with tiny balances.
- 100% stacked (not regular stacked) for portfolio composition because 
  the question is "what is the mix per loan type", not "how many loans 
  per type". The 100% normalization makes status proportions directly 
  comparable across types.

---

### Page 4 — Customer & Engagement

**Purpose:** Customer acquisition over time, demographics, digital 
adoption, KYC compliance state.

**Visuals:**
- 6 KPI cards: Total Customers (5,000), New Customers Last Complete 
  Month (63), MoM Acquisition (-1.6%), Digital Adoption Rate (74%), 
  KYC Verified Rate (25%), Avg Monthly Logins (30).
- Line: Monthly new customers, 2021-2026 (May 2026 excluded as partial).
- Clustered column: Age distribution by customer segment 
  (5 age bands × 4 segments).
- Donut: Preferred channel distribution (5 channels).
- Horizontal bar: KYC verification status (4 states).

**Business questions answered:** Q13 (customer acquisition trend), 
Q14 (demographics), Q15 (digital adoption), Q16 (KYC compliance), 
Q24 (channel preference).

**Key finding 1 — Acquisition is stable, not collapsing.** Earlier 
analysis on the last 6 months suggested a downtrend, but the full 
2021-2026 series shows monthly acquisition fluctuating between 50-90 
new customers throughout, with no sustained trend. The apparent recent 
decline was a partial-month artifact (May 2026).

**Key finding 2 — Age distribution is broadly realistic.** Customer 
counts peak in 36-50 (the widest band by design at 14 years) and 
decline toward extremes (18-25, 65+). This is one of the few patterns 
in the dataset that resembles real banking demographics. Segment mix 
within each age band is approximately uniform (no segment is age-targeted).

**Key finding 3 — Channel uniformity, again.** Each of the 5 channels 
(mobile 21.16%, branch 20.10%, atm 20.04%, web 19.42%, phone 19.28%) 
holds ~20% share. In real banking, mobile_app typically dominates with 
50%+ among customers under 45 and branch dominates among 65+. The 
generator distributes channels uniformly without modeling age-channel 
correlation.

**Key finding 4 — KYC distribution is unrealistic.** Each KYC status 
(pending 25.80%, verified 25.28%, expired 24.62%, rejected 24.30%) 
holds ~25% share. In an active customer base, verified should be >80%. 
Only 25% verified means 75% of customers would be operationally blocked.

**Design decisions:**
- MoM Acquisition measure explicitly excludes partial months. Without 
  this, the MoM showed -96% because May 2026 (21 days, 17 customers) 
  was compared against April (full month, 62 customers). The fix uses 
  `last completed month` logic, comparing April vs March.
- The acquisition line chart filters out May 2026 from the X-axis as 
  well, otherwise an artificial collapse at the right edge would mislead 
  readers.
- `age_bucket` rendering required a custom DAX column to sort properly. 
  The mart's bucket ordering by alphabetical string puts "65+" before 
  "18-25". A numeric `age_bucket_order` derived from `age` (not from 
  `age_bucket`, to avoid circular dependency) drives sort order.

---

### Consolidated dataset assessment

Across the four pages, a consistent finding emerges: **the synthetic 
generator produces uniform distributions where real banking data 
shows skewed/heavy-tailed distributions.** This manifests in:

| Dimension | Observed | Real-world expectation |
|---|---|---|
| Customer status | 25/25/25/25 | active >80% |
| Loan status × type | 25/25/25/25 in every type | mortgages ~95% current |
| DPD distribution | ~41% in 180+ | <5% in 180+ for solvent banks |
| Credit score | avg 573, even spread | avg ~700, right-skewed |
| Utilization vs delinquency | flat (no correlation) | strong positive correlation |
| Channel preference | 5 channels at ~20% each | mobile dominant (50%+) |
| KYC status | 4 states at ~25% each | verified >80% |
| Transaction failure rate | 25% | <3% |

The dashboard reports these findings honestly rather than masking them. 
This is a deliberate design choice: a portfolio of business questions 
answered with "the data shows X, which is not realistic because Y" 
demonstrates analytical judgment that surface-level metric reporting 
does not.