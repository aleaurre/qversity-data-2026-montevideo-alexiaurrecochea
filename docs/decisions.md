# Modeling and business decisions

> Living document. Last update: after exhaustive EDA of the raw dataset
> (Day 1, afternoon block).
>
> This document captures all data quality, modeling and metrics decisions
> that drive the implementation of the Silver and Gold layers of the
> pipeline.

---

## 1. Data quality - key EDA findings

### 1.1 Volume
- **5,100 customer records** (brief said ~5,000 ✓).
- 17,870 accounts | 89,470 transactions | 7,821 loans.
- Uniform top-level schema: 24 identical keys across all records.

### 1.2 Massive categorical inconsistencies
Categories arrive dirty in 3 combinable forms:
- **Inconsistent casing** (`Retail` / `RETAIL` / `retail`).
- **English/Spanish mix** (`pyme` ↔ `sme`, `cerrado` ↔ `closed`, `reembolso` ↔ `refund`).
- **Whitespace** in front of values (` credit_card`, `  investment`).

### 1.3 Nulls encoded as strings
Fields contain `"null"`, `"N/A"`, `"NA"`, `"None"`, `""` that are not
detected by automatic parsers. A normalization function converts them to
real NULL before any analysis.

### 1.4 Dates in 4 coexisting formats
No real nulls — only parsing problems. **The separator fully disambiguates
day/month order**, which makes the dataset 100% recoverable:

| Format       | %    | Discriminator           |
|--------------|------|-------------------------|
| YYYY-MM-DD   | 82%  | 4 digits at start       |
| MM-DD-YYYY   | 6%   | `-` separator (US)      |
| DD/MM/YYYY   | 6%   | `/` separator (Latin)   |
| YYYYMMDD     | 6%   | 8 digits no separator   |

Applies to: `registration_date`, `date_of_birth`, `accounts.opened_date`,
`transactions.date`, `loans.start_date`, `loans.end_date`,
`digital_engagement.last_login_date`.

### 1.5 Numerics as strings
~3% of records in numeric fields arrive as strings with 4 sub-formats:
- Clean: `"424926.82"` → direct cast.
- Latin: `"191286,14"` → replace `,` with `.`.
- Symbol: `"$1810568162.59"` → strip `$`.
- Currency suffix: `"404393.03 USD"` → strip suffix via regex.

Affects: `accounts.balance`, `transactions.amount`, `loans.principal`,
`loans.outstanding_balance`, `loans.monthly_payment`.

### 1.6 Extreme numeric outliers
Fields with values seemingly generated at wrong scale (max ~$2 billion):
`balance`, `principal`, `outstanding_balance`, `monthly_payment`,
`total_limit`, `total_used`.
- **Do not discard** — they may be legitimate in `private_banking`.
- ~~Flag with `is_outlier_<col>` by 99th percentile.~~
  **Status Day 10:** the Day 1 decision was NOT implemented (verified via
  `information_schema.columns` — 0 `is_outlier*` columns in the warehouse).
  Outliers are present in Gold unflagged; marts use standard SUM/AVG
  without filtering. If Power BI needs robustness against outliers,
  percentile filtering can be applied in the dashboard, or `is_outlier_*`
  columns can be added in a future iteration.
- In Gold, central-tendency KPIs use **median**, not mean, when
  outlier sensitivity matters (applied selectively in the marts where
  appropriate).

### 1.7 Corrupted credit_score
**524 records (10.3%) have credit_score outside the valid [300-850] range**,
with absurd values like `-99` and `999999`. Likely generator artifact.
- In Silver: out-of-range values → NULL + flag `credit_score_invalid`.
- In Gold: credit_score queries filter on valid values.
- **Documented in README as a limitation affecting business question #6.**

### 1.8 Booleans with mixed values
`bankruptcy_flag`, `push_notifications`, `paperless_statements` mix:
- real bools (`True` / `False`),
- English strings (`"true"` / `"false"`),
- `Y` / `N` letters,
- Spanish (`"si"`).

`mobile_app_registered` and `web_banking_registered` are pure bools.

---

## 2. Categorical canonicalization strategy

### 2.1 customer_segment → 4 values

| Canonical         | Variants to map                                            |
|-------------------|------------------------------------------------------------|
| `retail`          | retail, Retail, RETAIL, minorista                          |
| `premium`         | premium, Premium, PREMIUM                                  |
| `sme`             | sme, Sme, SME, pyme, PYME                                  |
| `private_banking` | private_banking, Private_Banking, PRIVATE_BANKING, banca_privada |

### 2.2 customer.status → 4 values

| Canonical   | Variants                                                     |
|-------------|--------------------------------------------------------------|
| `active`    | active, Active, ACTIVE, activo, Activo, ACTIVO               |
| `inactive`  | inactive, Inactive, INACTIVE, inactivo, Inactivo             |
| `suspended` | suspended, Suspended, SUSPENDED, suspendido                  |
| `closed`    | closed, Closed, CLOSED, cerrado, Cerrado                     |

### 2.3 accounts.status → 3 values
`active` | `frozen` | `closed`
(includes Spanish: `congelado` → frozen, `cerrado` → closed).

### 2.4 transactions.type → 6 values
`deposit` | `withdrawal` | `transfer` | `payment` | `refund` | `fee`
Spanish variants: `deposito`, `retiro`, `transferencia`, `pago`,
`reembolso`, `comision`.

### 2.5 transactions.status → 4 values
`pending` | `completed` | `failed` | `reversed` (only casings, no Spanish).

### 2.6 loans.type, loans.status, kyc_status, gender
Casings only → `lower()` + `trim()` resolves everything. For `gender`,
map to `F` / `M` / `Other` / NULL.

### 2.7 Whitespace in account_type and transactions.category
Apply `trim()` before any comparison.

### 2.8 Booleans

```sql
CASE
  WHEN LOWER(TRIM(col)) IN ('true','t','y','yes','si','1') THEN TRUE
  WHEN LOWER(TRIM(col)) IN ('false','f','n','no','0')      THEN FALSE
  ELSE NULL
END
```

---

## 3. Date strategy (PySpark)

Apply cascading parsing with `coalesce`, from most restrictive to most lax format:

```python
F.coalesce(
    F.to_date(col, "yyyy-MM-dd"),
    F.to_date(col, "yyyyMMdd"),
    F.to_date(col, "MM-dd-yyyy"),    # only matches with dash
    F.to_date(col, "dd/MM/yyyy"),    # only matches with slash
)
```

If all attempts fail → NULL + flag `<col>_parse_failed = TRUE` for
auditing.

**Do not trust automatic pandas/Spark parsing with mixed formats.**

---

## 4. Numeric strategy (PySpark)

```python
def parse_money(col):
    # 1. Strip currency suffixes " USD", " EUR", etc.
    cleaned = F.regexp_replace(
        col.cast("string"),
        r"\s+(USD|EUR|ARS|BRL|CLP|COP|MXN|PEN|UYU)$",
        ""
    )
    # 2. Strip currency symbols
    cleaned = F.regexp_replace(cleaned, r"[\$€£]", "")
    # 3. Handle Latin format with thousands ("1.234,56") vs decimal ("191286,14")
    cleaned = F.when(
        cleaned.rlike(r"^\d{1,3}(\.\d{3})+,\d+$"),
        F.regexp_replace(F.regexp_replace(cleaned, r"\.", ""), r",", ".")
    ).otherwise(
        F.regexp_replace(cleaned, r",", ".")
    )
    return cleaned.cast("double")
```

Unparseable strings → NULL + audit column
`<col>_parse_failed boolean`.
**dbt test:** ensure that `% of failures < 1%`.

---

## 5. Deduplication

### 5.1 Finding (EDA-validated)
Duplicates in nested arrays (accounts, transactions, loans) are **100%
derivative** of customer duplicates. There is no independent duplication
in the arrays. Each duplicated PK originates because its parent
`customer_id` appears more than once (98 customers × 2 + 1 customer × 3 =
100 total customer duplicates).

### 5.2 Strategy: one operation cascades the cleanup

1. **Canonicalize FIRST** all categorical fields in customers
   (`lower()`, Spanish→English, `trim()`). Necessary because duplicates
   differ exactly in casing/language/whitespace.

2. **Dedup on customers** with criterion:
   ```sql
   ROW_NUMBER() OVER (
     PARTITION BY customer_id
     ORDER BY
       (count of non-null fields) DESC,
       registration_date DESC NULLS LAST
   ) = 1
   ```
   The first criterion prioritizes the most complete record. The second
   breaks ties by recency. Handles N appearances without special logic.

3. **Cascade to arrays:** explode arrays via INNER JOIN against the
   deduplicated `stg_customers`. Duplicated IDs disappear automatically
   because their losing customer rows no longer exist.

### 5.3 PySpark implementation
- **Stage 1:** canonicalization + ROW_NUMBER on bronze → `silver.stg_customers`.
- **Stage 2:** explode arrays with JOIN against `stg_customers` →
  `silver.stg_accounts`, `silver.stg_transactions`, `silver.stg_loans`.
  PK-deduplicated by construction.

### 5.4 Verification in dbt
`unique` tests on `customer_id`, `account_id`, `transaction_id`, `loan_id`
must pass after cascading dedup.

---

## 6. Real null handling

| Field                                      | % nulls | Decision                                                        |
|--------------------------------------------|---------|-----------------------------------------------------------------|
| `relationship_manager`                     | 9.9%    | **Day 6: revisited.** The Day 1 decision (impute `'UNASSIGNED'`) was NOT implemented. `dim_customer.sql` passes the value through as it comes from the JSON. Any downstream consumer that needs to treat NULL as "unassigned" must apply `COALESCE(relationship_manager, 'UNASSIGNED')` in its query. |
| `address`                                  | 7.8%    | Preserve NULL. Does not block analysis.                          |
| `gender`                                   | 5.4%    | **Day 6: revisited.** The Day 1 decision (impute `'Unknown'`) was NOT implemented. `dim_customer.sql` passes NULL through: `(data ->> 'gender')::text as gender`. No imputation. |
| `accounts.credit_limit`                    | 74.6%   | **Legitimate:** only `credit_card` has it.                       |
| `transactions.description`                 | 10.2%   | Acceptable. Preserve NULL.                                       |
| `transactions.merchant`                    | 8.3%    | Acceptable. Preserve NULL.                                       |
| `transactions.category`                    | 4.9%    | Preserve NULL.                                                   |
| `loans.collateral_type`                    | 47.7%   | **Legitimate:** unsecured loans → map to `'unsecured'` in Gold.  |
| `digital_engagement.avg_monthly_logins`    | 7.4%    | **Day 6: revisited.** The Day 1 decision (validate hypothesis + impute 0) was NOT implemented. `dim_digital_engagement` applies `safe_cast_numeric(..., 'int')` and leaves NULL passthrough when the source is NULL or unparseable. The hypothesis "NULL coincides with `mobile_app_registered=FALSE AND web_banking_registered=FALSE`" was never formally validated. Any downstream consumer that needs to treat NULL as 0 must apply `COALESCE(avg_monthly_logins, 0)` in its query. |

**Observed pattern:** the Day 1 imputation decisions (UNASSIGNED, Unknown)
were not implemented — during dbt modeling, NULL pass-through was chosen
instead of imputation. This is defensible: NULL preserves the information
"data not available" without inventing values; downstream consumers
(Power BI, ad-hoc queries) can impute at point of use if needed.

---

## 7. Business metrics definitions

### Revenue
> Revenue = `fees` + proportional `interest_income`.
> - **Fees:** `SUM(amount) WHERE type='fee'` by customer/segment/month.
> - **Interest income:** `(loans.outstanding_balance * loans.interest_rate / 12)`
>   per active loan.

### Delinquency
> A loan is delinquent if `days_past_due > 30` OR
> `status IN ('delinquent', 'default')`.
> Delinquency rate = delinquent loans / total loans (excluding `paid_off`).

### Tenure
> Tenure (years) = `(today - registration_date) / 365.25`.

### Segmentation buckets

**Age:**
> 18–25 | 26–35 | 36–45 | 46–55 | 56–65 | 65+

**Risk score (0-100):**
> low: 0–25 | medium: 26–50 | high: 51–75 | critical: 76–100

**Credit utilization (0-100):**
> *(Day 1 definition — **superseded by Day 9 §4**: the final version has
> 6 buckets including `over_limit` and `unknown`. See section 4 "Business
> definitions for Gold marts" below.)*
> low: <30 | medium: 30–70 | high: 70–90 | critical: >90

**Credit score (300-850, validated):**
> poor: 300-579 | fair: 580-669 | good: 670-739 | very_good: 740-799 | excellent: 800-850

**Days past due:**
> current: 0 | 1–30 | 31–60 | 61–90 | 90+

---

## 8. Documented special cases

### 8.1 EUR in transactions
940 transactions (1%) in EUR despite being a LATAM dataset. **Preserve** —
they serve to answer business question #19 (international transfer
patterns).

### 8.2 `phone` in preferred_channel
`digital_engagement.preferred_channel` has values
`mobile / atm / branch / phone / web`, while `transactions.channel` has
`mobile / atm / branch / pos / web`. Different universes:
**do not create a unified `dim_channel`**.

### 8.3 collateral_type = "None" (string)
~48% of loans have no collateral. The string `"None"` is mapped to real
NULL in Silver and translated as `'unsecured'` in Gold for portfolio
queries.

### 8.4 Triplicated customer
1 customer (`CUST-0004728`) appears 3 times, while the rest of duplicates
are pairs. Arithmetic: 98 × 2 + 1 × 3 = 199 rows → 100 duplicates
detected by `.duplicated().sum()`. The ROW_NUMBER strategy by
completeness + recency handles N appearances without special logic.

---

## 9. Versions and infrastructure

### Airflow base image: 2.10.5
The brief asks for "Apache Airflow 2.7+". We chose 2.10.5 (latest stable
2.x at project start) for security — the 2.7.3 image accumulates critical
CVEs due to its age. Drastically reduces the vulnerability surface while
maintaining full compatibility with the brief.

### Runtime user
The Dockerfile uses `USER root` only during APT package installation and
the JDBC driver download (operations that require privileges). The
container runs at runtime as `airflow` (UID 50000), following the
recommended practice from the official Airflow documentation for extending
the image.

### PySpark in local mode
Spark runs in `local[*]` inside the Airflow container (no external
cluster). It is sufficient for the ~5k records of the dataset.
**Documented trade-off:** this configuration would not scale to millions
of rows; in that case it would require an external Spark cluster.


## PySpark setup + accounts flatten

### 1. Architectural decision: how PySpark is invoked from Airflow

**Chosen option**: `spark-submit` executed via `BashOperator`.
**Discarded alternative**: PySpark embedded inside a `PythonOperator`.

**Reasons**:
- The brief explicitly asks for "PySpark scripts live in a dedicated folder
  (e.g., spark/) and are triggered from Airflow", which suggests standalone
  invokable scripts, not embedded functions.
- Each Spark script is self-contained and locally testable
  (`spark-submit spark/flatten_accounts.py` from inside the container,
  without touching Airflow). This greatly accelerated debugging during
  the day.
- Clean separation between orchestration (Airflow) and execution (Spark).
- The overhead of bringing up a JVM per task (~10-15s) is irrelevant in
  a batch pipeline that runs 1x/day.

### 2. SparkSession configuration

Centralized in `spark/utils.py` with `get_spark_session()`:
- `master = local[*]` (parameterizable via env var `SPARK_MASTER`).
- `spark.jars`, `spark.driver.extraClassPath`, `spark.executor.extraClassPath`
  pointing to the JDBC JAR (`/opt/spark/jars/postgresql-42.7.3.jar`).
- `spark.sql.session.timeZone = UTC` to avoid implicit conversions based
  on the host's time zone. Conversion to local time is delegated to the
  BI layer.
- `spark.sql.shuffle.partitions = 4` (default 200), because we run in
  `local[*]` with a small dataset and 200 partitions generate thousands
  of minimal tasks with unnecessary overhead.

### 3. Credentials handling in Spark scripts

Postgres credentials are read from environment variables
(`POSTGRES_USER`, `POSTGRES_PASSWORD`, etc.), never hardcoded.

`get_jdbc_config()` uses `os.environ[...]` (not `.get()`) for user and
password. If the env var is missing, the script fails fast with an
explicit `KeyError` instead of connecting as `None` and returning a
cryptic Postgres error seconds later.

### 4. Reading bronze: jsonb arrives as string, parsed with `from_json`

The Postgres JDBC driver delivers `jsonb` columns as `text`, not as a
native struct. There is no way to avoid this round-trip. The logic:

1. Read `bronze.raw_fintech_data` via JDBC; `data` arrives as string.
2. Apply `from_json(col("data"), customer_partial_schema)` to parse to
   struct.
3. `explode()` over the `accounts` array.
4. Promote struct fields to top-level columns.

### 5. Partial schema per script

Each Spark script declares only the portion of the JSON it needs.
`flatten_accounts.py` declares `customer_id + accounts[]` and nothing
else. The other scripts (transactions, loans) will declare THEIR schemas.

**Why**: avoids implicit coupling. If the accounts script "knows" the
entire shape of the customer, when someone changes transactions this
script reacts inadvertently. The partial schema declares a minimal
contract: "I only need this".

### 6. JDBC read partitioning

`read_bronze` uses `partitionColumn=id`, `lowerBound=1`,
`upperBound=100000`, `numPartitions=4`.

For 10,200 records it is overkill (a single split would suffice), but it
is the correct practice and it shows when the dataset grows. Documented
so it gets reused in the transactions and loans scripts.

### 7. Deduplication strategy

**Finding**: bronze contains 10,200 records but only 5,000 unique
customers. Each customer appears duplicated in bronze due to multiple
`load_timestamp` values (natural consequence of re-running the DAG during
Day 2 development).

**We do not truncate bronze**: the brief asks to preserve raw faithfully,
and having load history is the correct thing to do in a professional
Bronze layer. Deduplication is delegated to Silver.

**Implementation**: window function in `spark/flatten_accounts.py`:

```python
Window.partitionBy("account_id").orderBy(col("load_timestamp").desc())
keep row_number() == 1
```

The most recent version of each account is kept. The same pattern will
be applied in `stg_transactions` and `stg_loans`.

**Result**: 35,740 accounts after explode → 17,529 after dedup
(eliminated exactly half, consistent with the double-load theory).

**Implication for dbt tests**: the PK `account_id` in
`silver.stg_accounts` must be unique. If in future runs this test fails,
it indicates the window function did not cover some edge case —
investigate before relaxing the rule.

### 8. Silver write mode: `overwrite` with `truncate=true`

`mode=overwrite` + `truncate=true` in the JDBC writer.

**Why overwrite**: the contract of a staging table is "this reflects the
latest view of bronze". If in the future we want change history, that
belongs in an SCD2 layer in silver/gold, not in staging.

**Why truncate=true**: reuses the existing table instead of dropping and
recreating it. This preserves permissions, constraints and indexes that
dbt or an admin might have added.

### 9. Data cleaning: separation of responsibilities Spark / dbt

During the flatten of `accounts[]` we detected serious categorical
inconsistencies (see findings below). The rule for what gets cleaned
where:

- **PySpark (silver staging)**: only universal syntactic cleaning.
  - `trim()` on all strings.
  - Empty string post-trim → `NULL`.
  - No business logic.
- **dbt (silver models)**: semantic normalization.
  - `lower()` to unify casing.
  - Explicit mapping of translations.
  - `accepted_values` tests to block the introduction of new inadvertent
    variants in future runs.

**Why**: semantic normalization involves decisions (are `cerrado` and
`closed` equivalent? yes, but it must be defended). Having it in
versioned SQL is auditable and gets documented in dbt's `schema.yml`;
having it in imperative Python code is not. Furthermore, dbt tests turn
these rules into automatically verifiable contracts.

### 10. Data quality findings in `accounts[]`

**Healthy structure**:
- 17,529 accounts, all with unique `account_id` (dedup OK).
- 5,000 customers, 2-5 accounts each, even distribution.
- Zero NULLs in categorical fields (`account_type`, `status`,
  `currency`, `branch_code`).

**`account_type` - 4 real values, 20 variants in bronze**
- 5 variants per value, all differentiated only by whitespace.
- Resolved in Spark with `trim()`: 20 → 4 canonical values.
- Canonical set: `savings`, `checking`, `investment`, `credit_card`.

**`status` - 3 real values, 12 variants in bronze**
- Variants due to (a) inconsistent casing, (b) Spanish translations.
- Trim does NOT collapse these variants (they are semantic decisions).
- Detected families:
  - **active** (5,877): `active`, `Active`, `ACTIVE`, `activo`
  - **frozen** (5,811): `frozen`, `Frozen`, `FROZEN`, `congelado`
  - **closed** (5,841): `closed`, `Closed`, `CLOSED`, `cerrado`
- To be resolved in dbt silver with `lower()` + mapping
  `{cerrado→closed, activo→active, congelado→frozen}`.

**Brief vs real data divergence**:
- The brief documents `status ∈ {active, inactive, suspended, closed}`.
- The data contains `{active, frozen, closed}`.
- Neither `inactive` nor `suspended` appear; `frozen` does.
- The `accepted_values` dbt test will use the REAL set, not the
  documented one.
- The brief explicitly warns about "unexpected statuses": this is
  exactly that case.

**`currency` - clean, coherent LATAM domain**:
- 8 values: USD + 7 local currencies (PEN, COP, MXN, UYU, BRL, ARS, CLP).
- Business finding: **~50% of accounts (8,734 / 17,529) are
  denominated in USD**, consistent with informal dollarization in the
  region (especially AR and UY).
- **Implication for Gold**: for question 2 ("total account balances by
  country"), it must be decided whether to report in nominal currency or
  convert to a common currency. Decision to be made on day 5-6.

**`branch_code` - clean**:
- Consistent pattern `BR-NNN` (3 digits).
- Even distribution in the top 20 (29-36 accounts per branch).
- No nulls, no weird variants.


## Deduplication strategy

The pipeline applies deduplication at two layers, each handling the type
of duplication that is natural at its abstraction level.

### Why deduplicate

Bronze is append-only: each DAG run inserts the full dataset again with a
new `load_id` and `load_timestamp`. This is intentional — Bronze must be
a faithful audit log of what arrived from source, not a deduplicated
view. During development the DAG runs multiple times, so by the time the
data reaches Silver, the same `customer_id` (and every nested PK inside)
appears in multiple Bronze rows.

Without deduplication, the staging tables in Silver would carry those
duplicates forward, breaking PK uniqueness tests in dbt and inflating
every downstream aggregate.

### Where deduplication happens

**PySpark (Silver staging tables) — dedup by array PK.**

Each of the three flatteners (`flatten_accounts.py`,
`flatten_transactions.py`, `flatten_loans.py`) deduplicates by the
natural primary key of the array it explodes:

| Script                       | Output table                  | Dedup key        |
|------------------------------|-------------------------------|------------------|
| `flatten_accounts.py`        | `silver.stg_accounts`         | `account_id`     |
| `flatten_transactions.py`    | `silver.stg_transactions`     | `transaction_id` |
| `flatten_loans.py`           | `silver.stg_loans`            | `loan_id`        |

The shared logic lives in `spark/utils.py::deduplicate_by_pk`, which
applies a window function partitioned by the PK and ordered by
`load_timestamp DESC`, keeping `row_number() == 1`. In plain words:
**for each PK, the row that came from the most recent Bronze load is
kept.**

This is the correct semantics for a staging table: Silver should reflect
the latest known state of each entity, not its history. If we ever need
history (SCD2-style), that lives in a dedicated dimensional model in
silver/gold, not in staging.

**dbt (Silver dimensions) — dedup by customer_id.**

Customer-level deduplication is NOT done in PySpark. The reason is the
tool roles of this project: PySpark's job is array flattening, and the
customer record itself has no nested arrays to flatten — its flat fields
are already flat in the source JSON. So `dim_customer` is built directly
in dbt, reading from `bronze.raw_fintech_data` via a staging model that
parses the `jsonb` and applies the same "latest `load_timestamp` wins"
rule using
`qualify row_number() over (partition by customer_id order by load_timestamp desc) = 1`.

This separation keeps each tool doing what the project asks for, and
avoids materializing an intermediate `silver.stg_customers` table that
would duplicate work between layers.

### Edge cases

- **Same `load_timestamp` for two versions of the same PK** — happens
  if the DAG triggers twice within the same second. Spark picks one
  arbitrarily; since rows are byte-identical when this happens (same
  source file, same parsing), it does not matter which one wins.
  Documented but not protected.
- **NULL PKs** — `row_number()` treats NULLs as their own group and
  would keep one. We do not filter NULLs in PySpark; if a NULL PK
  appears, it is an upstream data quality bug that dbt's `not_null`
  test will catch and fail loudly, which is the behavior we want.
- **Customers without loans** — `flatten_loans.py` uses `explode` (not
  `explode_outer`), so customers with empty `loans[]` produce zero
  rows. This is correct: `silver.stg_loans` is a fact table of loans,
  not a customer × loan matrix. Metrics like "% of customers with
  loan" are built in Gold via `LEFT JOIN` from `dim_customer`.

### Sanity checks

Each flattener logs three numbers per run:
- `bronze records read` — how many rows came from
  `bronze.raw_fintech_data`
- `<entity> after explode` — how many rows after exploding the array
- `<entity> after dedup` / `duplicates dropped` — final count vs.
  dropped

In a healthy run with N Bronze loads of the same dataset, `duplicates
dropped` should equal `(N-1) × <expected entity count>`. If higher,
there is an upstream PK collision that was not there before; if lower, a
load came in partial.



## Silver dbt design decisions

### Refinement of the Spark vs dbt split

The original split (Spark = syntactic, dbt = semantic) is refined to be
more precise:

- **Spark handles array flattening** (`accounts[]`, `transactions[]`,
  `loans[]`): cardinality changes via `explode`; distributed-compute
  semantics genuinely apply.
- **dbt handles flat fields and nested objects** (customer fields,
  `credit_info{}`, `digital_engagement{}`): cardinality is preserved 1:1
  with customer; Postgres `jsonb` operators are both performant (5k
  rows) and idiomatic.
- **Deduplication follows the same logic:** array entities (accounts,
  transactions, loans) are deduplicated in Spark as part of their
  explode pipeline. Customer dedup happens in dbt because customers are
  not exploded — they are extracted flat from Bronze.

**Rationale:** the meaningful distinction is *whether explode is needed*,
not *whether dedup is needed*. Forcing customer through Spark just to
deduplicate would require a script that actually flattens nothing,
breaking the (`flatten_*`) naming convention and introducing a fourth
Spark script without distributed-compute justification.

### status field divergence (correction to Day 1 EDA notes)

Previous notes confused two `status` fields. Distinct findings:

- **`account.status`** (`silver.stg_accounts`): contains
  `active / frozen / closed`. Diverges from the brief, which specifies
  `active / inactive / suspended / closed`. Divergence documented; the
  `accepted_values` test reflects the observed data.
- **`customer.status`** (bronze raw): contains 20 variants on the
  surface but only 4 canonical values:
  `active / inactive / suspended / closed`. **These 4 canonical values
  match exactly the brief.** The variants are casing chaos (`Active`,
  `ACTIVE`) plus Spanish translations (`activo`, `suspendido`,
  `cerrado`, `inactivo`, including their casing variants). 4,223 of
  5,000 records (84.5%) use a canonical value; 777 (15.5%) require
  normalization.
- Normalization happens in `stg_customers.sql` via lowercase + Spanish
  → English mapping. `accepted_values` test over the resulting 4
  canonical values.

### Data quality findings (from auditing `silver.dim_customer`, dropped today)

The legacy `silver.dim_customer` table (origin unknown; no script
generates it; dropped today) was useful as an audit surface and
surfaced three findings:

- **340 customers (6.8%) have invalid coordinates:** `lat` outside
  [-90, 90] or `lon` outside [-180, 180]. Probably a generator bug.
  Treatment: boolean flag `is_geo_valid` in `dim_customer`; `lat`/`lon`
  are set to NULL when invalid. Preserves the record (no row loss)
  while making the data quality issue explicit and queryable.
- **`nationality` equals `country` in 100% of records.** The field is
  informationally redundant. Treatment: kept in `dim_customer` as a
  verbatim copy (in case downstream analysis ever differentiates them),
  but a `dbt_utils.expression_is_true` test is added that asserts
  equality. If the test ever fails in a future run, it is a signal to
  revisit.
- **City names have typos** (e.g. `Lma` for `Lima`). Treatment:
  deferred. Documented as a known dataset issue; does not affect
  `country`-based aggregations (the primary geographic dimension for
  business questions 2, 6, 10).

### Bucketing definitions

These cover business questions 9, 11, 21, and partially 5, 6, 7.

**Age buckets** (from `date_of_birth`, computed via `AGE()`):
- `18-25` — students / early career
- `26-35` — millennials, peak product acquisition
- `36-50` — peak income years
- `51-65` — pre-retirement
- `65+` — retired

**Rationale:** standard LATAM fintech segmentation aligned with life
stages and product affinity. Any person under 18 in the data is treated
as a data quality issue (flagged, not bucketed).

**Tenure buckets** (from `registration_date`, computed via month
difference):
- `new` — < 6 months
- `established` — 6 to 24 months
- `loyal` — > 24 months

**Implementation:** macro `macros/tenure_bucket.sql`. Computes the delta
in total months using
`extract(year from age()) * 12 + extract(month from age())` (the
portable form in Postgres — `extract(month from age())` only returns
the month component [0-11], not the total).

**Risk score buckets** (from `risk_score`, 0-100 numeric):
- `low` — 0 to 30
- `medium` — 30 to 60
- `high` — 60 to 85
- `critical` — 85 to 100

Aligned with business question 9. Edges were chosen to approximately
roughly equal-population quartiles based on EDA.

**Credit score buckets** (FICO standard):
- `poor` — 300-579
- `fair` — 580-669
- `good` — 670-739
- `very_good` — 740-799
- `excellent` — 800-850

### dbt model naming conventions

- `stg_*` — staging layer, one model per source table or extracted
  object. Materialization: view (cheap to rebuild, no aggregation).
- `dim_*` — Silver-layer dimensions. Materialization: table (joined
  downstream by Gold models).
- `fact_*` — Silver-layer facts (transactions, loan_snapshots). Tables.
- Gold marts use the `mart_*` prefix (decided in Day 6+).


### Note on legacy table cleanup

A `silver.dim_customer` table existed at the start of Day 6, of unknown
origin (no Spark script generates it; probably created during Day 1
exploration or PoC work). It was manually dropped via
`DROP TABLE silver.dim_customer` before starting modeling in dbt. No
reproducible script is included because the table is not part of the
pipeline — the canonical `dim_customer` is now built by dbt and any
future clean-clone setup will never produce the legacy version.


### Generalized pattern: all customer-level categoricals need normalization

Day 6 testing revealed that the casing chaos + Spanish translation
pattern documented for `customer.status` is NOT isolated to that field.
It is a **generalized generator pattern** that affects every categorical
field in the dataset:

- `customer.status` — 20 variants, ~15.5% non-canonical (documented on
  Day 1).
- `customer.kyc_status` — 8 variants, ~7.6% non-canonical. Only casing
  variants (no Spanish translations). Normalized via
  `normalize_kyc_status`.
- `customer.customer_segment` — 16 variants, ~15.7% non-canonical. Both
  casing and Spanish translations. Normalized via
  `normalize_customer_segment`.

**Implication for downstream Silver models:** any categorical field
arriving from Bronze (transactions.channel, transactions.category,
transactions.type, transactions.status, accounts.account_type,
loans.type, loans.status, gender, etc.) must be assumed to have
casing/translation variants until empirically proven otherwise. Each one
has its own `normalize_*` macro following the same pattern (lowercase +
trim, optional CASE map ES→EN, else-passthrough so unexpected values
fail the tests loudly).


### Tests configured as `warn` for documented data quality issues

Some `not_null` tests are intentionally set with `severity: warn` instead
of the default error level. This captures source-generator data quality
issues without blocking the build. Affected fields:

- `silver.stg_accounts.balance` — 486 NULLs (2.8%), uniformly distributed
  across all `account_type` values. Confirmed not to be a parsing or join
  issue; arrives as NULL in the Bronze JSON for those records.

**The pattern is:** if a test fails due to source data quality (not a bug
in our code), `warn` keeps the issue visible in `dbt test` output while
the pipeline still runs. If the dataset ever refreshes and the NULL rate
changes significantly, we'll see it in the warn count.

## Staging, dimensions and aggregate models

### Macro architecture: normalize_casing + entity-specific

The "Option C" macro hierarchy was adopted:
- `normalize_casing(col)` — base macro that does `lower(trim(col))`.
- `normalize_<entity>_<field>(col)` — thin or rich wrappers using the
  base.

**Thin wrappers** (only delegate to normalize_casing):
`normalize_kyc_status`, `normalize_transaction_status`,
`normalize_loan_status`, `normalize_loan_type`. They exist for naming
consistency: call sites read as `{{ normalize_kyc_status(...) }}`
(intention-revealing) instead of `{{ normalize_casing(...) }}`
(generic).

**Rich wrappers** (lowercase + Spanish-to-English mapping):
`normalize_customer_status`, `normalize_customer_segment`,
`normalize_account_status`, `normalize_transaction_type`,
`normalize_transaction_category`, `normalize_collateral_type`.

For `account_type` and `transaction.channel`, `normalize_casing` is
applied inline in the model (no dedicated macro) because the field has
only casing variants and creating a macro for a one-liner would be
ritualistic.

### Schema separation: silver_raw (Spark) vs silver (dbt)

Day 6 discovered a naming collision: Spark wrote to
`silver.stg_accounts` and dbt tried to create a view with the same
fully-qualified name. dbt silently failed to materialize, causing the
tests to run against Spark's raw output instead of the normalized view.

**Fix:** the `silver_raw` schema was introduced for Spark outputs. dbt
reads from `silver_raw.stg_*` (declared as a source) and writes
views/tables to `silver`.

**Implementation:**
- Added env variable `SPARK_TARGET_SCHEMA` to `.env`, `env.example`,
  `docker-compose.yml`.
- Created constant `TARGET_SCHEMA` in `spark/utils.py` that reads from
  env.
- Updated the 3 Spark flatten scripts to use
  `f"{TARGET_SCHEMA}.stg_<name>"`.
- Updated `dbt/models/sources.yml`: `schema: silver_raw`.
- Migrated existing tables with `ALTER TABLE ... SET SCHEMA silver_raw`.

The default value `silver_raw` is hardcoded in `utils.py` so the script
works even if the env var is missing; the env var allows override for
test environments or future production deployments.

### Centralization of date parsing

Spark's `to_date("yyyy-MM-dd")` silently NULL-ed any non-ISO date. EDA
discovered 4 formats in the dataset's date fields:
- ISO (~91%): `2026-05-08`
- Compact (~3%): `20260613`
- Slash DMY (~3%): `26/04/2026`
- Dash MDY (~3%): `06-13-2024`

**Decision:** Spark passes dates as text; dbt's
`parse_date_multi_format` macro handles the 4 formats. **Rationale:**
choosing which formats are valid is a semantic decision, not a syntactic
one. Per the division of responsibilities, dbt owns it.

**Used by:** `dim_customer` (date_of_birth, registration_date),
`stg_accounts` (opened_date), `stg_transactions` (transaction_date),
`stg_loans` (start_date, end_date).

### Generalized data quality patterns

Day 6 confirmed and extended the generator's data quality patterns:
- **Casing chaos + Spanish translations** affect every categorical
  field (customer.status, customer.kyc_status, customer.customer_segment,
  account.status, transaction.type, transaction.status,
  transaction.category, loan.status, loan.type, loan.collateral_type,
  digital_engagement.preferred_channel).
- **String missing markers** (`''`, `'NA'`, `'N/A'`, `'null'`,
  `'NULL'`) appear in text categoricals AND in numeric fields. The
  `safe_cast_numeric` macro NULL-s the 5 markers before casting.
- **Unparseable numeric strings** (`'$78.26'`, `'89.5 USD'`, `'83,5'`)
  appear in 38 utilization_pct records (0.4%). The safe_cast_numeric
  macro returns NULL for any string that does not match
  `^-?[0-9]+\.?[0-9]*$`.
- **Out-of-range sentinels** in credit_score (~10%): values like
  999999, 0, negatives. Handled via the raw + flag + validated pattern.

### raw + flag + validated pattern

Consistently applied for fields with genuine data quality issues that
cannot be cleanly recovered:
- `dim_customer.lat/lon` + `is_geo_valid` (6.8% invalid coordinates).
- `stg_credit_info.credit_score_raw` + `is_credit_score_valid` +
  `credit_score` (10% out of range).
- `stg_credit_info.utilization_pct_raw` + `is_utilization_pct_valid` +
  `utilization_pct` (0.4% unparseable + range issues).

The pattern preserves the original value for audit, exposes a boolean
for filtering, and provides a NULL-ed validated version for
aggregations.

### EUR currency in transactions

Transactions contain ~1% EUR activity (924 rows). EUR is NOT present in
accounts. Likely cross-border activity. Documented in the
stg_transactions yaml; relevant for business question 19 (international
transfer patterns).

### Tests configured as `warn` for documented data quality issues

- `stg_accounts.balance` — 486 NULLs (2.8%), uniformly distributed.
- `stg_transactions.amount` — 2,619 NULLs (3.0%), uniformly distributed.

Both are source-generator data quality, not parsing or join issues. Warn
surfaces them without blocking the build.

### customer_summary moved from gold to silver as agg_customer_activity

Originally created in a previous session as `gold.customer_summary`.
After reflection during Day 6, its grain (1 row per customer) and
content (activity counts) fit better as a silver aggregate, not as a
business mart. Renamed to `agg_customer_activity` in silver. The old
gold model was dropped (file removed, Postgres table dropped via
CASCADE).

### dim_geography is hardcoded, not derived

7 countries from the brief (CO, UY, AR, MX, CL, PE, BR), implemented
with `VALUES` in the model. The region is hardcoded as 'LATAM' for
future extensibility. The city is NOT in this dimension because of typos
in `dim_customer.city` (e.g. `Lma` for `Lima`); `customer.city` can be
used directly when needed.

### Tag v0.2.0-silver (Silver layer close)

```bash
git tag -a v0.2.0-silver -m "Silver layer complete: PySpark flattening + dbt cleaning, dimensions, facts, 160 tests passing (7 warns documented as generator DQ)"
git push origin main
git push origin v0.2.0-silver
```


## Gold design + first 3 marts

### Day executive summary

- Gold layer design: 8 marts mapped to 24 questions (not one mart per question).
- 3 marts implemented and tested: `mart_acquisition_trend`, `mart_account_mix`, `mart_customer_360`.
- 65 dbt tests for the 3 marts, all PASS.
- 5 bucketing macros created in Gold; 1 new defensive macro (`safe_cast_boolean`).
- Structural refactor in Silver: `stg_credit_info` and `stg_digital_engagement` renamed to `dim_*` and materialized as `table`.
- 5 new data quality findings documented (boolean variants, NULL balance, BETWEEN with decimals, view-lazy cast, non-propagated CTE).
- Business decisions frozen: revenue, delinquency, 5 bucket sets.

---

### 1. Architecture: 8 marts covering 24 questions

Mart → questions mapping (**original Day 8 design — superseded by Day 9**):

| Mart (Day 8) | Grain | Questions |
|------|-------|-----------|
| `mart_customer_360` | 1 row/customer | Q1, Q9, Q10, Q11, Q13, Q14, Q24 + credit profile |
| `mart_revenue_by_segment` | segment × month | Q1 (rollup) |
| `mart_transactions_summary` | category × channel × month | Q3, Q15, Q16, Q17, Q18 |
| `mart_loan_portfolio` | loan_id (with buckets) | Q4, Q5, Q8, Q23 |
| `mart_acquisition_trend` | month | Q12 |
| `mart_digital_engagement` | segment × age_bucket | Q20, Q21 |
| `mart_account_mix` | country × account_type × currency | Q2, Q22 |
| `mart_international_transfers` | currency_pair × month | Q19 |

Coverage: 24/24. **Do not** create a mart per question — the brief
evaluates "reusability and clarity of metrics" and "model design
quality", which penalizes redundancy.

**Day 9 update:** during construction it was identified that several of
these marts grouped questions with genuinely different grain and diluted
clarity. They were split into more specific versions. Final mapping
(implemented in `dbt/models/gold/`):

| Mart (final, 16 total) | Grain | Questions |
|------|-------|-----------|
| `mart_acquisition_trend` | month | Q12 |
| `mart_customer_360` | 1 row/customer | Q1 (rollup), Q9, Q10, Q11, Q13, Q14, Q24 |
| `mart_revenue_by_segment_usd` | segment | Q1 (USD) |
| `mart_account_mix` | country × account_type × currency | Q2, Q22 |
| `mart_delinquency_by_segment` | segment | Q5 |
| `mart_credit_score_by_country` | country × score_bucket | Q6 |
| `mart_utilization_vs_delinquency` | utilization_bucket | Q7 |
| `mart_risk_buckets` | risk_bucket | Q9 |
| `mart_loan_dpd` | loan_type × dpd_bucket × currency | Q8 |
| `mart_loan_composition` | loan_type × status × currency | Q4, Q23 |
| `mart_tx_by_channel` | channel × currency | Q3, Q17, Q18 |
| `mart_tx_by_category` | category × currency | Q15 |
| `mart_tx_by_dow` | day_of_week × currency | Q16 |
| `mart_international_transfers` | origin_country × tx_currency | Q19 |
| `mart_digital_adoption_by_segment` | segment | Q20 |
| `mart_channel_preference_by_age` | age_bucket × channel | Q21 |

Final coverage: 24/24 (23 ✅ + Q13 ⚠️ documented with spec/dataset
divergence).

---

### 2. Fusion `mart_credit_risk` → `mart_customer_360`

Reason: the dataset is a single point in time (no historical snapshots).
Two marts with `customer` grain would be redundant. `mart_customer_360`
carries profile + credit_score + utilization + risk_bucket in a single
table. If snapshots existed in the future, it would be split into
`dim_customer` + `fct_credit_risk_snapshot`.

---

### 3. Revenue definition (the brief asks to define it)

`revenue = transaction_fees + interest_income`

- **`transaction_fees`**: `SUM(amount)` in `silver.fct_transactions`
  where `transaction_type = 'fee'` AND `status = 'completed'`.
- **`interest_income`**: `SUM(outstanding_balance × interest_rate / 12)`
  over loans with `status IN ('current', 'delinquent')` (active loans
  generate interest; default and paid_off do not).

Represents what the bank **earns**, not the transacted volume. Volume is
reported separately (`transaction_volume`) so as not to conflate Q1
(revenue) with Q15 (volume).

**Related DQ finding:** dataset fees have an almost uniform `status`
distribution (~25% in each of the 4 statuses), suggesting a synthetic
generator without business logic. In real banking `completed` should be
~95%. So the filter on `completed` is critical: without it, revenue
gets inflated 4x.

---

### 4. Delinquency definition (the brief asks to define it)

We trust the `status` field of the dataset as it comes in
`silver.dim_loan`/`silver.fct_loans`:
- `current` — up to date
- `delinquent` — in arrears
- `default` — non-payment
- `paid_off` — settled

Reason: the dataset already provides the status semantically. dbt tests
verify internal consistency (`accepted_values`). Inconsistencies between
`status` and `days_past_due`, if they exist, are documented but not
corrected in Gold — the source is truth.

---

### 5. Analytical buckets: distribution between Silver and Gold

| Dimension | Buckets | Lives in | Justification |
|-----------|---------|----------|---------------|
| `age_bucket` | 18-25, 26-35, 36-50, 51-65, 65+, under_18 (DQ flag), unknown | **Silver** (`dim_customer`) | Cross-cutting use in 3+ marts. |
| `tenure_bucket` | new (<6m), established (6-24m), loyal (>24m), unknown | **Silver** (`dim_customer`) | Cross-cutting use in 3+ marts. |
| `risk_bucket` | low, medium, high, critical | Gold (macro `get_risk_bucket`) | Literal Q9 → 4 mandatory levels. |
| `utilization_bucket` | healthy (<30%), moderate (30-70%), high (>70%) | Gold (macro `get_utilization_bucket`) | FICO rule. |
| `credit_score_bucket` | poor (300-579), fair (580-669), good (670-739), very_good (740-799), exceptional (800-850) | Gold (macro `get_credit_score_bucket`) | Standard FICO ranges. |
| `days_past_due_bucket` | current (0), 1-30, 31-60, 61-90, 90+ | Gold (macro `get_days_past_due_bucket`) | Bank provisioning regulatory buckets. |
| `tenure_years` | computed decimal | Gold (auxiliary macro `get_tenure_years`) | Cases where tenure-as-number complements the categorical bucket. |

**Operational heuristic for placing future buckets:**
- Used by 3+ marts? → Silver, materialized column in the dim.
- Used by 1-2 marts? → Gold, via macro.
- Definition changes frequently? → Gold (cheap changes).
- Stable and very general use? → Silver.

**DECIMAL risk bucket (fine numeric ranges):** see finding #11 below
about the BETWEEN bug.

---

### 6. Division of responsibilities per layer (refinement to 3 levels)

The original principle "Spark = syntactic / dbt = semantic" is extended
to three levels after Day 8:

| Layer | Responsibility | Examples |
|------|-----------------|----------|
| **PySpark → `silver_raw`** | Syntactic cleaning | `trim()`, empty → NULL, dedup, array flattening |
| **dbt → `silver`** | Semantic normalization + cross-cutting dimensions | Type casts, casing/translation normalization, neutral arithmetic derivations (`age`, `tenure_months`), cross-cutting buckets, constraints |
| **dbt → `gold`** | Specific analytical bucketing + business logic | `risk_bucket`, `utilization_bucket`, etc.; revenue, delinquency, aggregations by business grain |

**`silver_raw` vs `silver` sub-division (formalized in Day 8):**

| Schema | Owner | Content |
|--------|-------|---------|
| `silver_raw` | PySpark (via JDBC) | Post-flatten staging: exploded arrays, dedup, syntactic clean |
| `silver` | dbt | Clean models: type casts, semantic normalization, nested-object flattening, constraints |

Resulting contract:
- Gold reads only from `silver.*`, never from `silver_raw.*`.
- dbt silver reads from `silver_raw.*` and from `bronze.*` (for
  non-array objects like `credit_info`).
- Spark never writes into `silver.*`.

**Operational benefit:** if dbt silver explodes, `silver_raw` stays
intact and dbt can be re-run without re-running Spark. Isolates failures
per layer. The `SPARK_TARGET_SCHEMA=silver_raw` env var in `.env`
parameterizes Spark's destination.

---

### 7. `silver.agg_customer_activity` as structural, not analytical, support

This table pre-aggregates pure counts (`accounts_count`,
`transactions_count`, `loans_count`, `total_products`) per customer.
Day 8 confirmed that its place in Silver is correct: **it only contains
structural counts, not interpreted business metrics**. Analogous to
`dim_date` — it facilitates downstream without imposing interpretations.
For Q24 (avg products per segment) it avoids 3 LEFT JOIN + COUNT DISTINCT
in every consuming mart.

**General criterion:** a Silver aggregation is legitimate if it only
counts/groups structural attributes of the dataset (relationship
cardinality). An aggregation that applies business rules (revenue,
delinquency rate, segment definitions) belongs in Gold.

---

### 8. Silver refactor: `stg_credit_info` and `stg_digital_engagement` → `dim_*`

**Context:** designing `mart_customer_360` it was discovered that
`silver` had two models with `stg_*` prefix (legacy from an incomplete
rename on Day 7). The rest of Silver already followed the
`dim_*`/`fct_*`/`agg_*` convention. That asymmetry was closed:

- **`stg_credit_info` → `dim_credit_info`**: file rename, drop of orphan
  view in Postgres, YAML update, `materialized='table'`. 9/9 tests PASS.
- **`stg_digital_engagement` → `dim_digital_engagement`**: ditto. 7/7
  tests PASS post-refactor.

**Final Silver state:** all tables with `dim_*`/`fct_*`/`agg_*` prefix.
No `stg_*` in the `silver` schema (the `stg_*` ones live only in
`silver_raw`, Spark output).

**Operational lesson:** after a `git mv` of a dbt model, always follow
with `DROP` of the old object in the DB + `dbt run --full-refresh` +
update `schema.yml`. Otherwise, a silent inconsistency remains between
code and warehouse.

---

### 9. DQ finding: boolean variants in digital_engagement

When materializing `dim_digital_engagement` as a table (it was a view
before), Postgres exploded with
`invalid input syntax for type boolean: "si"`. The 4 boolean columns
(`mobile_app_registered`, `web_banking_registered`, `push_notifications`,
`paperless_statements`) contain non-standard variants:

| Variant | Language/encoding | Affected rows |
|---|---|---|
| `true`/`false` | Standard | ~8,500 |
| `yes`/`no` | English casing | ~120 |
| `si` | Spanish without accent | ~86 |
| `0`/`1` | Numeric | ~118 |

**Solution:** macro `safe_cast_boolean()` analogous to
`safe_cast_numeric()` from Day 6. Accepts
`true/t/yes/y/sí/si/1` → `TRUE`, `false/f/no/n/0` → `FALSE`, everything
else → NULL.

**Architectural lesson:** **converting Silver views to tables exposes
latent cast bugs.** View = lazy cast at read-time = bugs hidden until
someone queries the offending row. Table = eager cast at write-time =
bugs explicit at `dbt run`. Materializing as table is better for DQ, not
just for performance.

**Cross-impact analysis:** `bankruptcy_flag` in `credit_info` is the
only other boolean column in Silver. Verified clean (only `t`/`f`). No
fix required.

**Derived principle:** every non-trivial cast from JSON (numeric,
boolean, date) must use a defensive macro with NULL fallback, not native
`::type` casting. Available macros: `safe_cast_numeric`,
`safe_cast_boolean`, `parse_date_multi_format`. Native cast only when
data is provably clean and the model is a view (read-time, fail-fast
acceptable).

---

### 10. DQ finding: 183 active accounts with NULL balance (~3.1%)

Discovered during cross-validation of `mart_account_mix`. Distribution:
- Uniform among the 4 account_types (savings 29, checking 27, investment
  25, credit_card 23 in USD)
- Appears across the 8 currencies of the dataset
- No concentration pattern → synthetic-dataset generation noise

**Treatment in Gold:** the 183 accounts ARE INCLUDED in
`accounts_count` (Q22 is about popularity, not balance) but DO NOT
contribute to `total_balance`/`avg_balance` (SQL `SUM`/`AVG` ignore NULL
by definition). New column `accounts_with_balance` exposes the
discrepancy, making the missing-data rate queryable from BI.

**Modeling lesson:** when a column is added to an intermediate CTE in
dbt, it must be propagated explicitly in every downstream CTE that does
`SELECT enumerated`. It is the typical cause of the bug "the column
exists in the file but not in the table". `SELECT *` between
intermediate CTEs reduces this risk.

---

### 11. Finding: bug in `get_risk_bucket` — `BETWEEN` with decimals

Discovered while building `mart_customer_360`. The original macro used
`BETWEEN 0 AND 30`, `BETWEEN 31 AND 60`, etc. — correct pattern for
integers but **fatal with decimals**. Since `risk_score` is `numeric`,
values like 30.05, 30.08, 60.01, 85.01 fell into gaps between buckets
and were classified as `'unknown'`. 147 customers (~3%) affected.

**Solution:** replace `BETWEEN` with explicit comparators.

```sql
-- Before (buggy with decimals):
when {{ x }} between 0 and 30 then 'low'
when {{ x }} between 31 and 60 then 'medium'

-- After (correct):
when {{ x }} >= 0  and {{ x }} <= 30 then 'low'
when {{ x }} >  30 and {{ x }} <= 60 then 'medium'
```

**Adopted convention:** inclusive upper bounds (`<=`), exclusive lower
bounds (`>`), except the first bucket which uses `>=` to include 0.

**Cross-analysis of the other bucketing macros:**
- `get_credit_score_bucket`: input `integer` (FICO scores are integers),
  `BETWEEN` works. ✅
- `get_days_past_due_bucket`: input `integer`, `BETWEEN` works. ✅
- `get_utilization_bucket`: input `numeric`, but uses `<`/`>` without
  BETWEEN — no gaps. ✅

**General lesson:** bucketing macros with `BETWEEN` are only safe for
integer inputs. For decimals: use explicit comparators.

---

### 12. Multi-currency in `mart_account_mix`: don't convert, grain by currency

**Original decision (Day 8):** the dataset did not include an FX rate
table. Summing `balance` between USD and ARS would be mathematically
incorrect. Decision: marts that aggregate balance carry `currency` in
the grain and avoid invented conversion.

Result in `mart_account_mix`: grain = `country × account_type × currency`.
Q22 (popularity by type) comes out by aggregating by type. Q2 (balances
by country) comes out by aggregating by country showing currencies per
country. The dashboard can filter by currency or display side by side.

**Day 9 update:** `seeds/fx_rates.csv` was added with USD conversions
for the 8 LATAM currencies + EUR. `mart_revenue_by_segment_usd` consumes
this seed. `mart_account_mix` **keeps the grain by currency** (decision
preserved) because balances are typically reported in native currency
for audit, while cross-country revenue does require USD unification.
Power BI can apply conversion via join with `fx_rates` when the case
demands it.

---

### 13. `mart_customer_360`: complete design

**Grain:** 1 row per customer.
**Answers:** Q1 (revenue by segment), Q9 (risk buckets), Q10 (count by
country/city), Q11 (age by segment), Q13 (status breakdown), Q14 (KYC
distribution), Q24 (products per segment).

**Sources (all LEFT JOIN from `dim_customer` to not lose customers):**
- `silver.dim_customer` — identity, demographics, segment, age_bucket,
  tenure_bucket
- `silver.dim_credit_info` — credit_score, utilization, late_payments,
  bankruptcy_flag
- `silver.dim_digital_engagement` — mobile_app, web_banking
- `silver.agg_customer_activity` — accounts_count, loans_count,
  transactions_count, total_products (Q24)
- `silver.int_customer_monthly_revenue` — centralized revenue (fees +
  interest, native + USD). See section "Refactor:
  int_customer_monthly_revenue" (Day 10) for the complete definition.

**Day 10 note:** the intermediate's revenue columns
(`monthly_fee_revenue_native`, `monthly_interest_revenue_native`,
`total_monthly_revenue_native`) are renamed within the mart back to the
pre-refactor convention (`monthly_fee_revenue`, `monthly_interest_income`,
`total_revenue_monthly`) in a `revenue` CTE to preserve the downstream
contract with Power BI without touching dashboards.

**Final columns (30):** identity (1) + demographics (5) + relationship
(5) + risk (2) + credit profile (8) + digital (2) + products (4) +
revenue (3).

**Critical decisions:**
- **LEFT JOIN always from `dim_customer`:** we don't lose customers due
  to missing credit_info/digital/loans/transactions. Q10/Q13/Q14
  maintain the full universe.
- **`COALESCE(..., 0)` in counts and revenue:** a customer with no
  transactions has `total_fees_paid = 0`, not NULL. Simplifies
  aggregations in PowerBI.
- **DO NOT use `COALESCE` in credit_score/utilization_pct:** NULL there
  means "non-validatable data" (different from zero). PowerBI filters
  them naturally.
- **`WHERE risk_score IS NOT NULL` as a defensive safeguard:** current
  dataset has no NULLs, but it protects future loads.

---

### 14. dbt tests for Gold: 65 tests created, all PASS

`dbt/models/gold/_gold__mart.yml` covers the 3 marts of the day with:
- `unique` + `not_null` on PKs.
- `relationships` from `mart_customer_360.customer_id` →
  `dim_customer.customer_id`.
- `accepted_values` on all categoricals: country, account_type,
  currency, customer_segment, kyc_status, status, age_bucket,
  tenure_bucket, risk_bucket, credit_score_bucket, utilization_bucket.
- `dbt_utils.expression_is_true` for numeric invariants (non-negative,
  valid ranges).

**`accepted_values` of `risk_bucket`** intentionally does NOT include
`'unknown'` — if it appears in any future run, it indicates regression
in the `get_risk_bucket` macro (see finding #11).

**Lesson on `dbt_utils.expression_is_true` at column level:** the macro
auto-prefixes the column name before the expression. Patterns like
`"col_x is null or col_x between 0 and 100"` generate invalid SQL
(`where not(col_x col_x is null or...)`). **Solution:** move those
tests to model level (`tests:` sibling of `columns:`), where the
expression is not auto-prefixed.

---

### 15. Operational gotcha: empty `$env:VAR` in new PowerShell sessions

The `.env` variables only load in `docker compose`, NOT in the PowerShell
session. Commands like
`docker exec qversity_postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "..."`
fail with `role "-d" does not exist` because `$env:POSTGRES_USER`
expands to an empty string and `psql` reinterprets the flags.

**Portable solution:** read the envs from inside the container with
single quotes on the outside:

```powershell
docker exec qversity_postgres bash -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "..."'
```

Single quotes prevent PowerShell from expanding `$POSTGRES_USER` before
sending the command to the container; bash inside the container does
have the envs loaded.

---

## Business definitions for Gold marts

These decisions are upstream of all Gold marts built in Day 9. Each one
is a deliberate trade-off between simplicity, professional
defensibility, and dashboard expressiveness. The rationale is preserved
here so the choice can be re-evaluated if the business context changes.

### 1. Revenue

`revenue = fee_income + interest_income_accrued`

- `fee_income = SUM(transactions.amount)` where
  `type = 'fee' AND status = 'completed'`. Only `completed` fees count;
  failed/reversed/pending are excluded.
- `interest_income_accrued (monthly) = SUM(loans.outstanding_balance * interest_rate / 12)`
  where `loans.status IN ('current', 'delinquent')`. `paid_off` and
  `default` loans do not accrue interest (the first is settled, the
  second would go to non-accrual in real bank accounting).

**Documented trade-off:** interchange fees (merchant commissions on
card transactions) are excluded because the dataset has no field for
them. This would be a real revenue component in a production bank.

### 2. Delinquency

Two non-exclusive flags, both derived from `days_past_due` (not from
`status`):

- `is_delinquent = days_past_due >= 30` — operational/collections metric
- `is_default = days_past_due >= 90 OR status = 'default'` — regulatory
  (Basel/IFRS9 standard)

**DPD takes precedence over the `status` field** in case of conflict
(e.g. status='current' but DPD=45). The model emits a count of such
conflicts as a soft warning, without failing the build.

### 3. Days past due (DPD) buckets

Six aging buckets aligned with standard banking convention. Numeric
prefixes ensure correct alphabetical ordering in Power BI visuals.

| Bucket label             | DPD range  |
|--------------------------|------------|
| `00 - Current`           | 0          |
| `01 - Early (1-29)`      | 1-29       |
| `02 - 30-59 DPD`         | 30-59      |
| `03 - 60-89 DPD`         | 60-89      |
| `04 - 90-179 DPD`        | 90-179     |
| `05 - 180+ DPD`          | 180+       |

The 30/60/90/180 cutoffs match IFRS9 and common charge-off thresholds.

### 4. Credit utilization buckets

Six buckets. NULLs isolated, over-100% explicitly separated (since EDA
confirmed real values > 100% in the source data — they represent
financial stress, not corruption).

| Bucket label                | Utilization range    |
|-----------------------------|----------------------|
| `01 - Healthy (<30%)`       | < 30                 |
| `02 - Moderate (30-60%)`    | 30-60                |
| `03 - High (60-90%)`        | 60-90                |
| `04 - Maxed (90-100%)`      | 90-100               |
| `05 - Over-limit (>100%)`   | > 100                |
| `99 - Unknown`              | NULL                 |

### 5. Credit score buckets

FICO convention (industry standard). Seven values in total: Silver
retains out-of-range values as they are (already cast to int), so the
Gold mart isolates them in a dedicated `00 - Invalid` bucket instead of
letting them contaminate `01 - Poor` or `05 - Excellent`.

| Bucket label                  | Score range                       |
|-------------------------------|-----------------------------------|
| `00 - Invalid (out of range)` | NOT BETWEEN 300 AND 850           |
| `01 - Poor (300-579)`         | 300-579                           |
| `02 - Fair (580-669)`         | 580-669                           |
| `03 - Good (670-739)`         | 670-739                           |
| `04 - Very Good (740-799)`    | 740-799                           |
| `05 - Excellent (800-850)`    | 800-850                           |
| `99 - Unknown`                | NULL                              |

### 6. Age buckets

Neutral decade-based ranges. The project brief uses the neutral phrase
"age group" (Q21), and generational labels (Gen Z, Millennial...) have
disputed cutoffs depending on source (Pew vs Strauss-Howe, etc.), so
neutral bins are preferred.

| Bucket label    | Age range     |
|-----------------|---------------|
| `01 - 18-24`    | 18-24         |
| `02 - 25-34`    | 25-34         |
| `03 - 35-44`    | 35-44         |
| `04 - 45-54`    | 45-54         |
| `05 - 55-64`    | 55-64         |
| `06 - 65+`      | 65+           |
| `99 - Unknown`  | NULL          |

`age` is already derived in Silver from `date_of_birth`.

### 7. Currency strategy

Hybrid: **15 marts in native currency, 1 mart in USD.**

Marts that report monetary figures (account_mix, loan_composition,
loan_dpd, tx_by_channel, tx_by_category, tx_by_dow,
international_transfers, customer_360 in its revenue section) operate in
the **original currency** of the record, with `currency` included in the
grain when applicable. This avoids summing incompatible units (USD + ARS
without rate).

Only `mart_revenue_by_segment_usd` applies USD conversion: revenue per
segment crosses countries and demands a common unit for comparability.
Customer-grain revenue is exposed in both native (consumed by
`mart_customer_360`) and USD (consumed by `mart_revenue_by_segment_usd`)
from the intermediate `int_customer_monthly_revenue`.

**Infrastructure (shared):**
- `seeds/fx_rates.csv` — currency → `rate_to_usd`, as_of_date
- `seeds/country_currency.csv` — ISO country code → local currency code
- Macro `{{ to_usd(amount, currency) }}` — wraps the FX lookup

**FX rates (mid-market snapshot, May 2026):**

| Currency | rate_to_usd |
|----------|-------------|
| USD      | 1.000000    |
| ARS      | 0.000717    |
| UYU      | 0.024850    |
| COP      | 0.000264    |
| MXN      | 0.058000    |
| CLP      | 0.001127    |
| PEN      | 0.270000    |
| BRL      | 0.200000    |

Sources: Xe.com, exchange-rates.org, tradingeconomics.com (May 2026). In
production this would be replaced by a daily FX feed.

**EDA insight (documented, non-blocking):** the dataset shows ~50% of
activity denominated in USD across the LATAM region, reflecting real
dollarization patterns (notably UY and AR). This is a business finding,
not a data quality issue.

### 8. International transfer

`is_international = (type = 'transfer'
                     AND tx_currency != account_currency
                     AND tx_currency != customer_country_currency)`

The strictest of three definitions considered: a transfer is
international only if its currency differs from **both** the originating
account's currency **and** the customer's home country currency. This
minimizes false positives (e.g. a Uruguayan with a USD account sending
USD to another USD account is correctly classified as domestic).

The country→currency mapping is materialized as a seed
(`seeds/country_currency.csv`) to keep the logic out of the model SQL
and consistent with the FX seed pattern.

**Documented limitation:** without destination account data, this
remains a proxy. A cross-border USD-to-USD transfer that does not
require FX would be classified as domestic, which is acceptable for
revenue attribution purposes but not for AML reporting.

---

### Bug found: revenue temporal scale mismatch

After fixing the interest_rate scale (100x), the revenue numbers were
still 1-2 orders of magnitude too high: ~$19k/customer/month in USD,
where bank benchmarks suggest ~$50-500/customer/month.

**Root cause:** `total_revenue_monthly` summed `total_fees_paid`
(cumulative lifetime) with `monthly_interest_income` (single-month
projection), mixing temporal scales. The fees component dominated by an
order of magnitude proportional to `tenure_months`.

**Fix:** normalize fees to monthly average by dividing by tenure_months.
The renaming
`monthly_fee_revenue = total_fees_paid_lifetime / tenure_months`
provides temporal consistency. Lifetime cumulative is preserved as
`total_fees_paid_lifetime` for auditability.

**Lesson:** when summing metrics, verify that all components share the
same temporal grain. Add invariant tests on magnitude (e.g. revenue per
customer within a reasonable range) to catch this class of bug.

**Handled edge case:** ~0.9% of customers (45 of 5,000) have
`tenure_months = 0` (registered in the most recent load). For these,
`monthly_fee_revenue = NULLIF / 0` would give NULL and exclude them
from per-segment averages. We use `COALESCE(..., 0)` to attribute zero
monthly revenue to these customers — conceptually correct (they have no
accumulated fee time yet) and preserves the population count in
aggregates.

### Data scope expansion: EUR added to FX seed

When validating mart_tx_by_channel/category/dow against fx_rates via
relationships test, dbt flagged 924 transactions denominated in EUR
(~1.8% of total volume). The seed's original scope was LATAM-only based
on the project brief, but the dataset legitimately includes EUR
transactions (probably expat or international customers).

**Fix:** EUR added to fx_rates.csv at 1.16 USD (mid-market, May 2026,
source Xe.com). Seed's `accepted_values` test updated accordingly.

**Lesson:** the `relationships` test over currency was effective — it
surfaced a real gap in the data scope before it reached downstream
marts.


### Finding: synthetic data shows abnormal DPD distribution

`mart_loan_dpd` reveals that the source dataset has an unusual DPD
profile:
- Exactly 50.5% of loans at DPD=0 ('Current')
- The remaining ~49.5% spreads across delinquency buckets with a slight
  skew toward shorter durations (average DPD of the delinquent subset
  ≈ 134 days, not exactly uniform).

In real LATAM banking, the expected pattern is:
- 70-85% Current
- Exponential decay between DPD buckets (most late payers cure
  quickly)
- <2% in 180+ DPD (charge-off threshold in most jurisdictions)

**Hypothesis:** the data generator assigns DPD=0 to half the population
(approximating performing loans) but samples the rest from a biased
uniform distribution instead of modeling real delinquency dynamics. This
produces a portfolio that would be insolvent in reality (~16-18% in
180+ DPD across loan types).

**Decision:** keep the bucketing aligned with the IFRS9/Basel convention
(decisions.md §3). Document the divergence as a business-readable
insight in Power BI (Page 3: Risk & Credit). The metrics are computed
correctly; the unrealistic distribution is a property of the synthetic
source data.

Consistent pattern with the flatness finding in revenue-by-segment
(Mart 1): the generator does not correlate financial dimensions (DPD,
loan amount, revenue) with customer segments or loan types in realistic
ways.

### Finding: risk_score does not correlate with observed delinquency

The output of `mart_risk_buckets` reveals a critical observation:

| risk_bucket | customer_count | delinquency_rate |
|-------------|----------------|------------------|
| low         | 1,495          | 65.3%            |
| medium      | 1,539          | 63.2%            |
| high        | 1,218          | 62.8%            |
| critical    |   748          | 60.4%            |

The relationship is non-monotonic and counterintuitive: customers
classified as "low risk" show a HIGHER delinquency rate than those
classified as "critical". In a calibrated risk model, the gradient
should be the opposite and span tens of percentage points (e.g. low:
2-5%, critical: 70%+).

**Interpretation:** the synthetic dataset assigns `risk_score`
independently of the customer's underlying financial behavior. This is a
stronger finding than the previously documented flatness in revenue and
the utilization-delinquency correlation: it directly refutes the
predictive validity of the `risk_score` field.

Combined with the other Day 9 findings (flat revenue by segment, flat
utilization-delinquency correlation, uniform DPD distribution, nearly
flat risk segmentation), the consistent pattern is: the source data
generator samples financially risk-related dimensions independently
instead of modeling their natural correlations. This is a known
limitation of synthetic generators that do not implement joint
distributions.

**Decision:** report the findings honestly on the Risk & Credit page of
Power BI with explicit narrative. The metrics are computed correctly;
the data exhibits independence patterns that would not occur in
production banking.

This is a Day 9 insight worth highlighting in the project README as it
demonstrates the value of validation: a less careful analyst would have
delivered a "risk_score" dashboard without noticing that it predicts
nothing.


### Finding: digital engagement metrics show no demographic gradient

`mart_digital_adoption_by_segment` and `mart_channel_preference_by_age`
reveal that digital engagement metrics in the dataset are independent of
customer demographics:

**Q20 — Mobile adoption by segment:**
- retail: 47.6%
- premium: 49.2%
- private_banking: 49.8%
- sme: 49.9%

Spread: 2.3 percentage points.

**Q21 — Channel preference by age:**
- For each age bucket (18-25, 26-35, 36-50, 51-65, 65+), the five
  channels (mobile/web/atm/branch/phone) each capture ~20% of customers.
- Maximum spread within any age bucket is ~7 percentage points.

In real-world banking, both metrics show strong gradients:
- premium/private_banking customers (typically older, higher net
  worth) show LOWER mobile adoption than retail.
- 18-25 customers typically show 50%+ mobile preference; 65+ customers
  show 50%+ branch/phone preference. The dataset shows ~20%/~20% for
  all age groups across all channels.

The synthetic data assigns digital engagement attributes uniformly,
independent of segment or age. Combined with the other Day 9 findings
(flat revenue by segment, flat utilization-delinquency, risk_score not
predictive of delinquency), this is the fifth consistent observation
that the generator samples demographic and behavioral dimensions
INDEPENDENTLY instead of modeling their natural correlations.

This pattern is a known limitation of simple synthetic generators that
do not implement joint distributions.

**Decision:** the marts compute correctly. The findings are reported
honestly in the Power BI dashboard with explicit narrative, which
demonstrates analytical rigor (the metrics are produced, validated, and
contextualized, rather than presented uncritically).


### Finding: international transfers DO show structured patterns

Contrary to the other Day 9 findings (revenue, DPD,
utilization-delinquency, risk_score, channel preference — all uniformly
distributed), the international transfer mart reveals genuine structure
in the data:

**Bimodal distribution of transfer corridors:**
- Large corridors (~900 tx each): domestic transfers in local currency
  or in USD where the account currency matches. Examples: AR-ARS (956
  tx, 0% intl), CO-USD (940 tx, 1.3% intl).
- Small corridors (~30 tx each): rare exotic transfers, 100% intl.
  Examples: PE-EUR, MX-UYU, CL-MXN.

**International share per country (rolled up):**
- PE: 9.2%, MX: 9.0%, AR: 8.3%, CO: 8.2%, CL: 7.9%, BR: 7.8%, UY: 7.8%
- Relatively consistent across LATAM countries (spread 7.8-9.2%).

**Value asymmetry per country:**
- UY shows the highest per-country international transfer VALUE
  ($131,717 USD with only 9 transactions; ~$14,600 average ticket).
- Other LATAM countries: $30k-$70k totals, $4-5k average ticket.

**Interpretation:**
- The data generator implemented some geographic intent for transfers
  (most are domestic in local currency).
- Uruguay's high-value/low-count corridor is consistent with its real
  role as a regional financial hub.
- The strict definition of international (decisions.md Day 9 §8 —
  requires tx_currency to differ from BOTH account_currency AND
  customer_country_currency) is critical: a more permissive definition
  would have classified all USD transactions from non-USD countries as
  international, hiding the real signal.

This is the first Day 9 mart where the synthetic data shows realistic
structure. It is reportable as a positive finding in the dashboard.

## Intermediate models in silver (pre-Day 10 state)

Before Day 10 there were already two intermediate models in
`dbt/models/intermediate/`:

- `int_loan_portfolio_metrics` — consumed by `mart_loan_composition`,
  `mart_loan_dpd`. Centralizes loan-level derivations (`is_delinquent`,
  `is_default`, `monthly_interest_accrued`, dpd_bucket).
- `int_customer_risk_profile` — consumed by `mart_risk_buckets`,
  `mart_utilization_vs_delinquency`, `mart_delinquency_by_segment`.
  Centralizes customer-level risk derivations (computed risk_bucket,
  observed_delinquency_flag, utilization_bucket lookup).

Day 10 added the third (`int_customer_monthly_revenue`) following the
same pattern — extract duplication between marts that share a grain and
derivation logic. See section "Refactor: int_customer_monthly_revenue"
for the details.

**General criterion:** an `int_*` model is justified when the same
non-trivial logic appears in 2+ marts. If only one mart uses it, it
lives as a CTE inside the mart. If 2+ use it, it moves up to
intermediate.

---

## Refactor: int_customer_monthly_revenue

The Day 10 afternoon block extracted the customer-grain revenue
computation into an intermediate model (`int_customer_monthly_revenue`)
consumed by both `mart_customer_360` and `mart_revenue_by_segment_usd`.
Before the refactor, the same logic lived (duplicated) in both marts
with two cosmetic differences:

1. `mart_revenue_by_segment_usd` did not round per-customer values;
   rounding only happened at the segment-level rollup.
2. `mart_customer_360` rounded per-customer (it is a display mart) and
   used a longer JOIN path for fee attribution
   (`fct_transactions → dim_account → customer_id`) where the other
   mart used `fct_transactions.customer_id` directly.

Both differences were verified to produce **identical customer-level
revenue values** in the current dataset (see
`scripts/diagnose_fee_attribution.sql` and
`scripts/diagnose_revenue_mismatch.sql`). The refactor preserves exact
semantics: 0 / 5000 customers diverge in fee, interest, or total revenue
between the pre-refactor SQL and the intermediate.

### Adopted precision policy

When an intermediate model serves multiple consumers with different
aggregation patterns, precision must be set per-column, not globally:

- **Native currency columns** are rounded to 2 decimals at the customer
  grain. They are consumed by `mart_customer_360` (display, no
  additional aggregation inside the mart).
- **USD columns are NOT rounded** at the customer grain. They are
  consumed by `mart_revenue_by_segment_usd` via `AVG(...)` over ~1,200
  customers per segment. Rounding before the AVG would introduce small
  per-segment deltas (cents propagating to dollars across hundreds of
  customers).

**Lesson:** rounding at the wrong grain is a silent semantic change —
no test fails, but aggregate snapshots drift. The bug only surfaces upon
numeric reconciliation.

### What was centralized

| Logic | Pre-refactor location | Post-refactor location |
|--------|------------------------|-------------------------|
| Fee revenue per customer (native + USD) | Duplicated in both marts | `int_customer_monthly_revenue` |
| Interest revenue per customer (native + USD) | Duplicated in both marts | `int_customer_monthly_revenue` |
| `fees / tenure_months` normalization | Duplicated, with NULLIF + COALESCE | `int_customer_monthly_revenue` |
| Zero-tenure handling (COALESCE to 0) | Duplicated | `int_customer_monthly_revenue` |
| Active loan status filter (current, delinquent) | Duplicated | `int_customer_monthly_revenue` |

`mart_customer_360` and `mart_revenue_by_segment_usd` now only do their
own aggregation logic on top of the intermediate.

---

## Coverage map: business_questions.md (Day 10)

The document `docs/business_questions.md` maps each of the 24 brief
questions to its SQL query against the Gold layer, with real sample
results and a "Key Findings" block at the end highlighting four insights
from the dataset:

1. **`risk_score` is anti-predictive of delinquency** (low: 65.3%,
   critical: 60.4% — non-monotonic, inverted).
2. **Structural USD dollarization pattern** (~50% in all LATAM
   countries).
3. **Flatness in financial dimensions**
   (revenue/delinquency/failed_rate/digital_adoption almost uniform —
   property of the synthetic generator, not a pipeline bug).
4. **DQ honesty:** the pipeline exposes `accounts_with_balance` (Q2),
   `unknown` bucket (Q6) and `accepted_values` test in Q13, instead of
   hiding problems.

**Final coverage:** 24/24 (23 ✅ + 1 ⚠️ Q13 with spec-dataset divergence
documented — the dataset uses `active/frozen/closed` at the account
level while the spec says `active/inactive/suspended/closed`).

Backed by `scripts/validate_business_questions.sql` and
`scripts/validation_output.txt` (reproducible snapshot — see lesson on
staleness below).

---

## Lesson: snapshot staleness vs SQL equivalence (Day 10)

When validating the refactor, the morning snapshot in
`scripts/validation_output.txt` (sme 2890.38, premium 2676.87, retail
2635.99, private_banking 2482.45) did not match the post-refactor output
(sme 2897.17, premium 2666.19, retail 2583.02, private_banking 2534.46).

The instinct was to assume that the refactor had changed semantics.
**It did not.** Two diagnostic queries ruled out semantic divergence:

1. `diagnose_fee_attribution.sql`: 0 of 3,613 `completed` fees have
   `t.customer_id != a.customer_id` (ruling out fee attribution drift).
2. `diagnose_revenue_mismatch.sql`: re-implemented the pre-refactor mart
   logic inline and compared customer-by-customer against the
   intermediate; 0 of 5,000 customers diverge in fee, interest, or
   total.

The snapshot in `validation_output.txt` had gone stale between the
morning run (when the snapshot was captured) and the afternoon's
refactor validation. Some upstream dependency — a Silver rerun, a
Bronze reload, or a seed change — produced new underlying data without
regenerating the snapshot.

**Adopted procedural improvements:**

- **The acid test for refactor equivalence is SQL-level comparison,
  not snapshot comparison.** Re-implementing the pre-refactor logic
  inline and comparing row-by-row against the new intermediate is
  dispositive in a way that snapshot comparisons are not.
- **Validation snapshots must be regenerated when upstream dependencies
  change**, or treated as approximate references rather than exact
  contracts. The sample-result tables in `business_questions.md` for Q1
  were refreshed at the end of Day 10; the Q5 and Q9 outputs were
  verified unchanged between snapshots (no refresh required).
- **The validation runbook should include a snapshot-generation step**
  immediately before starting any refactor work, so the snapshot
  reflects the pre-refactor state of the real repo, not a state from
  earlier in the day.


---

## Power BI integration and downstream DQ findings

### Setup

Power BI Desktop connected to PostgreSQL at localhost:5432 via the native
connector (Import mode, not DirectQuery). Only `gold.mart_*` tables were
imported; bronze and silver were excluded from the `.pbix`.

No relationships were configured in the model between marts. Each mart
is pre-aggregated in dbt with its own grain and contains internally all
the dimensional context. This intentionally reflects a "wide marts"
pattern instead of a star schema, defensible because aggregations live
in dbt (auditable, tested) and not in DAX (opaque, untested).

### Bug found via Power BI integration: `monthly_fee_revenue` explosion

When validating the four base DAX measures in the Page 1 KPI cards,
`Avg Revenue per Customer USD` returned USD 1.03M/month/customer, which
is implausible by more than 4 orders of magnitude.

Diagnosis via SQL on `gold.mart_customer_360`:

* 721 customers (14.4%) have `total_revenue_monthly > $1M`
* maximum = $207,379,782 (`CUST-0003449`, `sme`, `tenure_months=1`)
* The top 10 outliers share `tenure_months ≤ 3` OR have abnormally high
  interest income.

**Root cause #1 — fee revenue division by near-zero values:**
The original `monthly_fee_revenue = total_fees_paid_lifetime / tenure_months`
formula correctly normalized lifetime fees to monthly scale (Day 9 fix),
but failed for customers with 1-3 months of tenure. A customer who paid
$200M in fees during the first month would be projected to $200M/month,
treating the lifetime value as if it were a monthly run-rate. This is a
formula defect, not a source data defect.

**Fix:** clamp the denominator with `GREATEST(tenure_months, 3)`.
Trade-off: it underestimates revenue for genuinely young and active
customers, but it stabilizes the metric for the 38+71+74 = 183 customers
(3.7%) with `tenure ≤ 2`. Applied in `mart_customer_360.sql` and
`mart_revenue_by_segment_usd.sql`.

**Root cause #2 — source data: extreme outliers in `outstanding_balance`:**
Independent of the fee logic, `monthly_interest_income` showed customers
with $20-37M/month interest accrual. Investigation:

* `interest_rate_decimal`: validated, range [0.0301, 0.35], median 19.4%.
  Day 9 fix is correct.
* `outstanding_balance` in `silver.fct_loans`: maximum $1,841,582,518.
  1,227 loans > $1M, 452 loans > $100M, all in COP currency for
  retail/SME customers labeled as auto loans.
* Example: `CUST-0001189` has an auto loan in COP for $1,285M. The math
  is correct:
  ($1.285B × 0.3469 / 12 = $37.16M COP interest/month).

This is a property of the **synthetic dataset**, not a pipeline bug.
The generator created retail loans with corporate-finance scale balances,
inconsistent with the assigned segments. This adds to the growing list
of already-documented generator artifacts (flat 50% DPD distribution,
`risk_score` not correlated with delinquency, `customer_segment` not
correlated with revenue, etc.).

**Action:** instrumented as a `warn` test:

```yaml
- name: outstanding_balance
  tests:
    - dbt_utils.expression_is_true:
        expression: "< 100000000 or outstanding_balance is null"
        config:
          severity: warn
```

`dbt test` now produces a visible warning on every run, making this DQ
issue self-documenting in the build output.

Additionally, `mart_customer_360.total_revenue_monthly` received a
range `warn` test (`>= 0 and < 1M`). With the Day 11 fix, this is
expected to produce warnings about ~30-50 customers (down from 721),
all attributable to interest income from the corporate loans mentioned
above. This confirms that the fee bug is fully closed and that the
residual belongs to the source data, not the formula.

### Dashboard implication: median over average

Given the residual long-tail caused by the interest income outliers,
the Executive Overview KPI was changed from
`AVG(total_revenue_monthly)` to `MEDIAN`.

The median is the correct statistical measure of central tendency for
any right-skewed financial distribution (always true for per-customer
revenue in real banking) and is robust to the residual synthetic
outliers.

New DAX:

```DAX
Median Revenue per Customer USD =
MEDIANX(mart_customer_360, mart_customer_360[total_revenue_monthly])
```

Documented in the dashboard via a tooltip over the KPI card.

### Dashboard currency strategy

[fill in with what you decide: USD only / global slicer / multi-legend]

### Process lesson

The bug had remained latent since Day 9. dbt tests did not detect it
because:

* `expression_is_true: ">= 0"` passes for $206M (it is effectively
  `>= 0`).
* No magnitude / range / outlier tests existed on revenue metrics.

The bug only emerged when a downstream BI consumer rendered the value in
a human-readable card. **Lesson:** numeric metrics need magnitude tests,
not just sign tests, especially when they derive from divisions or
accruals. The new `warn` test on `total_revenue_monthly` is the
generalization of this fix.

### Post-fix diagnostic of residual outliers

After applying the tenure clamp fix, 722 customers (vs. 721 before) still
showed `total_revenue_monthly > $1M`. The almost identical count
suggested the fix had a different effect than naively expected: instead
of reducing the number of outliers, it reduced the **magnitude** of
individual blowups (`CUST-0003449` went from $206M to $68.9M in
fee_revenue) without necessarily removing them from the `>$1M` bucket.

Decomposition of the 722 outliers:

* 369 (51%) driven only by `interest_income`
  (average tenure 36.7 months, unaffected by the clamp).
  Root cause: ~693 source loans with `outstanding_balance > $100M`.
* 265 (37%) driven only by `fee_revenue`
  (average tenure 27.3 months, outside the clamp's effective window).
  Root cause: source customers with corporate-scale
  `total_fees_paid_lifetime` (e.g.: `CUST-0000347`: $348M lifetime fees
  over 27 months of tenure).
* 73 (10%) with both components in the millions.
* 15 (2%) below $1M individually but above when summed.

The synthetic dataset thus contains two parallel "scale escapes" (loans
+ fees), both inconsistent with the affected customers' implied
retail/SME segments. Both were documented as `warn` tests in dbt, making
them visible on every build without blocking the pipeline.

Conclusion: the Day 11 formula fix is complete. The residual outliers
belong to the **source data, not the pipeline behavior**. Therefore, the
median (not the mean) is the statistically correct KPI for the
dashboard, and the `warn` tests serve as continuous evidence of the
source dataset's limitations.

### New marts created during dashboard integration

The layout of Page 2 in Power BI exposed two gaps in the existing Gold
layer.

**Gap 1 — Top merchants ranking (Q22):**
`mart_top_merchants` was added with grain (`merchant × currency`).

It aggregates only completed transactions and ranks within each currency
to avoid mixing scales across LATAM currencies. Includes a `top_category`
column computed via window function with deterministic tiebreaker
(alphabetical), so each merchant is enriched with its most frequent
category as qualitative context.

The dashboard's Top-10 table filters by
`rank_by_value_within_currency <= 10` and connects to the global
currency slicer.

The column was called `merchant` (not `merchant_name` as originally
assumed). The first attempt failed `dbt run` with "column does not
exist"; verified via `information_schema.columns` and corrected in an
iteration.

**Gap 2 — Monthly revenue time series (Q2):**
`mart_revenue_by_segment_usd` has a snapshot grain (1 row per segment)
and cannot feed a temporal line chart.
`mart_revenue_monthly_by_segment_usd` was added with grain
(`revenue_month × customer_segment`) to feed the dashboard's main chart.

Two design decisions in this mart:

1. **Current-month cutoff:**
   The current calendar month is excluded via:

   ```sql
   WHERE transaction_date < date_trunc('month', current_date)
   ```

   Otherwise, a partial month would render as an abrupt drop at the end
   of the line chart, misleadingly suggesting a trend change.

2. **Interest accrual simplification:**
   `silver.fct_loans` has no `loan_schedule` table; only a snapshot
   exists with `outstanding_balance` and `start_date`.

   Monthly interest is distributed as a flat accrual:
   `(balance × rate / 12)`,
   assigned identically to each month from `start_date` onward, capped
   by the cutoff.

   Implications:

   * Old months may be slightly overestimated (historically closed
     loans still appear "active" because there is no close date in the
     source).
   * Interest revenue grows roughly linearly, while fee revenue grows
     exponentially in the data. This is consistent with the source and
     produces an interpretable shift in the fee/interest ratio over
     time (Jun 2025: 27/73; Apr 2026: 65/35), interpretable as a
     transition from a balance-sheet-revenue-dominated model to a
     transaction-revenue-dominated one.

   In production, a `loan_schedule` table would replace this
   simplification. Documented as a known limitation.

**Observed temporal range in source:**
September 2020 → May 2026.
After cutoff: September 2020 → April 2026
(61 months × 4 segments = 244 rows).

Both marts received standard schema tests:

* `not_null` on grain keys
* `expression_is_true >= 0` on monetary columns
* `accepted_values` where applicable.


## Reproducibility test findings

The clean-clone reproducibility test surfaced five infrastructure bugs
invisible during normal development (because local state had been built
up incrementally across the prior days). All five are now fixed and the
dry-run completes end-to-end on both Windows/amd64 and macOS/arm64.

### Bug 1 — `silver_raw` schema missing from `init_db.sql`

Spark writes its flattened outputs to `silver_raw.stg_*`, but the
bootstrap script only created `bronze`, `silver`, and `gold`. The
`silver_raw` schema had been created manually on Day 1 and had never
been added to the init. After `docker compose down -v` the volume was
clean and the schema did not exist, which made the three Spark flatten
tasks fail with "schema does not exist".

**Fix:** added the line
`CREATE SCHEMA IF NOT EXISTS silver_raw AUTHORIZATION qversity;` in
`init_db.sql`.

### Bug 2 — DAG running `dbt --select` with a stale MVP selector

The `dbt_run_mvp` and `dbt_test_mvp` tasks still carried the
`--select silver.dim_customer gold.customer_summary` flag from the
Day 5 scaffolding. Two consequences: only `dim_customer` was built
(which made 14 `relationships` tests fail from a clean state because
the relations they pointed to did not exist), and `gold.customer_summary`
no longer exists (it was refactored to `silver.agg_customer_activity`
on Day 6).

**Fix:** removed the selector from both commands and renamed the tasks
to `dbt_run` / `dbt_test`. They now run the full project.

### Bug 3 — `dbt seed` never invoked by the DAG

Several models (`int_customer_monthly_revenue`,
`mart_revenue_by_segment_usd`, `mart_revenue_monthly_by_segment_usd`,
`mart_international_transfers`) join against seed tables (`fx_rates`,
`country_currency`) that live as CSVs in `dbt/seeds/`. dbt does NOT load
seeds as part of `dbt run` — they require an explicit `dbt seed`
invocation. The seeds had been loaded manually during early development
and the step had never been added to the DAG.

**Fix:** added a `dbt_seed` task between the Spark group and `dbt_run`.

### Bug 4 — `JAVA_HOME` hardcoded to the amd64 path

Both the Dockerfile and the DAG hardcoded
`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`. On Apple Silicon
(arm64), openjdk installs at `/usr/lib/jvm/java-17-openjdk-arm64`, so
`spark-submit` could not find the Java binary on M-series Macs. The
three flatten tasks were failing with "java: No such file or directory".

**Fix:**
- The Dockerfile now detects the real JVM path at build time via
  `readlink` on the `java` binary, and creates a stable symlink at
  `/usr/lib/jvm/default-java`. `JAVA_HOME` points to that symlink and
  works on both architectures without conditional logic.
- The DAG's `SPARK_ENV` no longer sets `JAVA_HOME`, so the container
  value (correct for the actual architecture) propagates via
  `append_env=True`.

Reported by teammates running on Apple Silicon during the Day 13
dry-run.

### Bug 5 — `dbt deps` never invoked by the DAG

The dbt project depends on `dbt_utils` (declared in `dbt/packages.yml`,
used by many tests including `relationships`, `accepted_values`, and
`expression_is_true`). dbt does not install packages automatically —
they require `dbt deps`. The `dbt_packages/` directory had been kept
locally between runs because the directory is bind-mounted, and it was
never explicitly recreated. In a clean clone the directory does not
exist and dbt aborts compilation with:

> `dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed in dbt_packages.`

**Fix:** added a `dbt_deps` task before `dbt_seed`. Also added
`dbt/dbt_packages/` to `.gitignore` to keep it as a build artifact
instead of committed code.

### Conclusion

All five were classic "works on my machine" bugs — invisible until the
clean clone. Their detection validates the value of the reproducibility
test itself. The pipeline is now genuinely reproducible across
platforms.