# Modelo de datos — referencia técnica

Inventario de tablas del pipeline `qversity` con grain, PK/FK, columnas clave
y consumidores downstream. Complementa los diagramas visuales (`silver_er.md`
y `pipeline_lineage.md`) con un formato searchable.

---

## Capa Bronze (PostgreSQL — schema: `bronze`)

### `raw_fintech_data`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por record source × load |
| Materialización | append-only, particionada por `load_id` |
| PK lógica | `(load_id, customer_id)` extraído del `jsonb` |
| Origen | S3 `fintech_banking_dataset.json`, ingestado por Airflow DAG |
| Columnas | `data jsonb`, `load_id`, `load_timestamp` |
| Consumidores | PySpark (flatten arrays), dbt (extract flat + nested objects) |

---

## Capa Silver raw (PostgreSQL — schema: `silver_raw`)

Outputs crudos de PySpark. dbt los lee como `source` y los promociona a la
capa `silver` aplicando normalización categórica y casteos defensivos.

### `stg_accounts`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por account (post-dedup) |
| Volumen | ~17.000 |
| PK | `account_id` |
| FK | `customer_id` |
| Dedup en | `account_id` ORDER BY `load_timestamp` DESC |
| Productor | `spark/flatten_accounts.py` |

### `stg_transactions`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por transaction (post-dedup) |
| Volumen | ~89.000 |
| PK | `transaction_id` |
| FK | `customer_id`, `account_id` |
| Dedup en | `transaction_id` ORDER BY `load_timestamp` DESC |
| Productor | `spark/flatten_transactions.py` |

### `stg_loans`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por loan (post-dedup) |
| Volumen | ~7.600 |
| PK | `loan_id` |
| FK | `customer_id` |
| Dedup en | `loan_id` ORDER BY `load_timestamp` DESC |
| Productor | `spark/flatten_loans.py` |

---

## Capa Silver (PostgreSQL — schema: `silver`)

### Dimensiones

#### `dim_customer`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por customer |
| Volumen | 5.000 |
| PK | `customer_id` |
| FK | `country` → `dim_geography.country` |
| Materialización | table |
| Producido por | dbt directamente desde `bronze.raw_fintech_data` (jsonb ops + dedup por `customer_id` + `load_timestamp` DESC) |
| Columnas clave | `customer_segment`, `kyc_status`, `status`, `risk_score`, `country`, `age`, `age_bucket`, `tenure_months`, `tenure_bucket`, `lat`, `lon`, `is_geo_valid` |
| Normalizaciones aplicadas | `normalize_customer_status`, `normalize_customer_segment`, `normalize_kyc_status` |
| Validaciones críticas | `accepted_values` en status/segment/kyc/country; coordenadas validadas vía `is_geo_valid`; `nationality = country` (defensive) |
| Consumidores | `dim_account` (FK target), `dim_loan` (FK target), `dim_credit_info` (FK target), `dim_digital_engagement` (FK target), `fct_transactions` (FK target), `fct_loans` (FK target), `agg_customer_activity`, 9 marts Gold |

#### `dim_account`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por account |
| PK | `account_id` |
| FK | `customer_id` → `dim_customer` |
| Materialización | table |
| Producido por | promoción de `stg_accounts` con normalización + derivación de `account_age_months` |
| Columnas clave | `account_type` (savings/checking/investment/credit_card), `status` (active/frozen/closed), `currency`, `opened_date`, `balance` |
| Divergencia con spec | `account.status` = `active/frozen/closed`. La spec listaba `active/inactive/suspended/closed` |
| Consumidores | `fct_transactions` (FK target), `agg_customer_activity`, `mart_account_mix`, `mart_international_transfers` |

#### `dim_loan`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por loan |
| PK | `loan_id` |
| FK | `customer_id` → `dim_customer` |
| Materialización | table |
| Producido por | promoción de `stg_loans` con normalización + derivación de `loan_age_months`, `remaining_term_months`, `principal_repaid` |
| Columnas clave | `loan_type` (personal/mortgage/auto/education/business), `status` (current/delinquent/default/paid_off), `principal`, `outstanding_balance`, `interest_rate`, `interest_rate_decimal`, `days_past_due` |
| Consumidores | `fct_loans` (FK target), `agg_customer_activity` |

#### `dim_credit_info`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por customer |
| PK | `customer_id` |
| FK | `customer_id` → `dim_customer` |
| Materialización | table |
| Producido por | dbt jsonb ops desde el objeto nested `credit_info` del Bronze |
| Patrón aplicado | `raw + flag + validated` para `credit_score` y `utilization_pct` (10% del credit_score corrupto fuera de [300,850]) |
| Columnas clave | `credit_score` (validated), `is_credit_score_valid`, `utilization_pct`, `is_utilization_pct_valid`, `total_limit`, `total_used`, `late_payments_12m`, `bankruptcy_flag` |
| Consumidores | `mart_customer_360`, `int_customer_risk_profile` |

#### `dim_digital_engagement`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por customer |
| PK | `customer_id` |
| FK | `customer_id` → `dim_customer` |
| Materialización | table |
| Producido por | dbt jsonb ops desde el objeto nested `digital_engagement` |
| Booleans | normalizados vía `safe_cast_boolean` (~5% de variantes no-estándar: `yes`/`no`, `si`, `0`/`1`) |
| Columnas clave | `mobile_app_registered`, `web_banking_registered`, `preferred_channel` (mobile/atm/branch/phone/web), `avg_monthly_logins`, `push_notifications`, `paperless_statements` |
| Observación crítica | `preferred_channel` contiene `phone` mientras `fct_transactions.channel` contiene `pos` — son dimensiones semánticamente distintas, NO unificar |
| Consumidores | `mart_customer_360`, `mart_digital_adoption_by_segment`, `mart_channel_preference_by_age` |

#### `dim_geography`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por country |
| Volumen | 7 (LATAM) |
| PK | `country` |
| Materialización | table |
| Producido por | dbt VALUES literal (hardcoded — no derivado) |
| Justificación | typos en `dim_customer.city` (ej. `Lma` por `Lima`) excluyen ciudad de esta dimensión |
| Consumidores | join opcional desde `dim_customer` |

#### `dim_date`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por día |
| Volumen | ~5.844 (2015-01-01 a 2030-12-31) |
| PK | `date_day` |
| Materialización | table |
| Columnas clave | `year`, `quarter`, `month`, `day_of_week_num` (0=Sun, 6=Sat), `is_weekend`, `year_month_key` (YYYYMM sortable) |
| Consumidores | join lógico desde marts time-based (Power BI) |

### Facts

#### `fct_transactions`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por transaction |
| PK | `transaction_id` |
| FK | `customer_id` → `dim_customer`, `account_id` → `dim_account` |
| Materialización | table |
| Producido por | promoción de `stg_transactions` con derivaciones sintácticas (`is_failed`, `day_of_week`) |
| Columnas clave | `transaction_date`, `amount`, `currency`, `transaction_type` (deposit/withdrawal/transfer/payment/refund/fee), `category`, `channel` (mobile/web/atm/branch/pos), `status` (completed/pending/failed/reversed) |
| Tests | 0 orphans confirmados en customer_id y account_id |
| Consumidores | `agg_customer_activity`, `int_customer_monthly_revenue`, 5 marts Gold de transactions |

#### `fct_loans`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por loan (mismo grain que `dim_loan`) |
| PK | `loan_id` |
| FK | `customer_id` → `dim_customer`, `loan_id` ↔ `dim_loan` |
| Materialización | table |
| Justificación de fact separado | analíticas de status-changing (DPD, delinquency, default); `dim_loan` provee atributos estáticos |
| Consumidores | `int_loan_portfolio_metrics`, `int_customer_monthly_revenue` |

### Aggregates

#### `agg_customer_activity`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por customer |
| PK | `customer_id` |
| FK | `customer_id` → `dim_customer` |
| Materialización | table |
| Producido por | LEFT JOIN de dim_customer + counts(account/loans/transactions) |
| Columnas | `accounts_count`, `transactions_count`, `loans_count`, `total_products` (= accounts + loans) |
| Criterio "agg en silver" | solo cuenta atributos estructurales (cardinalidad); no aplica reglas de negocio. Si las aplicara, viviría en Gold |
| Consumidores | `mart_customer_360` (Q24) |

---

## Capa Intermediate (PostgreSQL — schema: `silver`, materialización: `view`)

Lógica de negocio compartida entre marts Gold. Centraliza derivaciones para
que cada mart aplique solo su agregación específica.

### `int_loan_portfolio_metrics`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por loan |
| Lee de | `fct_loans` |
| Agrega | `is_delinquent` (DPD>=30), `is_default` (DPD>=90 OR status='default'), `dpd_bucket` (6 buckets IFRS9), `monthly_interest_accrued` |
| Consumidores | `mart_loan_dpd`, `mart_loan_composition` |

### `int_customer_risk_profile`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por customer |
| Lee de | `dim_customer`, `dim_credit_info`, `int_loan_portfolio_metrics` |
| Agrega | `risk_bucket`, `credit_score_bucket`, `utilization_bucket`, `is_delinquent_customer` (any loan delinquent), `is_default_customer`, `loans_count` |
| Consumidores | `mart_risk_buckets`, `mart_utilization_vs_delinquency`, `mart_delinquency_by_segment`, `mart_credit_score_by_country` |

### `int_customer_monthly_revenue` *(creado en Día 10)*

| Atributo | Valor |
|---|---|
| Grain | 1 fila por customer |
| Lee de | `fct_transactions`, `fct_loans`, `dim_customer`, `fx_rates` |
| Agrega | `monthly_fee_revenue_native` + `_usd`, `monthly_interest_revenue_native` + `_usd`, `total_monthly_revenue_native` + `_usd` |
| Política de precisión | native rounded 2 decimales (display grain); USD sin redondear (downstream AVG sobre 1.200 customers/segment) |
| Consumidores | `mart_customer_360`, `mart_revenue_by_segment_usd` |

---

## Seeds (dbt — versionados en repo)

### `fx_rates.csv`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por currency |
| PK | `currency` |
| Volumen | 9 (USD, ARS, BRL, CLP, COP, MXN, PEN, UYU, EUR) |
| Columnas | `currency`, `rate_to_usd`, `as_of_date` |
| Fuente | mid-market snapshot Xe.com (mayo 2026) |
| Consumidores | `int_customer_monthly_revenue`, 5 marts Gold (relationship tests) |

### `country_currency.csv`

| Atributo | Valor |
|---|---|
| Grain | 1 fila por country |
| PK | `country` |
| Columnas | `country`, `currency_local` |
| Consumidores | `mart_international_transfers` (lookup para detectar transfers cross-currency) |

---

## Capa Gold (PostgreSQL — schema: `gold` — 16 marts)

Cada mart cubre una o más business questions del set Q1-Q24. Materialización:
`table`. Cobertura final: **24/24** (23 ✅ + 1 ⚠️ Q13 con divergencia spec).

### Acquisition & Demographics

| Mart | Grain | Preguntas | Lee de |
|---|---|---|---|
| `mart_acquisition_trend` | mes | Q12 | `dim_customer` |
| `mart_customer_360` | customer | Q1, Q9, Q10, Q11, Q13, Q14, Q24 | `dim_customer`, `dim_credit_info`, `dim_digital_engagement`, `agg_customer_activity`, `int_customer_monthly_revenue` |

### Revenue

| Mart | Grain | Preguntas | Lee de |
|---|---|---|---|
| `mart_revenue_by_segment_usd` | customer_segment | Q1 (USD) | `int_customer_monthly_revenue`, `dim_customer` |
| `mart_account_mix` | country × account_type × currency | Q2, Q22 | `dim_account`, `dim_customer` |

### Risk & Credit

| Mart | Grain | Preguntas | Lee de |
|---|---|---|---|
| `mart_delinquency_by_segment` | customer_segment | Q5 | `int_customer_risk_profile`, `dim_customer` |
| `mart_credit_score_by_country` | country × credit_score_bucket | Q6 | `int_customer_risk_profile`, `dim_customer` |
| `mart_utilization_vs_delinquency` | utilization_bucket | Q7 | `int_customer_risk_profile` |
| `mart_risk_buckets` | risk_bucket | Q9 | `int_customer_risk_profile` |
| `mart_loan_dpd` | loan_type × dpd_bucket × currency | Q8 | `int_loan_portfolio_metrics` |
| `mart_loan_composition` | loan_type × status × currency | Q4, Q23 | `int_loan_portfolio_metrics` |

### Transaction Patterns

| Mart | Grain | Preguntas | Lee de |
|---|---|---|---|
| `mart_tx_by_channel` | channel × currency | Q3, Q17, Q18 | `fct_transactions` |
| `mart_tx_by_category` | category × currency | Q15 | `fct_transactions` |
| `mart_tx_by_dow` | day_of_week × currency | Q16 | `fct_transactions` |
| `mart_international_transfers` | origin_country × tx_currency | Q19 | `fct_transactions`, `dim_account`, `dim_customer`, `country_currency` |

### Digital Engagement

| Mart | Grain | Preguntas | Lee de |
|---|---|---|---|
| `mart_digital_adoption_by_segment` | customer_segment | Q20 | `dim_customer`, `dim_digital_engagement` |
| `mart_channel_preference_by_age` | age_bucket × preferred_channel | Q21 | `dim_customer`, `dim_digital_engagement` |

---

## Resumen de fan-out por entidad

Para entender qué tabla es "más central" al modelo, el siguiente conteo
muestra cuántos modelos downstream consumen cada entidad de Silver:

| Tabla Silver | Consumidores downstream |
|---|---|
| `dim_customer` | 9 marts + 3 intermediates + 4 dims FK (account/loan/credit/digital) + 2 facts (tx/loans) |
| `fct_transactions` | 5 marts + 2 intermediates + 1 aggregate |
| `dim_account` | 3 marts + 1 fact (tx FK) + 1 aggregate |
| `dim_credit_info` | 1 mart + 1 intermediate |
| `dim_digital_engagement` | 3 marts |
| `dim_loan` | 1 aggregate + 1 fact (loans, shared PK) |
| `fct_loans` | 2 intermediates |
| `agg_customer_activity` | 1 mart |

**Insight:** `dim_customer` es el hub (lo confirma la hipótesis del Día 1
sobre la jerarquía: customer es el root, todo lo demás cascadea). Esto
también justifica que la dedup customer-level cascada cleanup a los
arrays vía INNER JOIN.

---

## Tests dbt en producción

Inventario de tests al cierre del Día 10:

| Capa | Modelos | Tests |
|---|---|---|
| Bronze | 1 source | sources solo, sin tests dbt |
| Silver | 11 modelos | ~150 tests (unique, not_null, accepted_values, relationships, expression_is_true) |
| Intermediate | 3 modelos | ~25 tests |
| Gold | 16 marts | 196 tests |
| **Total** | **31 modelos** | **~370 tests** |

Todos los tests passing al cierre del Día 10.
