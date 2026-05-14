{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['customer_id']},
            {'columns': ['start_date']},
            {'columns': ['status']}
        ]
    )
}}

-- =============================================================================
-- fct_loans
-- =============================================================================
-- Grano: una fila por loan.
--
-- Promoción semántica de stg_loans a fact table:
--   - Materializado como `table` (no view) por consistencia con fct_transactions
--     y patrón Kimball estándar. Volumen bajo (~7.6k filas).
--   - FK hacia dim_customer.customer_id. dim_loan ya existe como dimensión
--     descriptiva del loan en sí (loan_id, type, term, etc.), separada de las
--     métricas financieras (principal, outstanding_balance, interest_rate)
--     que viven acá.
--   - Indexes en customer_id (joins con dim_customer en gold), start_date
--     (filtros temporales) y status (filtros por current/delinquent/default/paid_off).
--
-- Decisiones diferidas a gold (intencional):
--   - interest_income_estimated: requiere definir si es principal*interest_rate
--     (anual nominal) o cálculo de amortización sobre outstanding_balance.
--     Diferido para iterar la definición sin tocar silver. (BQ 4).
--   - is_delinquent: requiere definir si es status='delinquent' estricto o
--     incluye days_past_due > 0. Diferido. (BQ 5, 8).
--   - delinquency_bucket por days_past_due: bucketing es lógica de negocio,
--     vive en gold.
-- =============================================================================

WITH src AS (
    SELECT
        loan_id,
        customer_id,
        loan_type,
        currency,
        principal,
        outstanding_balance,
        interest_rate,
        term_months,
        monthly_payment,
        start_date,
        end_date,
        status,
        days_past_due,
        collateral_type,
        bronze_id,
        load_timestamp
    FROM {{ ref('stg_loans') }}
)

SELECT
    -- Primary key
    loan_id,

    -- Foreign keys (testeados en schema yml)
    customer_id,
    start_date,
    end_date,

    -- Loan descriptors
    loan_type,
    currency,
    collateral_type,
    term_months,

    -- Measures (financial)
    principal,
    outstanding_balance,
    interest_rate,
    monthly_payment,

    -- State
    status,
    days_past_due,

    -- Lineage / auditoría
    bronze_id,
    load_timestamp
FROM src