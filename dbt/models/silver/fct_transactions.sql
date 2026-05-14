{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['customer_id']},
            {'columns': ['account_id']},
            {'columns': ['transaction_date']}
        ]
    )
}}

-- =============================================================================
-- fct_transactions
-- =============================================================================
-- Grano: una fila por transacción.
--
-- Promoción semántica de stg_transactions a fact table:
--   - Materializado como `table` (no view) para performance de PowerBI sobre 87k filas.
--   - FKs hacia dim_customer.customer_id y dim_account.account_id, testeadas en
--     _silver__fct.yml con `relationships` severity=error (0 orphans confirmados
--     en silver el día 7 antes de crear este modelo).
--   - Indexes en customer_id, account_id, transaction_date para acelerar joins
--     y filtros temporales en gold y PowerBI.
--
-- Derivaciones agregadas (mínimas y sintácticas):
--   - is_failed: status = 'failed'. NO incluye 'pending' (incompleta) ni 'reversed'
--     (revertida pero originalmente exitosa). Definición pensada para BQ 18
--     (failed transaction rate by channel).
--   - day_of_week: número 0-6 (domingo=0) extraído de transaction_date.
--     Útil para BQ 16 (transaction volume by day of week). Se deja como int
--     para flexibilidad; el label legible (Mon/Tue/...) se deriva en gold o
--     en el dashboard.
--
-- Lo que NO incluye (consciente):
--   - amount_signed: requiere cerrar definición de revenue/net_flow. Diferido a gold.
--   - is_revenue_event: ídem.
-- =============================================================================

WITH src AS (
    SELECT
        transaction_id,
        customer_id,
        account_id,
        transaction_date,
        amount,
        currency,
        transaction_type,
        category,
        merchant,
        channel,
        status,
        description,
        bronze_id,
        load_timestamp
    FROM {{ ref('stg_transactions') }}
)

SELECT
    -- Primary key
    transaction_id,

    -- Foreign keys (testeados en schema yml)
    customer_id,
    account_id,
    transaction_date,

    -- Measures
    amount,
    currency,

    -- Attributes
    transaction_type,
    category,
    merchant,
    channel,
    status,
    description,

    -- Derived (sintácticas, no de negocio)
    (status = 'failed')                    AS is_failed,
    EXTRACT(DOW FROM transaction_date)::int AS day_of_week,

    -- Lineage / auditoría
    bronze_id,
    load_timestamp
FROM src