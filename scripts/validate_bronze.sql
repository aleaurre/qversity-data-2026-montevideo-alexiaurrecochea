-- scripts/validate_bronze.sql
-- Manual validation queries for the Bronze layer.
-- Usage:
--   docker exec -i qversity_postgres psql -U qversity -d qversity_warehouse < scripts/validate_bronze.sql

\echo '== 1) Total records in bronze.raw_fintech_data =='
SELECT COUNT(*) AS total FROM bronze.raw_fintech_data;

\echo ''
\echo '== 2) Distinct load_ids (one per DAG run) =='
SELECT load_id, COUNT(*) AS records, MAX(load_timestamp) AS loaded_at
FROM bronze.raw_fintech_data
GROUP BY load_id
ORDER BY loaded_at DESC;

\echo ''
\echo '== 3) Latest batch size (must be 5100, regardless of total runs) =='
SELECT COUNT(*) AS latest_batch_size
FROM bronze.raw_fintech_data
WHERE load_id = (
    SELECT load_id FROM bronze.raw_fintech_data
    ORDER BY load_timestamp DESC LIMIT 1
);

\echo ''
\echo '== 4) Sample of customer records from latest batch =='
SELECT id, data->>'customer_id' AS customer_id, data->>'country' AS country
FROM bronze.raw_fintech_data
WHERE load_id = (
    SELECT load_id FROM bronze.raw_fintech_data
    ORDER BY load_timestamp DESC LIMIT 1
)
LIMIT 5;