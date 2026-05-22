-- Creamos los schemas Bronze/Silver/Gold desde el arranque del contenedor.
-- Esto evita tener que hacerlo manual o desde Airflow después.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver_raw AUTHORIZATION qversity;
CREATE SCHEMA IF NOT EXISTS gold;

-- Schema separado para Airflow metadata (mejor práctica que mezclar con warehouse)
-- Si preferís usar la misma DB, Airflow va a crear sus tablas en public por default.
COMMENT ON SCHEMA bronze IS 'Raw ingestion layer — JSONB';
COMMENT ON SCHEMA silver IS 'Cleaned and flattened — staging + normalized';
COMMENT ON SCHEMA gold IS 'Analytics-ready models for BI';
