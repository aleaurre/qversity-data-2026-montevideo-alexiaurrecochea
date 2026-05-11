{#
    Override of dbt's default `generate_schema_name`.

    Default behavior (when a model has +schema: bronze):
        target_schema = "public" (from profiles.yml)
        custom_schema = "bronze" (from dbt_project.yml +schema config)
        -> dbt writes to "public_bronze"

    What we want for this project:
        -> dbt writes to "bronze" (no prefix)

    Reason: Spark already writes to `silver.stg_accounts`, `silver.stg_transactions`,
    `silver.stg_loans` directly. If dbt prefixed schemas as `public_silver`, dbt
    sources couldn't reference the Spark-produced tables without manual schema
    overrides on every source. Cleaner to use unprefixed schemas everywhere.

    Rule implemented here:
        - If a model has a custom_schema_name set (+schema), use it as-is.
        - Otherwise, fall back to the target schema from profiles.yml.

    Reference: https://docs.getdbt.com/docs/build/custom-schemas
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}