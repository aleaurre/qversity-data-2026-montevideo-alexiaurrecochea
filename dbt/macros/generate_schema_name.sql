{#
    Override of dbt's default `generate_schema_name`.

    UNCHANGED from the Postgres version — this is pure Jinja, adapter-agnostic.
    It works identically on dbt-databricks.

    Behavior: if a model sets +schema (e.g. silver / gold), use it as-is
    (no `<target_schema>_` prefix). Otherwise fall back to target.schema.

    Unity Catalog note: this macro resolves the SCHEMA only. The CATALOG comes
    from profiles.yml (`catalog: qversity`). So a gold model resolves to
    `qversity.gold.<model>` — same medallion layout as Postgres, now as a
    3-level UC name. No generate_database_name override is needed because the
    whole project lives in a single catalog.

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
