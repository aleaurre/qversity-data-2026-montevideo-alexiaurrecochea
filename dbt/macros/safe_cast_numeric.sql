{#
    Safely cast a jsonb-extracted text value to a numeric type, handling
    multiple categories of data quality issues in this dataset:

    1. Missing markers used by the generator instead of JSON null:
       '', 'NA', 'N/A', 'null', 'NULL'
    2. Non-parseable numeric strings discovered in EDA:
       - '$78.26' (dollar prefix)
       - '89.5 USD' (currency suffix)
       - '83,5' (comma decimal — European format)

    Strategy:
      - NULL missing markers explicitly.
      - For everything else, return NULL if the string doesn't match a
        clean numeric pattern. The flag column in the consuming model
        (e.g., is_<field>_valid) tracks which rows were unparseable so
        downstream queries can filter or aggregate them out.

    Why not regex-clean the dirty values:
    EDA confirmed the dirty values are <0.4% of the dataset across the
    affected fields. The cost of writing and maintaining cleaning regex
    is not justified by the recoverable volume. We prefer to surface
    the issue via the flag pattern, which makes the data quality
    visible in downstream models.

    Args:
      column_expr:  the raw text expression (e.g., "data ->> 'foo'")
      target_type:  Postgres type ('int', 'numeric', 'numeric(18,2)', etc.)

    Usage:
      {{ safe_cast_numeric("data -> 'credit_info' ->> 'utilization_pct'", 'numeric') }} as utilization_pct
#}

{% macro safe_cast_numeric(column_expr, target_type) %}
    case
        when ({{ column_expr }}) is null then null
        when ({{ column_expr }}) in ('', 'NA', 'N/A', 'null', 'NULL') then null
        -- Only attempt cast if the string matches a clean numeric pattern.
        -- Anything else (dollar signs, currency suffixes, comma decimals)
        -- returns NULL, with the data quality issue tracked via flag columns.
        when ({{ column_expr }}) ~ '^-?[0-9]+\.?[0-9]*$' then ({{ column_expr }})::{{ target_type }}
        else null
    end
{% endmacro %}