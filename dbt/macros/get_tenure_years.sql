{% macro get_tenure_years(registration_date_column) %}
    ROUND(
        CAST(
            (CURRENT_DATE - {{ registration_date_column }}) / 365.25 AS NUMERIC
        ),
        2
    )
{% endmacro %}