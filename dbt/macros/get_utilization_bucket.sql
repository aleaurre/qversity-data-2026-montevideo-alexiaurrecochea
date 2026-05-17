{% macro get_utilization_bucket(utilization_column) %}
    CASE
        WHEN {{ utilization_column }} IS NULL THEN 'unknown'
        WHEN {{ utilization_column }} < 30 THEN 'healthy'
        WHEN {{ utilization_column }} < 60 THEN 'moderate'
        WHEN {{ utilization_column }} < 90 THEN 'high'
        WHEN {{ utilization_column }} <= 100 THEN 'maxed'
        ELSE 'over_limit'
    END
{% endmacro %}