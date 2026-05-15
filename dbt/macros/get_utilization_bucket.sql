{% macro get_utilization_bucket(utilization_column) %}
    CASE
        WHEN {{ utilization_column }} < 30 THEN 'healthy'
        WHEN {{ utilization_column }} BETWEEN 30 AND 70 THEN 'moderate'
        WHEN {{ utilization_column }} > 70 THEN 'high'
        ELSE 'unknown'
    END
{% endmacro %}