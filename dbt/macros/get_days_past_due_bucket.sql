{% macro get_days_past_due_bucket(dpd_column) %}
    CASE
        WHEN {{ dpd_column }} = 0 THEN 'current'
        WHEN {{ dpd_column }} BETWEEN 1 AND 30 THEN '1-30'
        WHEN {{ dpd_column }} BETWEEN 31 AND 60 THEN '31-60'
        WHEN {{ dpd_column }} BETWEEN 61 AND 90 THEN '61-90'
        WHEN {{ dpd_column }} > 90 THEN '90+'
        ELSE 'unknown'
    END
{% endmacro %}