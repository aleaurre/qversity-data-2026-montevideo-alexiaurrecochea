{% macro get_risk_bucket(risk_score_column) %}
    CASE
        WHEN {{ risk_score_column }} BETWEEN 0 AND 30 THEN 'low'
        WHEN {{ risk_score_column }} BETWEEN 31 AND 60 THEN 'medium'
        WHEN {{ risk_score_column }} BETWEEN 61 AND 85 THEN 'high'
        WHEN {{ risk_score_column }} BETWEEN 86 AND 100 THEN 'critical'
        ELSE 'unknown'
    END
{% endmacro %}