{% macro get_credit_score_bucket(credit_score_column) %}
    CASE
        WHEN {{ credit_score_column }} BETWEEN 300 AND 579 THEN 'poor'
        WHEN {{ credit_score_column }} BETWEEN 580 AND 669 THEN 'fair'
        WHEN {{ credit_score_column }} BETWEEN 670 AND 739 THEN 'good'
        WHEN {{ credit_score_column }} BETWEEN 740 AND 799 THEN 'very_good'
        WHEN {{ credit_score_column }} BETWEEN 800 AND 850 THEN 'exceptional'
        ELSE 'unknown'
    END
{% endmacro %}