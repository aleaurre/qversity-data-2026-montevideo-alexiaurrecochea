{% macro get_age_bucket(age_column) %}
    CASE
        WHEN {{ age_column }} BETWEEN 18 AND 25 THEN '18-25'
        WHEN {{ age_column }} BETWEEN 26 AND 35 THEN '26-35'
        WHEN {{ age_column }} BETWEEN 36 AND 50 THEN '36-50'
        WHEN {{ age_column }} BETWEEN 51 AND 65 THEN '51-65'
        WHEN {{ age_column }} > 65 THEN '65+'
        ELSE 'unknown'
    END
{% endmacro %}