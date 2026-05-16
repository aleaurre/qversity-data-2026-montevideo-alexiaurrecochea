{% macro get_risk_bucket(risk_score_column) %}
    case
        when {{ risk_score_column }} >= 0  and {{ risk_score_column }} <= 30  then 'low'
        when {{ risk_score_column }} >  30 and {{ risk_score_column }} <= 60  then 'medium'
        when {{ risk_score_column }} >  60 and {{ risk_score_column }} <= 85  then 'high'
        when {{ risk_score_column }} >  85 and {{ risk_score_column }} <= 100 then 'critical'
        else 'unknown'
    end
{% endmacro %}