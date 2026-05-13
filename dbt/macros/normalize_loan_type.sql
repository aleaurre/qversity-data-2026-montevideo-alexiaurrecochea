{#
    Normalize loan.loan_type to its 5 canonical lowercase values:
    'personal', 'mortgage', 'auto', 'education', 'business'.

    The bronze dataset contains 10 surface variants across those 5 canonical
    values — only casing variants, no Spanish translations.

    Strategy: defer to normalize_casing.

    Usage:
        select {{ normalize_loan_type('loan_type') }} as loan_type
        from ...
#}

{% macro normalize_loan_type(column_name) %}
    {{ normalize_casing(column_name) }}
{% endmacro %}