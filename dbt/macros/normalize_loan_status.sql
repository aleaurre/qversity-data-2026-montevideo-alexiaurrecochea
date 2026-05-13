{#
    Normalize loan.status to its 4 canonical lowercase values:
    'current', 'delinquent', 'default', 'paid_off'.

    The bronze dataset contains 8 surface variants across those 4 canonical
    values — only casing variants, no Spanish translations.

    Strategy: defer to normalize_casing — no semantic mapping needed.

    Usage:
        select {{ normalize_loan_status('status') }} as status
        from ...
#}

{% macro normalize_loan_status(column_name) %}
    {{ normalize_casing(column_name) }}
{% endmacro %}