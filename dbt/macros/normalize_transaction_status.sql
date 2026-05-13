{#
    Normalize transaction.status to its 4 canonical lowercase values:
    'completed', 'pending', 'failed', 'reversed'.

    The bronze dataset contains 8 surface variants across those 4 canonical
    values — only casing variants (uppercase forms), no Spanish translations.
    EDA day 6 confirmed.

    Strategy: defer to normalize_casing — no semantic mapping needed.
    Thin wrapper exists to make intent explicit at call sites.

    Usage:
        select {{ normalize_transaction_status('status') }} as status
        from ...
#}

{% macro normalize_transaction_status(column_name) %}
    {{ normalize_casing(column_name) }}
{% endmacro %}