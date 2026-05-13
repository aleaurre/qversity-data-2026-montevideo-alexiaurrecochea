{#
    Normalize kyc_status to its 4 canonical lowercase values:
    'verified', 'pending', 'expired', 'rejected'.

    The bronze dataset contains 8 surface variants of this field across
    those 4 canonical values — only casing variants, no Spanish
    translations (EDA day 6 confirmed: 'PENDING', 'VERIFIED', 'EXPIRED',
    'REJECTED' uppercase forms plus the 4 lowercase canonicals).

    Strategy: defer to normalize_casing — there's no semantic mapping
    needed for this field. The wrapper exists so call sites in models
    read as `{{ normalize_kyc_status(...) }}` (intent-revealing) instead
    of `{{ normalize_casing(...) }}` (generic).

    Usage:
        select {{ normalize_kyc_status('kyc_status') }} as kyc_status
        from ...
#}

{% macro normalize_kyc_status(column_name) %}
    {{ normalize_casing(column_name) }}
{% endmacro %}
