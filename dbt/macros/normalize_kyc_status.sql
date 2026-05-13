{#
    Normalize kyc_status to its 4 canonical lowercase values:
    'verified', 'pending', 'expired', 'rejected'.

    The bronze dataset contains 8 surface variants of this field across
    those 4 canonical values — but only casing variants, no Spanish
    translations (unlike customer.status and customer_segment, which do
    have language variants). EDA confirmed: 'PENDING', 'VERIFIED',
    'EXPIRED', 'REJECTED' uppercase forms plus the 4 lowercase canonicals.

    Strategy: lower(trim(...)) alone is sufficient. No CASE mapping needed.

    Why a macro for this trivial case: consistency. Every categorical
    field in this dataset has its own normalize_* macro so the pattern
    is uniform and discoverable. If a future data generator update adds
    Spanish KYC terms, the fix lives in this one macro.

    Usage:
        select {{ normalize_kyc_status('kyc_status') }} as kyc_status
        from ...
#}

{% macro normalize_kyc_status(column_name) %}
    lower(trim({{ column_name }}))
{% endmacro %}
