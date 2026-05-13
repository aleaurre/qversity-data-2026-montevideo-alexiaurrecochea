{#
    Normalize loan.collateral_type. NULL-out the four missing indicators
    used by the data generator, lowercase the rest, and preserve 'none'
    as a meaningful semantic value.

    The bronze dataset uses 4 missing-value markers (same pattern as
    transaction.category):
      - empty string ''
      - 'NA', 'N/A', 'null'
    Combined: ~821 rows (~10.7% of loans).

    Important distinction:
    'none' is a CANONICAL value, NOT a missing indicator. It means
    "this loan has no collateral" (e.g., a personal loan without backing).
    Preserved as 'none' in the output. The 4 missing markers above
    are converted to NULL because they semantically mean "we don't know
    if there's collateral" — different from "we know there isn't any".

    Canonical values: 'none', 'real_estate', 'vehicle',
                      'investment_portfolio', 'savings_account'.

    Strategy: normalize_casing, then CASE-out the 4 missing markers.

    Usage:
        select {{ normalize_collateral_type('collateral_type') }} as collateral_type
        from ...
#}

{% macro normalize_collateral_type(column_name) %}
    case {{ normalize_casing(column_name) }}
        when ''     then null
        when 'na'   then null
        when 'null' then null
        when 'n/a'  then null
        else {{ normalize_casing(column_name) }}
    end
{% endmacro %}
