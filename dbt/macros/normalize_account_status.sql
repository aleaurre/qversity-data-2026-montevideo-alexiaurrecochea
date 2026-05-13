{#
    Normalize account.status to its 3 canonical lowercase values:
    'active', 'frozen', 'closed'.

    Note: this is a documented SPEC DIVERGENCE. The Qversity brief lists
    `active / inactive / suspended / closed` for account status, but the
    actual dataset contains `active / frozen / closed` — no `inactive` or
    `suspended`. EDA day 1 documented this; the accepted_values test
    reflects the observed values, not the spec. See decisions.md.

    The bronze dataset contains 12 surface variants across those 3
    canonical values:
      - Casing chaos: 'ACTIVE', 'Active', 'FROZEN', 'Frozen', 'CLOSED',
                      'Closed' (collapsed by normalize_casing)
      - Spanish translations: 'activo', 'congelado', 'cerrado' (mapped here)

    Usage:
        select {{ normalize_account_status('status') }} as status
        from ...
#}

{% macro normalize_account_status(column_name) %}
    case {{ normalize_casing(column_name) }}
        when 'activo'    then 'active'
        when 'congelado' then 'frozen'
        when 'cerrado'   then 'closed'
        else {{ normalize_casing(column_name) }}
    end
{% endmacro %}