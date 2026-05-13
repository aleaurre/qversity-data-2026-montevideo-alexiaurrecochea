{#
    Normalize customer_segment to its 4 canonical lowercase English values:
    'retail', 'premium', 'private_banking', 'sme'.

    The bronze dataset contains 16 surface variants across those 4 canonical
    values:
      - Casing chaos: 'Retail', 'RETAIL', 'Premium', 'PREMIUM',
                      'Private_Banking', 'PRIVATE_BANKING', 'Sme', 'SME'
        (collapsed by normalize_casing)
      - Spanish translations: 'minorista' (retail), 'banca_privada'
                              (private_banking), 'pyme'/'PYME' (sme)
        (mapped here)

    'premium' has no Spanish variant in the dataset — the English form
    is used as a marketing term in LATAM banking.

    Strategy: normalize_casing then CASE-map Spanish forms. Same
    pattern and defensive-passthrough rationale as normalize_customer_status.

    Usage:
        select {{ normalize_customer_segment('customer_segment') }} as customer_segment
        from ...
#}

{% macro normalize_customer_segment(column_name) %}
    case {{ normalize_casing(column_name) }}
        when 'minorista'     then 'retail'
        when 'banca_privada' then 'private_banking'
        when 'pyme'          then 'sme'
        else {{ normalize_casing(column_name) }}
    end
{% endmacro %}