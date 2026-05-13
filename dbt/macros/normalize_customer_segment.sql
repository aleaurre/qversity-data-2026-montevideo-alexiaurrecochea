{#
    Normalize customer_segment to its 4 canonical lowercase English values:
    'retail', 'premium', 'private_banking', 'sme'.

    The bronze dataset contains 16 surface variants across those 4 canonical
    values:
      - Casing chaos: 'Retail', 'RETAIL', 'Premium', 'PREMIUM',
                      'Private_Banking', 'PRIVATE_BANKING', 'Sme', 'SME'
      - Spanish translations: 'minorista' (retail), 'banca_privada' (private),
                              'pyme' / 'PYME' (sme)

    Strategy:
      1. lower(trim(...)) collapses casing.
      2. CASE-map the Spanish forms to English canonicals.
      3. Anything else passes through lowercased so the accepted_values
         test catches unexpected residue (same defensive pattern as
         normalize_customer_status).

    Note: 'premium' has no Spanish variant in the data — the English form
    is used as a marketing term in LATAM banking.

    Usage:
        select {{ normalize_customer_segment('customer_segment') }} as customer_segment
        from ...
#}

{% macro normalize_customer_segment(column_name) %}
    case lower(trim({{ column_name }}))
        when 'minorista'     then 'retail'
        when 'banca_privada' then 'private_banking'
        when 'pyme'          then 'sme'
        else lower(trim({{ column_name }}))
    end
{% endmacro %}