{#
    Normalize customer.status to its 4 canonical lowercase English values:
    'active', 'inactive', 'suspended', 'closed'.

    The bronze dataset contains 20 surface variants of this field across
    those 4 canonical values:
      - Casing chaos: 'Active', 'ACTIVE', 'active' (collapsed by normalize_casing)
      - Spanish translations: 'activo', 'suspendido', 'cerrado', 'inactivo'
        (mapped here)

    Strategy:
      1. normalize_casing collapses casing variants to lowercase+trim.
      2. CASE-map the resulting Spanish forms to English canonicals.
      3. Anything else passes through the lowercased value. The
         accepted_values test in schema.yml then catches unexpected
         residue and fails loudly — that's intentional: silent
         fallthroughs hide bugs.

    Usage:
        select {{ normalize_customer_status('status') }} as status
        from ...
#}

{% macro normalize_customer_status(column_name) %}
    case {{ normalize_casing(column_name) }}
        when 'activo'     then 'active'
        when 'inactivo'   then 'inactive'
        when 'suspendido' then 'suspended'
        when 'cerrado'    then 'closed'
        else {{ normalize_casing(column_name) }}
    end
{% endmacro %}