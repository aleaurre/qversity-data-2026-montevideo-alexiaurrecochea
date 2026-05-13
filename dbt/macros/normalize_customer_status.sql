{#
    Normalize customer.status to its 4 canonical lowercase English values:
    'active', 'inactive', 'suspended', 'closed'.

    The bronze dataset contains 20 surface variants of this field across
    those 4 canonical values:
      - Casing chaos: 'Active', 'ACTIVE', 'active'
      - Spanish translations: 'activo', 'suspendido', 'cerrado', 'inactivo'
      - Spanish + casing combined: 'ACTIVO', 'Suspendido', etc.

    Strategy:
      1. Lowercase + trim the input (collapses casing variants).
      2. CASE-map the Spanish forms to their English canonical equivalents.
      3. Anything not matching one of the 8 expected forms (4 EN + 4 ES)
         passes through as the lowercased input. The accepted_values test
         in schema.yml will then catch any unexpected residue and fail
         loudly — that's intentional: silent fallthroughs hide bugs.

    Usage:
        select {{ normalize_customer_status('status') }} as status
        from ...
#}

{% macro normalize_customer_status(column_name) %}
    case lower(trim({{ column_name }}))
        when 'activo'     then 'active'
        when 'inactivo'   then 'inactive'
        when 'suspendido' then 'suspended'
        when 'cerrado'    then 'closed'
        else lower(trim({{ column_name }}))
    end
{% endmacro %}